use self::commands::Command;
use self::processing::*;
use self::replica::is_stream_replica;
use self::synchronize::construct_rdb;

use crate::config::Config;
use crate::resp::resp_deserializer::RespParser;

use core::fmt;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock};
use tokio::task;

pub mod commands;
pub mod processing;
pub mod replica;
pub mod synchronize;

#[derive(PartialEq, Debug, Clone, Copy)]
pub enum RedisState {
    Master,
    Replica,
}

impl fmt::Display for RedisState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RedisState::Replica => write!(f, "slave"),
            RedisState::Master => write!(f, "master"),
        }
    }
}

pub struct Redis {
    database: Arc<Mutex<HashMap<String, String>>>,
    config: Arc<Config>,
    listener: TcpListener,
    replica_connections: Arc<RwLock<Option<HashMap<i32, Arc<RwLock<TcpStream>>>>>>,
    master_connection: Option<Arc<RwLock<TcpStream>>>,
    // Replicas must maintain their connection to master, this is they will propagate changes
}

impl Redis {
    async fn handle_conn(&mut self, stream: Arc<RwLock<TcpStream>>, parser: Option<RespParser>) {
        let database: Arc<Mutex<HashMap<String, String>>> = Arc::clone(&self.database);
        let config = Arc::clone(&self.config);
        let replica_connections = Arc::clone(&self.replica_connections);
        // Each connection should have a dedicated parser
        let mut parser = match parser {
            Some(x) => x,
            None => RespParser::new(String::from(""), Arc::clone(&stream)),
        };
        let mut total_bytes_processed = 0;
        let mut write_bytes_processed = 0;
        let mut write_commands_to_process = 0;
        task::spawn(async move {
            loop {
                // If a stream is a replica stream, don't automatically listen to it after the
                // handshake
                let command: Command;
                if !is_stream_replica(Arc::clone(&replica_connections), Arc::clone(&stream)).await {
                    if let Some((comm, bytes)) = parser.parse_command().await {
                        // Increase bytes processed every time we process a command
                        command = comm;
                        if config.role == RedisState::Replica {
                            total_bytes_processed += bytes;
                        } else if command.is_write() {
                            write_bytes_processed += bytes;
                            write_commands_to_process += 1;
                        }
                    } else {
                        // other side has ended connection
                        break;
                    }
                } else {
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                    continue;
                }

                // If command is write and this is the master, propagate command to all replicas
                if config.role == RedisState::Master && command.is_write() {
                    let replica_connections = replica_connections.read().await;
                    if let Some(ref connections) = *replica_connections {
                        for (_fd, replica_stream) in connections.iter() {
                            synchronize::propagate_command_to_replica(
                                Arc::clone(&replica_stream),
                                &command,
                            )
                            .await;
                        }
                    }
                }

                match command {
                    Command::Echo(message) => {
                        handle_echo(message, Arc::clone(&stream), config.role).await;
                    }
                    Command::Ping => {
                        handle_ping(Arc::clone(&stream), config.role).await;
                    }
                    Command::Set(key, value, expiry) => {
                        handle_set(
                            key,
                            value,
                            expiry,
                            Arc::clone(&stream),
                            Arc::clone(&database),
                            config.role,
                        )
                        .await;
                    }
                    Command::Get(key) => {
                        handle_get(key, Arc::clone(&stream), Arc::clone(&database)).await;
                    }
                    Command::Info(arg) => {
                        handle_info(arg, Arc::clone(&config), Arc::clone(&stream)).await;
                    }
                    Command::ReplConf(arg1, _arg2) => {
                        match arg1.to_lowercase().as_str() {
                            "getack" => {
                                if config.role == RedisState::Master {
                                    panic!("Recieving REPLCONF command as a master, should exclusively be sent by masters to replicas");
                                }
                                replica::handle_replconf_getack(
                                    Arc::clone(&stream),
                                    total_bytes_processed - 37,
                                )
                                .await;
                            }
                            _ => replica::handle_replconf(Arc::clone(&stream)).await,
                        };
                    }
                    Command::Psync(replication_id, offset) => {
                        if config.role == RedisState::Replica {
                            panic!("Recieving PSYNC command as a replica, should exclusively be sent by replicas to masters");
                        }
                        replica::handle_psync(
                            replication_id,
                            offset,
                            Arc::clone(&stream),
                            Arc::clone(&database),
                        )
                        .await;

                        use std::os::unix::io::AsRawFd;
                        let mut guard = replica_connections.write().await;
                        match *guard {
                            Some(ref mut connections) => {
                                let fd = stream.read().await.as_raw_fd();
                                let _ = connections.insert(fd, Arc::clone(&stream));
                            }
                            None => panic!("Master should have a hashmap dedicated to storing connections to replicas"),
                        }
                    }
                    Command::Wait(replicas_to_wait_for, timeout) => {
                        if config.role == RedisState::Replica {
                            panic!(
                                "Replica recieved WAIT command as replica - only meant for MASTER"
                            );
                        }
                        handle_wait(
                            Arc::clone(&replica_connections),
                            Arc::clone(&stream),
                            timeout,
                            replicas_to_wait_for,
                            write_bytes_processed,
                            write_commands_to_process,
                        )
                        .await;
                        write_commands_to_process = 0;
                    }
                };
            }
        });
    }

    pub async fn listen(&mut self) -> Result<Self, Box<(dyn std::error::Error + 'static)>> {
        match self.config.role {
            RedisState::Replica => {
                let parser = replica::perform_handshake(self).await;
                let master_connection = {
                    match &self.master_connection {
                        Some(x) => Arc::clone(&x),
                        None => panic!("Expected to have master connection on replica"),
                    }
                };
                self.handle_conn(master_connection, Some(parser)).await;
            }
            RedisState::Master => (),
        }
        loop {
            let (stream, _) = self.listener.accept().await?;
            println!("New stream connected to master: {:?}", stream);
            let stream = Arc::new(RwLock::new(stream));
            self.handle_conn(Arc::clone(&stream), None).await;
        }
    }

    pub async fn new(
        config: Arc<Config>,
        listener: TcpListener,
    ) -> Result<Self, Box<(dyn std::error::Error + 'static)>> {
        let connections: Arc<RwLock<Option<HashMap<i32, Arc<RwLock<TcpStream>>>>>> =
            match config.role {
                RedisState::Master => Arc::new(RwLock::new(Some(HashMap::new()))),
                RedisState::Replica => Arc::new(RwLock::new(None)),
            };
        Ok(Redis {
            database: Arc::new(Mutex::new(HashMap::new())),
            config,
            listener,
            replica_connections: connections,
            master_connection: None,
        })
    }
}
