use self::command::Command;
use self::synchronize::construct_rdb;
use crate::config::Config;
use crate::resp::resp_serializer::create_null_string;
use crate::resp::{
    resp_deserializer::RespParser,
    resp_serializer::{serialize_command, serialize_resp_data},
    RespType,
};

use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock};
use tokio::task;
use tokio::time::{sleep, timeout, Duration, Instant};

pub mod command;
pub mod replica;
pub mod synchronize;

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
        task::spawn(async move {
            loop {
                // If a stream is a replica stream, don't automatically listen to it after the
                // handshake
                let command: Command;
                // Don't listen to replica streams
                if !is_stream_replica(Arc::clone(&replica_connections), Arc::clone(&stream)).await {
                    if let Some((comm, bytes)) = parser.parse_command().await {
                        // Increase bytes processed every time we process a command
                        command = comm;
                        if !config.is_master() {
                            total_bytes_processed += bytes;
                        } else if command.is_write() {
                            write_bytes_processed += bytes;
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
                if config.is_master() && command.is_write() {
                    let replica_connections = replica_connections.read().await;
                    if let Some(ref connections) = *replica_connections {
                        for (fd, replica_stream) in connections.iter() {
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
                        handle_echo(message, Arc::clone(&stream), config.is_master()).await;
                    }
                    Command::Ping => {
                        handle_ping(Arc::clone(&stream), config.is_master()).await;
                    }
                    Command::Set(key, value, expiry) => {
                        handle_set(
                            key,
                            value,
                            expiry,
                            Arc::clone(&stream),
                            Arc::clone(&database),
                            config.is_master(),
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
                        // Only a problem for certain types of REPLCONF
                        /* if !config.is_master() {
                            panic!("Recieving REPLCONF command as a replica, should exclusively be sent by replicas to masters");
                        } */

                        match arg1.to_lowercase().as_str() {
                            "getack" => {
                                println!("Handling GETACK");
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
                        if !config.is_master() {
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
                        if !config.is_master() {
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
                        )
                        .await;
                    }
                    _ => panic!("Unsupported Command"),
                };
            }
        });
    }

    pub async fn listen(&mut self) -> Result<Self, Box<(dyn std::error::Error + 'static)>> {
        if !self.config.is_master() {
            let parser = replica::perform_handshake(self).await;
            let master_connection = {
                match &self.master_connection {
                    Some(x) => Arc::clone(&x),
                    None => panic!("Expected to have master connection on replica"),
                }
            };
            self.handle_conn(master_connection, Some(parser)).await;
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
            match config.is_master() {
                true => Arc::new(RwLock::new(Some(HashMap::new()))),
                false => Arc::new(RwLock::new(None)),
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

// TODO: Add memoization for efficiency as we scale
async fn is_stream_replica(
    replica_connections: Arc<RwLock<Option<HashMap<i32, Arc<RwLock<TcpStream>>>>>>,
    stream: Arc<RwLock<TcpStream>>,
) -> bool {
    use std::os::unix::io::AsRawFd;

    let replica_connections = replica_connections.write().await;
    if let Some(ref connections) = *replica_connections {
        let stream = stream.read().await;
        let stream_fd = stream.as_raw_fd();
        for (replica_fd, _replica_stream) in connections.iter() {
            if stream_fd == *replica_fd {
                return true;
            }
        }
    }
    false
}

async fn handle_wait(
    replica_connections: Arc<RwLock<Option<HashMap<i32, Arc<RwLock<TcpStream>>>>>>,
    stream: Arc<RwLock<TcpStream>>,
    timeout: i32,
    _replicas_to_wait_for: i32,
    write_bytes_processed: usize,
) {
    // NOTE: handle_wait should only be called if a previous command was a write
    // Implement a check for that later

    // Send getAck and if the response is less than the write_bytes_processed, we don't count that
    // as a replica which has processed the command

    let get_ack_command = Command::ReplConf(String::from("GETACK"), Some(String::from("*")));
    let get_ack_string = serialize_command(&get_ack_command);
    let up_to_date_replicas: Arc<Mutex<usize>> = Arc::new(Mutex::new(0));
    let total_responses = Arc::clone(&up_to_date_replicas);
    let timeout = Duration::from_millis(timeout as u64);
    let start_time = Instant::now();
    let handle = task::spawn(async move {
        let replica_connections = replica_connections.read().await;
        if let Some(ref connections) = *replica_connections {
            let mut replica_fds: Vec<i32> = connections.keys().copied().collect();
            replica_fds.sort();
            println!("Sorted list of replica fd's: {:?}", replica_fds);
            for fd in replica_fds {
                println!("Handling FD {}", fd);
                match connections.get(&fd) {
                    Some(replica_stream) => {
                        // TODO: Store a parser instead of a stream in replica_connections
                        let mut parser =
                            RespParser::new(String::from(""), Arc::clone(&replica_stream));
                        {
                            let mut replica_stream = replica_stream.write().await;
                            let _ = replica_stream.write_all(get_ack_string.as_bytes()).await;
                            println!("Sent GETACK to {:?}", replica_stream);
                        }
                        println!("About to read response from FD: {}", fd);

                        let response = match parser.parse_command().await {
                            Some((command, _bytes_read)) => command,
                            None => panic!("Expected response to GETACK"),
                        };
                        println!("Read response from FD: {}", fd);

                        match response {
                            Command::ReplConf(arg1, x) => {
                                if arg1.as_str() == "ACK" {
                                    if let Some(byte_processed_by_repliac) = x {
                                        if byte_processed_by_repliac.parse::<usize>().unwrap()
                                            >= write_bytes_processed
                                        {
                                            let mut total_responses = total_responses.lock().await;
                                            *total_responses += 1;
                                        }
                                    }
                                } else {
                                    panic!("Expected REPLCFONG Response to GETACK to be ACK");
                                }
                            }
                            _ => panic!("Expected REPLCONF response to GETACK"),
                        }
                        println!("Handled FD: {}", fd);
                    }
                    None => panic!("THIS SHOULD LITERALLY NEVER HAPPPEN"),
                }
                /* if start_time.elapsed() >= timeout {
                    break;
                } */
            }
        }
    });
    let _ = handle.await;

    let up_to_date_replicas = up_to_date_replicas.lock().await;
    let response = serialize_resp_data(RespType::Integer(up_to_date_replicas.to_owned() as i64));
    let mut stream = stream.write().await;
    let _ = stream.write_all(response.as_bytes()).await;
}

async fn handle_echo(message: String, stream: Arc<RwLock<TcpStream>>, is_master: bool) {
    let response = serialize_resp_data(RespType::BulkString(Some(String::from(format!(
        "{}",
        message
    )))));
    if is_master {
        let mut stream = stream.write().await;
        let _ = stream.write_all(response.as_bytes()).await;
    }
}

async fn handle_ping(stream: Arc<RwLock<TcpStream>>, is_master: bool) {
    let response = serialize_resp_data(RespType::SimpleString(String::from("PONG")));
    if is_master {
        let mut stream = stream.write().await;
        let _ = stream.write_all(response.as_bytes()).await;
    }
}

async fn handle_set(
    key: String,
    value: String,
    expiry: Option<u64>,
    stream: Arc<RwLock<TcpStream>>,
    db: Arc<Mutex<HashMap<String, String>>>,
    is_master: bool,
) {
    {
        let mut db = db.lock().await;
        db.insert(key.clone(), value);
    }
    if let Some(delay_millis) = expiry {
        let key_clone = key.clone();
        let db_clone = Arc::clone(&db);
        tokio::spawn(async move {
            sleep(Duration::from_millis(delay_millis)).await;
            let mut db = db_clone.lock().await;
            db.remove(&key_clone);
        });
    }
    let response = serialize_resp_data(RespType::SimpleString(String::from("OK")));
    if is_master {
        let mut stream = stream.write().await;
        let _ = stream.write_all(response.as_bytes()).await;
    }
}

async fn handle_get(
    key: String,
    stream: Arc<RwLock<TcpStream>>,
    db: Arc<Mutex<HashMap<String, String>>>,
) {
    let db = db.lock().await;
    let response = match db.get(&key) {
        Some(x) => serialize_resp_data(RespType::BulkString(Some(String::from(x)))),
        None => create_null_string(),
    };
    let mut stream = stream.write().await;
    let _ = stream.write_all(response.as_bytes()).await;
}

async fn handle_info(_arg: String, config: Arc<Config>, stream: Arc<RwLock<TcpStream>>) {
    let response = match config.is_master() {
        true => serialize_resp_data(RespType::BulkString(Some(format!(
            "role:{}\nmaster_replid:{}\nmaster_repl_offset:{}\n",
            config.role,
            config.master_replid.as_ref().unwrap(),
            config.master_repl_offset.as_ref().unwrap()
        )))),
        false => serialize_resp_data(RespType::BulkString(format!("role:{}", config.role).into())),
    };
    let mut stream = stream.write().await;
    let _ = stream.write_all(response.as_bytes()).await;
}
