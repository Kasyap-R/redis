use self::command::Command;
use self::synchronize::construct_rdb;
use crate::config::Config;
use crate::resp::resp_serializer::create_null_string;
use crate::resp::{resp_parser::RespParser, resp_serializer::serialize_resp_data, RespType};

use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock};
use tokio::task;
use tokio::time::{sleep, Duration};

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
    async fn handle_conn(&mut self, stream: Arc<RwLock<TcpStream>>) {
        let database: Arc<Mutex<HashMap<String, String>>> = Arc::clone(&self.database);
        let config = Arc::clone(&self.config);
        let replica_connections = Arc::clone(&self.replica_connections);
        // Each connection should have a dedicated parser
        task::spawn(async move {
            loop {
                // If a stream is a replica stream, don't automatically listen to it
                // We do have to listen until the handshake is complete though
                let stream_data: String;
                if !is_replica(Arc::clone(&replica_connections), Arc::clone(&stream)).await {
                    stream_data = match read_from_stream(Arc::clone(&stream)).await {
                        Some(x) => x,
                        None => break,
                    }
                } else {
                    break;
                }

                println!("====== Receiving new transmission ======");
                println!("{}", stream_data,);
                println!("====== End of new transmission ======");

                let mut parser = RespParser::new(stream_data, Arc::clone(&stream));
                let command = parser.parse_command();

                // If command is write and this is the master, propagate command to all replicas
                if config.is_master() && command.is_write() {
                    let replica_connections = replica_connections.read().await;
                    if let Some(ref connections) = *replica_connections {
                        for (_port, replica_stream) in connections.iter() {
                            println!("Found replica stream");
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
                        handle_echo(message, Arc::clone(&stream)).await;
                    }
                    Command::Ping => {
                        handle_ping(Arc::clone(&stream)).await;
                    }
                    Command::Set(key, value, expiry) => {
                        handle_set(
                            key,
                            value,
                            expiry,
                            Arc::clone(&stream),
                            Arc::clone(&database),
                        )
                        .await;
                    }
                    Command::Get(key) => {
                        handle_get(key, Arc::clone(&stream), Arc::clone(&database)).await;
                    }
                    Command::Info(arg) => {
                        handle_info(arg, Arc::clone(&config), Arc::clone(&stream)).await;
                    }
                    Command::ReplConf(_arg1, _arg2) => {
                        if !config.is_master() {
                            panic!("Recieving REPLCONF command as a replica, should exclusively be sent by replicas to masters");
                        }
                        replica::handle_replconf(Arc::clone(&stream)).await;
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
                    _ => panic!("Unsupported Command"),
                };
            }
        });
    }

    pub async fn listen(&mut self) -> Result<Self, Box<(dyn std::error::Error + 'static)>> {
        if !self.config.is_master() {
            let master_connection = {
                match &self.master_connection {
                    Some(x) => Arc::clone(&x),
                    None => panic!("Expected to have master connection on replica"),
                }
            };
            self.handle_conn(master_connection).await;
        }
        loop {
            let (stream, _) = self.listener.accept().await?;
            println!("New stream connected to master: {:?}", stream);
            let stream = Arc::new(RwLock::new(stream));
            self.handle_conn(Arc::clone(&stream)).await;
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

async fn read_from_stream(stream: Arc<RwLock<TcpStream>>) -> Option<String> {
    let mut stream = stream.write().await;
    let mut buffer: [u8; 1024] = [0; 1024];
    match stream.read(&mut buffer).await {
        Ok(0) => None,
        Ok(_) => Some(String::from_utf8(buffer.to_vec()).expect("Expected valid utf-8 sequence")),
        Err(e) => panic!("Stream returned error: {:?}", e),
    }
}

// TODO: Add memoization for efficiency as we scale
async fn is_replica(
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

async fn handle_echo(message: String, stream: Arc<RwLock<TcpStream>>) {
    let response = serialize_resp_data(RespType::BulkString(Some(String::from(format!(
        "{}",
        message
    )))));
    let mut stream = stream.write().await;
    let _ = stream.write_all(response.as_bytes()).await;
}

async fn handle_ping(stream: Arc<RwLock<TcpStream>>) {
    let response = serialize_resp_data(RespType::SimpleString(String::from("Pong")));
    let mut stream = stream.write().await;
    let _ = stream.write_all(response.as_bytes()).await;
}

async fn handle_set(
    key: String,
    value: String,
    expiry: Option<u64>,
    stream: Arc<RwLock<TcpStream>>,
    db: Arc<Mutex<HashMap<String, String>>>,
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
    let response = serialize_resp_data(RespType::SimpleString(String::from("Ok")));
    let mut stream = stream.write().await;
    let _ = stream.write_all(response.as_bytes()).await;
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
    let response = match config.role.as_str() {
        "master" => serialize_resp_data(RespType::BulkString(Some(format!(
            "role:{}\nmaster_replid:{}\nmaster_repl_offset:{}\n",
            config.role,
            config.master_replid.as_ref().unwrap(),
            config.master_repl_offset.as_ref().unwrap()
        )))),
        "slave" => {
            serialize_resp_data(RespType::BulkString(format!("role:{}", config.role).into()))
        }
        _ => panic!("Redis instance must be either slave or master"),
    };
    let mut stream = stream.write().await;
    let _ = stream.write_all(response.as_bytes()).await;
}
