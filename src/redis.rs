use self::command::Command;
use self::synchronize::construct_rdb;
use crate::config::Config;
use crate::resp::{resp_parser::RespParser, resp_serializer::serialize_resp_data, RespType};

use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::task;
use tokio::time::{sleep, Duration};

pub mod command;
pub mod synchronize;

pub struct Redis {
    database: Arc<Mutex<HashMap<String, String>>>,
    config: Arc<Config>,
    listener: TcpListener,
    replica_connections: Arc<Mutex<Option<HashMap<String, Arc<Mutex<TcpStream>>>>>>,
    master_connection: Arc<Mutex<Option<Arc<Mutex<TcpStream>>>>>,
    // Replicas must maintain their connection to master, this is they will propagate changes
}

impl Redis {
    async fn handle_conn(&mut self, stream: Arc<Mutex<TcpStream>>) {
        let database: Arc<Mutex<HashMap<String, String>>> = Arc::clone(&self.database);
        let config = Arc::clone(&self.config);
        let replica_connections = Arc::clone(&self.replica_connections);
        task::spawn(async move {
            let mut buf = [0; 512];
            loop {
                {
                    let mut stream = stream.lock().await;
                    match stream.read(&mut buf).await {
                        Ok(0) => break,
                        Ok(n) => n,
                        Err(e) => {
                            println!("Failed to read from stream; err = {:?}", e);
                            return;
                        }
                    };
                }
                println!(
                    "{}",
                    String::from_utf8(buf.to_vec()).expect("Invalid UTF-8 sequence")
                );
                let mut parser = RespParser::new(
                    String::from_utf8(buf.to_vec()).expect("Invalid UTF-8 sequence"),
                );
                let command = parser.parse_command();
                // If command is write and this is the master, propagate command to all replicas
                if config.is_master() && command.is_write() {
                    let replica_connections = replica_connections.lock().await;
                    if let Some(ref connections) = *replica_connections {
                        for (_port, replica_stream) in connections.iter() {
                            println!("Propagating changes");
                            synchronize::propagate_command_to_replica(
                                Arc::clone(replica_stream),
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
                    Command::ReplConf(arg1, arg2) => {
                        if !config.is_master() {
                            panic!("Recieving REPLCONF command as a replica, should exclusively be sent by replicas to masters");
                        }
                        handle_replconf(
                            arg1,
                            arg2,
                            Arc::clone(&stream),
                            Arc::clone(&replica_connections),
                        )
                        .await;
                    }
                    Command::Psync(replication_id, offset) => {
                        if !config.is_master() {
                            panic!("Recieving PSYNC command as a replica, should exclusively be sent by replicas to masters");
                        }
                        handle_psync(
                            replication_id,
                            offset,
                            Arc::clone(&stream),
                            Arc::clone(&database),
                        )
                        .await;
                    }
                    _ => panic!("Unsupported Command"),
                };
            }
        });
    }
    async fn send_and_recieve(
        stream: &mut TcpStream,
        message: &str,
    ) -> Result<String, Box<dyn std::error::Error>> {
        // Write the message to the stream
        stream.write_all(message.as_bytes()).await?;
        stream.flush().await?;

        // Buffer to store the response
        let mut buf = [0; 1024];
        let n = stream.read(&mut buf).await?;

        // Convert the response to a String
        let response = String::from_utf8_lossy(&buf[..n]).to_string();
        Ok(response)
    }
    pub async fn perform_handshake(&mut self) {
        use crate::resp::RespType;
        if self.config.is_master() {
            return;
        }
        let ping: RespType =
            RespType::Array(vec![RespType::BulkString(Some(String::from("PING")))]);
        let repl_port = RespType::Array(vec![
            RespType::BulkString(Some(String::from("REPLCONF"))),
            RespType::BulkString(Some(String::from("listening-port"))),
            RespType::BulkString(Some(String::from(&format!("{}", self.config.port)))),
        ]);
        let repl_capa = RespType::Array(vec![
            RespType::BulkString(Some(String::from("REPLCONF"))),
            RespType::BulkString(Some(String::from("capa"))),
            RespType::BulkString(Some(String::from("psync2"))),
        ]);
        let psync = RespType::Array(vec![
            RespType::BulkString(Some(String::from("PSYNC"))),
            RespType::BulkString(Some(String::from("?"))),
            RespType::BulkString(Some(String::from("-1"))),
        ]);

        let serialized_ping = serialize_resp_data(ping);
        let serialized_repl_port = serialize_resp_data(repl_port);
        let serialized_repl_capa = serialize_resp_data(repl_capa);
        let serialized_psync = serialize_resp_data(psync);
        let mut stream = match TcpStream::connect(format!(
            "{}:{}",
            self.config.master_host.as_ref().unwrap(),
            self.config.master_port.as_ref().unwrap()
        ))
        .await
        {
            Ok(x) => x,
            Err(e) => panic!("{}", e),
        };
        let _ = Redis::send_and_recieve(&mut stream, &serialized_ping).await;
        let _ = Redis::send_and_recieve(&mut stream, &serialized_repl_port).await;
        let _ = Redis::send_and_recieve(&mut stream, &serialized_repl_capa).await;
        let _ = Redis::send_and_recieve(&mut stream, &serialized_psync).await;
        self.master_connection = Arc::new(Mutex::new(Some(Arc::new(Mutex::new(stream)))));
    }

    pub async fn listen(&mut self) -> Result<Self, Box<(dyn std::error::Error + 'static)>> {
        if !self.config.is_master() {
            let master_connection = {
                let master_connection_guard = self.master_connection.lock().await;
                if let Some(ref master_connection) = *master_connection_guard {
                    Arc::clone(master_connection)
                } else {
                    panic!("Expected to have master connection on replica");
                }
            };
            self.handle_conn(master_connection).await;
        }
        loop {
            let (stream, _) = self.listener.accept().await?;
            println!("New stream connected to master: {:?}", stream);
            let stream = Arc::new(Mutex::new(stream));
            self.handle_conn(Arc::clone(&stream)).await;
        }
    }

    pub async fn new(
        config: Arc<Config>,
        listener: TcpListener,
    ) -> Result<Self, Box<(dyn std::error::Error + 'static)>> {
        let connections: Arc<Mutex<Option<HashMap<String, Arc<Mutex<TcpStream>>>>>> =
            match config.is_master() {
                true => Arc::new(Mutex::new(Some(HashMap::new()))),
                false => Arc::new(Mutex::new(None)),
            };
        Ok(Redis {
            database: Arc::new(Mutex::new(HashMap::new())),
            config,
            listener,
            replica_connections: connections,
            master_connection: Arc::new(Mutex::new(None)),
        })
    }
}

async fn handle_echo(message: String, stream: Arc<Mutex<TcpStream>>) {
    let response = String::from(&format!("${}\r\n{}\r\n", message.len(), message));
    let mut stream = stream.lock().await;
    let _ = stream.write_all(response.as_bytes()).await;
}

async fn handle_ping(stream: Arc<Mutex<TcpStream>>) {
    let response = String::from("+PONG\r\n");
    let mut stream = stream.lock().await;
    let _ = stream.write_all(response.as_bytes()).await;
}

async fn handle_set(
    key: String,
    value: String,
    expiry: Option<u64>,
    stream: Arc<Mutex<TcpStream>>,
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
    let response = String::from("+OK\r\n");
    let mut stream = stream.lock().await;
    let _ = stream.write_all(response.as_bytes()).await;
}

async fn handle_get(
    key: String,
    stream: Arc<Mutex<TcpStream>>,
    db: Arc<Mutex<HashMap<String, String>>>,
) {
    let db = db.lock().await;
    let response = match db.get(&key) {
        Some(y) => String::from(format!("${}\r\n{}\r\n", y.len(), y)),
        None => String::from("$-1\r\n"),
    };
    let mut stream = stream.lock().await;
    let _ = stream.write_all(response.as_bytes()).await;
}

async fn handle_info(_arg: String, config: Arc<Config>, stream: Arc<Mutex<TcpStream>>) {
    let response = match config.role.as_str() {
        "master" => serialize_resp_data(RespType::BulkString(
            format!(
                "role:{}\nmaster_replid:{}\nmaster_repl_offset:{}\n",
                config.role,
                config.master_replid.as_ref().unwrap(),
                config.master_repl_offset.as_ref().unwrap()
            )
            .into(),
        )),
        "slave" => {
            serialize_resp_data(RespType::BulkString(format!("role:{}", config.role).into()))
        }
        _ => panic!("Redis instance must be either slave or master"),
    };
    let mut stream = stream.lock().await;
    let _ = stream.write_all(response.as_bytes()).await;
}

async fn handle_replconf(
    arg1: String,
    arg2: String,
    stream: Arc<Mutex<TcpStream>>,
    replica_connections: Arc<Mutex<Option<HashMap<String, Arc<Mutex<TcpStream>>>>>>,
) {
    // If we recieve a replconf with a port argument, we know it is a replica, add its port to our
    // hashmap for synchronization purposes
    let mut guard = replica_connections.lock().await;
    match *guard {
        Some(ref mut connections) => {
            if arg1 == "listening-port" {
                println!("Listening Port: {}", arg2);
                let addr = format!("127.0.0.1:{}", arg2);
                /* match TcpStream::connect(&addr).await {
                    Ok(new_stream) => {
                        let new_stream = Arc::new(Mutex::new(new_stream));
                        let _ = connections.insert(arg2, Arc::clone(&new_stream));
                        println!("Added new stream to map");

                        // Respond to the original stream with +OK
                        let response = String::from("+OK\r\n");
                        let mut original_stream = stream.lock().await;
                        let _ = original_stream.write_all(response.as_bytes()).await;
                        println!("Handshake stream: {:?}", stream);
                    }
                    Err(e) => {
                        println!("Failed to connect to replica at {}: {}", addr, e);
                    }
                } */
                let _ = connections.insert(arg2, Arc::clone(&stream));
                println!("Added stream to map");
            }
        }
        None => panic!("Master should have a hashmap dedicated to storing connections to replicas"),
    }
    let response = String::from("+OK\r\n");
    println!("Handshake stream: {:?}", stream);
    let mut stream = stream.lock().await;
    let _ = stream.write_all(response.as_bytes()).await;
}

async fn handle_psync(
    _replication_id: String,
    _offset: String,
    stream: Arc<Mutex<TcpStream>>,
    db: Arc<Mutex<HashMap<String, String>>>,
) {
    let repl_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    let response = String::from(&format!("+FULLRESYNC {} 0\r\n", repl_id));
    let mut stream = stream.lock().await;
    let _ = stream.write_all(response.as_bytes()).await;
    let (length, binary) = construct_rdb(Arc::clone(&db));
    let _ = stream.write_all(length.as_bytes()).await;
    let _ = stream.write_all(&binary).await;
}
