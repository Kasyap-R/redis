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
// In future, add modules like redis::commands (maybe move current command), redis::synchronize

pub mod command;
pub mod synchronize;

pub struct Redis {
    database: Arc<Mutex<HashMap<String, String>>>,
    config: Arc<Config>,
    listener: TcpListener,
    replica_connections: Arc<Mutex<Option<HashMap<String, Arc<Mutex<TcpStream>>>>>>,
}

impl Redis {
    async fn handle_conn(&mut self, mut stream: TcpStream) {
        let database: Arc<Mutex<HashMap<String, String>>> = Arc::clone(&self.database);
        let config = Arc::clone(&self.config);
        let replica_connections = Arc::clone(&self.replica_connections);
        task::spawn(async move {
            let mut buf = [0; 512];
            loop {
                let _num_bytes = match stream.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => n,
                    Err(e) => {
                        println!("Failed to read from stream; err = {:?}", e);
                        return;
                    }
                };
                println!(
                    "{}",
                    String::from_utf8(buf.to_vec()).expect("Invalid UTF-8 sequence")
                );
                let mut parser = RespParser::new(
                    String::from_utf8(buf.to_vec()).expect("Invalid UTF-8 sequence"),
                );
                let command = parser.parse_command();
                // If command is write and this is the master, propagate to all replicas
                match command {
                    Command::Echo(message) => {
                        handle_echo(message, &mut stream).await;
                    }
                    Command::Ping => {
                        handle_ping(&mut stream).await;
                    }
                    Command::Set(key, value, expiry) => {
                        handle_set(key, value, expiry, &mut stream, Arc::clone(&database)).await;
                    }
                    Command::Get(key) => {
                        handle_get(key, &mut stream, Arc::clone(&database)).await;
                    }
                    Command::Info(arg) => {
                        handle_info(arg, Arc::clone(&config), &mut stream).await;
                    }
                    Command::ReplConf(arg1, arg2) => {
                        if !config.is_master() {
                            panic!("Recieving REPLCONF command as a replica, should exclusively be sent by replicas to masters");
                        }
                        handle_replconf(arg1, arg2, &mut stream, Arc::clone(&replica_connections))
                            .await;
                    }
                    Command::Psync(replication_id, offset) => {
                        handle_psync(replication_id, offset, &mut stream, Arc::clone(&database))
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
    pub async fn perform_handshake(&self) {
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
    }

    pub async fn listen(&mut self) -> Result<Self, Box<(dyn std::error::Error + 'static)>> {
        loop {
            let (stream, _) = self.listener.accept().await?;
            self.handle_conn(stream).await;
        }
    }

    pub async fn new(
        config: Arc<Config>,
        listener: TcpListener,
    ) -> Result<Self, Box<(dyn std::error::Error + 'static)>> {
        Ok(Redis {
            database: Arc::new(Mutex::new(HashMap::new())),
            config,
            listener,
            replica_connections: Arc::new(Mutex::new(None)),
        })
    }
}

async fn handle_echo(message: String, stream: &mut tokio::net::TcpStream) {
    let response = String::from(&format!("${}\r\n{}\r\n", message.len(), message));
    let _ = stream.write_all(response.as_bytes()).await;
}

async fn handle_ping(stream: &mut tokio::net::TcpStream) {
    let response = String::from("+PONG\r\n");
    let _ = stream.write_all(response.as_bytes()).await;
}

async fn handle_set(
    key: String,
    value: String,
    expiry: Option<u64>,
    stream: &mut tokio::net::TcpStream,
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
    let _ = stream.write_all(response.as_bytes()).await;
}

async fn handle_get(
    key: String,
    stream: &mut tokio::net::TcpStream,
    db: Arc<Mutex<HashMap<String, String>>>,
) {
    let db = db.lock().await;
    let response = match db.get(&key) {
        Some(y) => String::from(format!("${}\r\n{}\r\n", y.len(), y)),
        None => String::from("$-1\r\n"),
    };
    let _ = stream.write_all(response.as_bytes()).await;
}

async fn handle_info(_arg: String, config: Arc<Config>, stream: &mut tokio::net::TcpStream) {
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
    let _ = stream.write_all(response.as_bytes()).await;
}

async fn handle_replconf(
    arg1: String,
    arg2: String,
    mut stream: TcpStream,
    replica_connections: Arc<Mutex<Option<HashMap<String, Arc<Mutex<TcpStream>>>>>>,
) {
    // If we recieve a replconf with a port argument, we know it is a replica, add its port to our
    // hashmap for synchronization purposes
    let mut guard = replica_connections.lock().await;
    match *guard {
        Some(ref mut connections) => {
            if arg1 == "listening-port" {
                let wrapped_stream = Arc::new(Mutex::new(stream));
                let _ = connections.insert(arg2, Arc::clone(&wrapped_stream));
                stream = wrapped_stream.lock().await;
            }
        }
        None => panic!("Master should have a hashmap dedicated to storing connections to replicas"),
    }
    let response = String::from("+OK\r\n");
    let _ = stream.write_all(response.as_bytes()).await;
}

async fn handle_psync(
    _replication_id: String,
    _offset: String,
    stream: &mut tokio::net::TcpStream,
    db: Arc<Mutex<HashMap<String, String>>>,
) {
    let repl_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    let response = String::from(&format!("+FULLRESYNC {} 0\r\n", repl_id));
    let _ = stream.write_all(response.as_bytes()).await;
    let (length, binary) = construct_rdb(Arc::clone(&db));
    let _ = stream.write_all(length.as_bytes()).await;
    let _ = stream.write_all(&binary).await;
}
