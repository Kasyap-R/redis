use self::synchronize::construct_rdb;
use crate::command::Command;
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

pub mod synchronize;

pub struct Redis {
    database: Arc<Mutex<HashMap<String, String>>>,
    config: Arc<Config>,
    listener: TcpListener,
}

impl Redis {
    async fn handle_conn(&self, mut stream: TcpStream) {
        let database: Arc<Mutex<HashMap<String, String>>> = Arc::clone(&self.database);
        let config = Arc::clone(&self.config);
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
                let mut db = database.lock().await;
                let command = parser.parse_command();
                match command {
                    Command::Echo(message) => {
                        let response =
                            String::from(&format!("${}\r\n{}\r\n", message.len(), message));
                        let _ = stream.write_all(response.as_bytes()).await;
                    }
                    Command::Ping => {
                        let response = String::from("+PONG\r\n");
                        let _ = stream.write_all(response.as_bytes()).await;
                    }
                    Command::Set(key, value, expiry) => {
                        {
                            db.insert(key.clone(), value);
                        }
                        if let Some(delay_millis) = expiry {
                            let key_clone = key.clone();
                            let db_clone = Arc::clone(&database);
                            tokio::spawn(async move {
                                sleep(Duration::from_millis(delay_millis)).await;
                                let mut db = db_clone.lock().await;
                                db.remove(&key_clone);
                            });
                        }
                        let response = String::from("+OK\r\n");
                        let _ = stream.write_all(response.as_bytes()).await;
                    }
                    Command::Get(key) => {
                        let response = match db.get(&key) {
                            Some(y) => String::from(format!("${}\r\n{}\r\n", y.len(), y)),
                            None => String::from("$-1\r\n"),
                        };
                        let _ = stream.write_all(response.as_bytes()).await;
                    }
                    Command::Info(_arg) => {
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
                            "slave" => serialize_resp_data(RespType::BulkString(
                                format!("role:{}", config.role).into(),
                            )),
                            _ => panic!("Redis instance must be either slave or master"),
                        };
                        let _ = stream.write_all(response.as_bytes()).await;
                    }
                    Command::ReplConf(_arg1, _arg2) => {
                        let response = String::from("+OK\r\n");
                        let _ = stream.write_all(response.as_bytes()).await;
                    }
                    Command::Psync(_replication_id, _offset) => {
                        let repl_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
                        let response = String::from(&format!("+FULLRESYNC {} 0\r\n", repl_id));
                        let _ = stream.write_all(response.as_bytes()).await;
                        let (length, binary) = construct_rdb(Arc::clone(&database));
                        let _ = stream.write_all(length.as_bytes()).await;
                        let _ = stream.write_all(&binary).await;
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
        })
    }
}
