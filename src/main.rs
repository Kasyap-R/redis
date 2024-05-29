use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task;
use tokio::time::{sleep, Duration};

pub mod config;
pub mod resp;
use crate::config::*;
use crate::resp::resp_parser::*;
use crate::resp::resp_serializer::*;

async fn perform_handshake(config: &Arc<Config>) {
    use crate::resp::RespType;
    if config.is_master() {
        return;
    }
    println!("{}", config.master_port.as_ref().unwrap());
    println!("{}", config.master_host.as_ref().unwrap());
    let handshake: RespType =
        RespType::Array(vec![RespType::BulkString(Some(String::from("PING")))]);
    let serialized_handshake = serialize_resp_data(handshake);
    let mut stream = match TcpStream::connect(format!(
        "{}:{}",
        config.master_host.as_ref().unwrap(),
        config.master_port.as_ref().unwrap()
    ))
    .await
    {
        Ok(x) => x,
        Err(e) => panic!("{}", e),
    };
    let _ = stream.write_all(serialized_handshake.as_bytes()).await;
    println!("{}", serialized_handshake);
}

#[tokio::main]
async fn main() -> Result<(), Box<(dyn std::error::Error + 'static)>> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let config = Arc::new(Config::parse());
    let listener = TcpListener::bind(format!("127.0.0.1:{}", &config.port)).await?;
    perform_handshake(&config).await;
    loop {
        let (stream, _) = listener.accept().await?;
        let config = Arc::clone(&config);
        handle_conn(stream, config).await;
    }
}

async fn handle_conn(mut stream: TcpStream, config: Arc<Config>) {
    let database: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));
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
            let mut parser =
                RespParser::new(String::from_utf8(buf.to_vec()).expect("Invalid UTF-8 sequence"));
            let command = parser.parse_command();
            match command {
                Command::Echo(message) => {
                    let mut response = "$".to_string();
                    response.push_str(&format!("{}\r\n{}\r\n", message.len(), message));
                    let _ = stream.write_all(response.as_bytes()).await;
                }
                Command::Ping => {
                    let response = "+PONG\r\n".to_string();
                    let _ = stream.write_all(response.as_bytes()).await;
                }
                Command::Set(key, value, expiry) => {
                    {
                        let mut db = database.lock().unwrap();
                        db.insert(key.clone(), value);
                    }
                    let response = "+OK\r\n".to_string();
                    if let Some(delay_millis) = expiry {
                        let key_clone = key.clone();
                        let db_clone = Arc::clone(&database);
                        tokio::spawn(async move {
                            sleep(Duration::from_millis(delay_millis)).await;
                            let mut db = db_clone.lock().unwrap();
                            db.remove(&key_clone);
                        });
                    }

                    let _ = stream.write_all(response.as_bytes()).await;
                }
                Command::Get(key) => {
                    let response = {
                        let db = database.lock().unwrap();
                        match db.get(&key) {
                            Some(y) => format!("${}\r\n{}\r\n", y.len(), y),
                            None => String::from("$-1\r\n"),
                        }
                    };
                    let _ = stream.write_all(response.as_bytes()).await;
                }
                Command::Info(_arg) => {
                    let response = match config.role.as_str() {
                        "master" => serialize_resp_data(resp::RespType::BulkString(
                            format!(
                                "role:{}\nmaster_replid:{}\nmaster_repl_offset:{}\n",
                                config.role,
                                config.master_replid.as_ref().unwrap(),
                                config.master_repl_offset.as_ref().unwrap()
                            )
                            .into(),
                        )),
                        "slave" => serialize_resp_data(resp::RespType::BulkString(
                            format!("role:{}", config.role).into(),
                        )),
                        _ => panic!("Redis instance must be either slave or master"),
                    };
                    let _ = stream.write_all(response.as_bytes()).await;
                }
                _ => panic!("Unsupported Command"),
            };
        }
    });
}
