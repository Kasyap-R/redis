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

async fn perform_handshake(config: &Arc<Config>) {
    use crate::resp::RespType;
    if config.is_master() {
        return;
    }
    let ping: RespType = RespType::Array(vec![RespType::BulkString(Some(String::from("PING")))]);
    let repl_port = RespType::Array(vec![
        RespType::BulkString(Some(String::from("REPLCONF"))),
        RespType::BulkString(Some(String::from("listening-port"))),
        RespType::BulkString(Some(String::from(&format!("{}", config.port)))),
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
        config.master_host.as_ref().unwrap(),
        config.master_port.as_ref().unwrap()
    ))
    .await
    {
        Ok(x) => x,
        Err(e) => panic!("{}", e),
    };
    let _ = send_and_recieve(&mut stream, &serialized_ping).await;
    let _ = send_and_recieve(&mut stream, &serialized_repl_port).await;
    let _ = send_and_recieve(&mut stream, &serialized_repl_capa).await;
    let _ = send_and_recieve(&mut stream, &serialized_psync).await;
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
            let response: String = match command {
                Command::Echo(message) => {
                    String::from(&format!("${}\r\n{}\r\n", message.len(), message))
                }
                Command::Ping => String::from("+PONG\r\n"),
                Command::Set(key, value, expiry) => {
                    {
                        let mut db = database.lock().unwrap();
                        db.insert(key.clone(), value);
                    }
                    if let Some(delay_millis) = expiry {
                        let key_clone = key.clone();
                        let db_clone = Arc::clone(&database);
                        tokio::spawn(async move {
                            sleep(Duration::from_millis(delay_millis)).await;
                            let mut db = db_clone.lock().unwrap();
                            db.remove(&key_clone);
                        });
                    }
                    String::from("+OK\r\n")
                }
                Command::Get(key) => {
                    let db = database.lock().unwrap();
                    match db.get(&key) {
                        Some(y) => String::from(format!("${}\r\n{}\r\n", y.len(), y)),
                        None => String::from("$-1\r\n"),
                    }
                }
                Command::Info(_arg) => match config.role.as_str() {
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
                },
                Command::ReplConf(_arg1, _arg2, _arg3) => String::from("+OK\r\n"),
                Command::Psync(_replication_id, _offset) => {
                    String::from("+FULLRESYNC <REPL_ID> 0\r\n")
                }
                _ => panic!("Unsupported Command"),
            };
            let _ = stream.write_all(response.as_bytes()).await;
        }
    });
}
