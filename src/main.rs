use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::task;
use tokio::time::{sleep, Duration};

pub mod resp_parser;
use crate::resp_parser::*;

async fn delete_key(database: &mut HashMap<String, String>, key: String) {
    database.remove(&key);
}

#[tokio::main]
async fn main() -> Result<(), Box<(dyn std::error::Error + 'static)>> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    let database: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let (mut socket, _) = listener.accept().await?;
        let db_clone = Arc::clone(&database);

        task::spawn(async move {
            let mut buf = [0; 512];
            loop {
                let _num_bytes = match socket.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => n,
                    Err(e) => {
                        println!("Failed to read from socket; err = {:?}", e);
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
                match command {
                    Command::Echo(x) => {
                        let mut response = "$".to_string();
                        response.push_str(&format!("{}\r\n{}\r\n", x.len(), x));
                        let _ = socket.write_all(response.as_bytes()).await;
                    }
                    Command::Ping => {
                        let response = "+PONG\r\n".to_string();
                        let _ = socket.write_all(response.as_bytes()).await;
                    }
                    Command::Set(key, value, expiry) => {
                        let mut db = db_clone.lock().await;
                        db.insert(key.clone(), value);
                        let response = "+OK\r\n".to_string();
                        if let Some(delay_millis) = expiry {
                            let db_clone_for_deletion = Arc::clone(&db_clone);
                            let key_clone = key.clone();
                            tokio::spawn(async move {
                                sleep(Duration::from_millis(delay_millis)).await;
                                let mut db = db_clone_for_deletion.lock().await;
                                db.remove(&key_clone);
                            });
                        }

                        let _ = socket.write_all(response.as_bytes()).await;
                    }
                    Command::Get(x) => {
                        let db = db_clone.lock().await;
                        let mut response = "".to_string();
                        match db.get(&x) {
                            Some(y) => response.push_str(&format!("${}\r\n{}\r\n", y.len(), y)),
                            None => response.push_str(&format!("$-1\r\n")),
                        }
                        let _ = socket.write_all(response.as_bytes()).await;
                    }
                    _ => panic!("Unsupported Command"),
                };
            }
        });
    }
}
