use super::commands::Command;
use super::RedisState;

use crate::config::Config;
use crate::resp::{
    resp_deserializer::RespParser,
    resp_serializer::{create_null_string, serialize_command, serialize_resp_data},
    RespType,
};

use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{self, sleep, Duration};

pub async fn handle_echo(message: String, stream: Arc<RwLock<TcpStream>>, role: RedisState) {
    let response = serialize_resp_data(RespType::BulkString(Some(String::from(format!(
        "{}",
        message
    )))));
    if role == RedisState::Master {
        let mut stream = stream.write().await;
        let _ = stream.write_all(response.as_bytes()).await;
    }
}

pub async fn handle_ping(stream: Arc<RwLock<TcpStream>>, role: RedisState) {
    let response = serialize_resp_data(RespType::SimpleString(String::from("PONG")));
    if role == RedisState::Master {
        let mut stream = stream.write().await;
        let _ = stream.write_all(response.as_bytes()).await;
    }
}

pub async fn handle_set(
    key: String,
    value: String,
    expiry: Option<u64>,
    stream: Arc<RwLock<TcpStream>>,
    db: Arc<Mutex<HashMap<String, String>>>,
    role: RedisState,
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
    if role == RedisState::Master {
        let mut stream = stream.write().await;
        let _ = stream.write_all(response.as_bytes()).await;
    }
}

pub async fn handle_get(
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

pub async fn handle_info(_arg: String, config: Arc<Config>, stream: Arc<RwLock<TcpStream>>) {
    let response = match config.role {
        RedisState::Master => serialize_resp_data(RespType::BulkString(Some(format!(
            "role:{}\nmaster_replid:{}\nmaster_repl_offset:{}\n",
            config.role,
            config.master_replid.as_ref().unwrap(),
            config.master_repl_offset.as_ref().unwrap()
        )))),
        RedisState::Replica => {
            serialize_resp_data(RespType::BulkString(format!("role:{}", config.role).into()))
        }
    };
    let mut stream = stream.write().await;
    let _ = stream.write_all(response.as_bytes()).await;
}

pub async fn handle_wait(
    replica_connections: Arc<RwLock<Option<HashMap<i32, Arc<RwLock<TcpStream>>>>>>,
    stream: Arc<RwLock<TcpStream>>,
    timeout: i32,
    _replicas_to_wait_for: i32,
    write_bytes_processed: usize,
    write_commands_to_process: usize,
) {
    let get_ack_command = serialize_command(&Command::ReplConf(
        String::from("GETACK"),
        Some(String::from("*")),
    ));
    let replica_connections = replica_connections.read().await;
    let mut up_to_date_replicas: usize = 0;
    if write_commands_to_process == 0 {
        up_to_date_replicas = replica_connections.as_ref().unwrap().values().len();
        let response = serialize_resp_data(RespType::Integer(up_to_date_replicas as i64));
        let mut stream = stream.write().await;
        let _ = stream.write_all(response.as_bytes()).await;
        return;
    }

    let timeout = Duration::from_millis(timeout as u64);
    let connections = replica_connections
        .as_ref()
        .expect("Master didn't have replica_connections while processing WAIT");
    let mut replica_fds: Vec<i32> = connections.keys().copied().collect();
    replica_fds.sort();

    for fd in replica_fds {
        let replica_stream = connections.get(&fd).unwrap();
        let mut parser = RespParser::new(String::from(""), Arc::clone(&replica_stream));
        {
            let mut replica_stream = replica_stream.write().await;
            let _ = replica_stream.write_all(get_ack_command.as_bytes()).await;
        }

        // If we don't recieve a response within timeout, continue
        let response = match time::timeout(timeout, parser.parse_command()).await {
            Ok(Some((command, _bytes_read))) => command,
            Ok(None) => panic!("Didn't recieve response to GETACK"),
            Err(_) => continue,
        };

        match response {
            Command::ReplConf(arg1, x) => {
                if arg1.as_str() == "ACK" {
                    if let Some(byte_processed_by_repliac) = x {
                        if byte_processed_by_repliac.parse::<usize>().unwrap()
                            >= write_bytes_processed
                        {
                            up_to_date_replicas += 1;
                        }
                    }
                } else {
                    panic!("Expected REPLCONF Response to GETACK to be ACK");
                }
            }
            _ => panic!("Expected REPLCONF response to GETACK"),
        }
    }

    let response = serialize_resp_data(RespType::Integer(up_to_date_replicas as i64));
    let mut stream = stream.write().await;
    let _ = stream.write_all(response.as_bytes()).await;
}
