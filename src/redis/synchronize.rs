use crate::redis::command::Command;
use crate::resp::resp_serializer::serialize_command;
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
extern crate base64;

const RDB_B64: &str = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";

pub async fn propagate_command_to_replica(stream: Arc<Mutex<TcpStream>>, command: &Command) {
    println!("{:?}", command);
    let serialized_command = serialize_command(command);
    println!("Serialized command: {}", serialized_command);
    std::io::stdout().flush().unwrap();
    let mut stream = stream.lock().await;
    println!("Writing to {:?}", stream);
    if let Err(e) = stream.write_all(serialized_command.as_bytes()).await {
        println!("Failed to write to stream: {}", e);
    }
    if let Err(e) = stream.flush().await {
        println!("Failed to flush stream: {}", e);
    }
    std::io::stdout().flush().unwrap();
}

pub fn construct_rdb(database: Arc<Mutex<HashMap<String, String>>>) -> (String, Vec<u8>) {
    let binary_data = base64::decode(RDB_B64).expect("Failed to decode base64");
    let length = binary_data.len();
    (format!("${}\r\n", length), binary_data)
}
