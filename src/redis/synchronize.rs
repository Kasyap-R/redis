use crate::redis::commands::Command;
use crate::resp::resp_serializer::serialize_command;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::{Mutex, RwLock};

extern crate base64;

const RDB_B64: &str = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";

pub async fn propagate_command_to_replica(stream: Arc<RwLock<TcpStream>>, command: &Command) {
    let serialized_command = serialize_command(command);
    let mut stream = stream.write().await;
    if let Err(e) = stream.write_all(serialized_command.as_bytes()).await {
        println!("Failed to write to stream: {}", e);
    }
    if let Err(e) = stream.flush().await {
        println!("Failed to flush stream: {}", e);
    }
}

pub fn construct_rdb(_database: Arc<Mutex<HashMap<String, String>>>) -> (String, Vec<u8>) {
    let binary_data = base64::decode(RDB_B64).expect("Failed to decode base64");
    let length = binary_data.len();
    (format!("${}\r\n", length), binary_data)
}
