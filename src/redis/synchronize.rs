use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
extern crate base64;

const RDB_B64: &str = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";

pub fn construct_rdb(database: Arc<Mutex<HashMap<String, String>>>) -> (String, Vec<u8>) {
    let binary_data = base64::decode(RDB_B64).expect("Failed to decode base64");
    let length = binary_data.len();
    (format!("${}\r\n", length), binary_data)
}
