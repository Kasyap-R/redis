use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub struct RdbParser {
    data: Vec<u8>,
    index: usize,
}

const EOF_FLAG: u8 = 0xff;
const EXPIRY_MS_FLAG: u8 = 0xfc;
const EXPIRY_S_FLAG: u8 = 0xfd;

impl RdbParser {
    // Public
    pub fn new(data: Vec<u8>) -> Self {
        Self { data, index: 0 }
    }

    pub fn rdb_to_db(&mut self) -> (HashMap<String, String>, HashMap<String, SystemTime>) {
        self.parse_header();
        self.parse_metadata();
        let mut database: HashMap<String, String> = HashMap::new();
        let mut expiry: HashMap<String, SystemTime> = HashMap::new();
        while self.data[self.index] != EOF_FLAG {
            println!("Reading KEY-Value");
            let (expiration, key, value) = self.parse_key_value();
            if let Some(x) = expiration {
                expiry.insert(key.clone(), x);
            }
            database.insert(key, value);
        }
        (database, expiry)
    }

    // Private
    fn parse_header(&mut self) {
        let header = &self.data[0..9];
        println!("Header: {:?}", header);
        self.index += 9;
    }

    fn parse_metadata(&mut self) {
        let hash_start_index = self.data.iter().position(|&b| b == 0xfb).unwrap();
        self.index = hash_start_index;
        // Skip over RESIZE DB Field
        self.index += 3;
    }

    fn parse_key_value(&mut self) -> (Option<SystemTime>, String, String) {
        let expiry = self.parse_expiry();
        // Skip over Value field
        self.index += 1;
        let length_key = self.data[self.index];
        self.index += 1;

        let key = &self.data[self.index..(self.index + length_key as usize)];
        let key = std::str::from_utf8(key).unwrap();
        self.index += length_key as usize;

        println!("Key Length: {}", length_key);
        println!("Key: {}", key);

        let length_value = self.data[self.index];
        self.index += 1;

        let value = &self.data[self.index..(self.index + length_value as usize)];
        let value = std::str::from_utf8(value).unwrap();
        self.index += length_value as usize;

        println!("Length of Value: {}", length_value);
        println!("Value: {}", value);
        if let Some(x) = expiry {
            println!("Expiry: {:?}", x);
        }

        (expiry, key.to_string(), value.to_string())
    }

    fn parse_expiry(&mut self) -> Option<SystemTime> {
        match self.data[self.index] {
            EXPIRY_MS_FLAG => {
                self.index += 1;
                let bytes: [u8; 8] = self.data[self.index..self.index + 8].try_into().unwrap();
                self.index += 8;
                let millis = u64::from_le_bytes(bytes);
                Some(UNIX_EPOCH + Duration::from_millis(millis))
            }
            EXPIRY_S_FLAG => {
                self.index += 1;
                let bytes: [u8; 4] = self.data[self.index..self.index + 4].try_into().unwrap();
                self.index += 4;
                let secs = u32::from_le_bytes(bytes) as u64;
                Some(UNIX_EPOCH + Duration::from_secs(secs))
            }
            _ => None,
        }
    }
}
