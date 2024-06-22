use std::collections::HashMap;

pub struct RdbParser {
    data: Vec<u8>,
    index: usize,
}

enum OpCodes {}

// Public
impl RdbParser {
    pub fn new(data: Vec<u8>) -> Self {
        Self { data, index: 0 }
    }

    pub fn rdb_to_db(&mut self) -> HashMap<String, String> {
        let fb_pos = self.data.iter().position(|&b| b == 0xfb).unwrap();
        let mut pos = fb_pos + 4;
        let len = self.data[pos];
        pos += 1;
        let key = &self.data[pos..(pos + len as usize)];
        println!("1 {:x?}", key);
        let pars = std::str::from_utf8(key).unwrap();
        println!("2 {:?}", pars);
        let mut database: HashMap<String, String> = HashMap::new();
        database.insert(String::from(pars), "0".to_string());
        database
    }
}

// Private
/* fn parse_header();
fn parse_metadata();
fn parse();
fn parse_key_value(); */
