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
        self.parse_header();
        self.parse_metadata();
        // Skip over RESIZE DB Field
        self.index += 4;
        let mut database: HashMap<String, String> = HashMap::new();
        while self.data[self.index] != 0xfe {
            let (key, value) = self.parse_key_value();
            database.insert(key, value);
        }
        database
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
    }
    fn parse_key_value(&mut self) -> (String, String) {
        let length_key = self.data[self.index];
        self.index += 1;
        let key = &self.data[self.index..(self.index + length_key as usize)];
        let key = std::str::from_utf8(key).unwrap();
        self.index += length_key as usize;
        let length_value = self.data[self.index];
        let value = &self.data[self.index..(self.index + length_value as usize)];
        let value = std::str::from_utf8(value).unwrap();
        self.index += length_value as usize;
        (key.to_string(), value.to_string())
    }
}
