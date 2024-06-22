use std::collections::HashMap;

pub struct RdbParser {
    data: Vec<u8>,
    index: usize,
}

const EOF: u8 = 0xff;

// Public
impl RdbParser {
    pub fn new(data: Vec<u8>) -> Self {
        Self { data, index: 0 }
    }

    pub fn rdb_to_db(&mut self) -> HashMap<String, String> {
        self.parse_header();
        self.parse_metadata();
        let mut database: HashMap<String, String> = HashMap::new();
        while self.data[self.index] != EOF {
            println!("Reading KEY-Value");
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
        // Skip over RESIZE DB Field
        self.index += 3;
    }

    fn parse_key_value(&mut self) -> (String, String) {
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

        (key.to_string(), value.to_string())
    }
}
