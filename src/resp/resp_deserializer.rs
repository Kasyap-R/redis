use super::RespType;
use crate::redis::command::Command;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::sync::RwLock;

pub struct RespParser {
    data: String,
    index: usize,
    stream: Arc<RwLock<TcpStream>>,
}

impl RespParser {
    // Public Interface
    pub fn new(data: String, stream: Arc<RwLock<TcpStream>>) -> RespParser {
        RespParser {
            data,
            index: 0,
            stream,
        }
    }
    pub async fn parse_command(&mut self) -> Option<Command> {
        assert!(self.index == 0);
        let num_args = match self.find_num_args_in_array().await {
            Some(x) => x,
            None => return None,
        };

        if !self.validate_array_length(num_args).await {
            return None;
        }
        // Assume command_name will always be provided as a bulk string
        let command_name = self.parse_bulk_string();
        let mut command_data: Vec<RespType> = Vec::new();
        for _ in 1..=(num_args - 1) {
            if self.index > self.data.len() {
                panic!("Index is >= length of data while parsing arguments");
            }
            match self.data.chars().nth(self.index).unwrap() {
                '$' => command_data.push(self.parse_bulk_string()),
                '+' => command_data.push(self.parse_bulk_string()),
                other @ _ => panic!("Unsupported RESP data type encountered: {}", other),
            }
        }
        let command = match command_name {
            RespType::BulkString(x) => Command::string_to_command(x.unwrap(), command_data),
            _ => panic!("Expected Command Name to be provided as a bulk string"),
        };
        self.reset_data();

        Some(command)
    }

    pub async fn parse_handshake(&mut self) -> (String, Vec<u8>) {
        assert!(self.index == 0);
        // First parse the simple string
        while !self.data.contains("\r\n") {
            self.read_data_from_stream().await;
        }
        let simple = self.parse_simple_string();
        self.reset_data();

        // Then parse the RDB file
        while !self.data.contains("\r\n") {
            self.read_data_from_stream().await;
        }
        if !self.check_next_substring("$") {
            panic!("Expected bulk string indicator byte before RDB file");
        }
        self.index += 1;
        let length: usize = self
            .read_to_crlf()
            .parse()
            .expect("Failed to convert RDB length to usize");
        let mut remainder = &self.data[self.index..];

        println!("Length of RDB: {}", length);
        println!("Length of remainder: {}", remainder.len());
        // Now we must read the next x chars
        let num_bytes: usize = 120;
        while remainder.len() < length {
            self.read_data_from_stream().await;
            remainder = &self.data[self.index..];
        }

        let rdb = remainder[0..num_bytes].as_bytes().to_vec();
        self.index += num_bytes;

        let resync = match simple {
            RespType::SimpleString(x) => x,
            _ => panic!("Expected parse_simple_string to return a simple string"),
        };
        self.reset_data();
        println!("Data after parsing RDB: {}", self.data);
        println!("Length of data after parsing RDB: {}", self.data.len());
        (resync, rdb)
    }

    // ----------------- Private -----------------
    // |                                         |
    // -------------------------------------------

    async fn read_data_from_stream(&mut self) -> Option<usize> {
        let mut stream = self.stream.write().await;
        let mut buffer: [u8; 1024] = [0; 1024];
        match stream.read(&mut buffer).await {
            // This should return none in future, panic is for debugging
            Ok(0) => panic!("Stream was closed"),
            Ok(bytes_read) => {
                let valid_data = &buffer[..bytes_read];
                let string_data = String::from_utf8_lossy(valid_data).to_string();
                self.data += &string_data;
                println!("========Recieved New Transmission========");
                println!("{}", string_data);
                println!("========End of New Transmission========");
                Some(bytes_read)
            }
            Err(_) => {
                // In future, this function should return a result type
                panic!("Error reading from stream");
            }
        }
    }

    // Function assumes the data has been reset and leads with an asterisk
    async fn find_num_args_in_array(&mut self) -> Option<usize> {
        // I can change this to work from current index but for now I just want to check
        // my assumption that it will only be called by parse_command
        assert!(self.index == 0);
        while !self.data.contains("\r\n") {
            self.read_data_from_stream().await;
        }
        if !self.check_next_substring("*") {
            panic!("Expected first character to be * while finding num args");
        }
        self.index += 1;
        let num_args: i32 = self
            .read_to_crlf()
            .parse()
            .expect("Could not parse num args into usize");
        if num_args <= 0 {
            panic!("Expected num_args to be > 0");
        }
        Some(num_args as usize)
    }
    async fn validate_array_length(&mut self, num_args: usize) -> bool {
        if num_args == 0 {
            panic!("Expected num_args to be greater than zero");
        }
        let mut num_crlfs = num_args;
        let mut remainder = self.data[self.index..].to_owned();
        let mut validating_index: usize = self.index;
        // Index might be greater than length after parsing num_args
        while num_crlfs > 0 || self.index >= self.data.len() {
            if self.check_next_substring("*") {
                match self.find_num_args_in_array().await {
                    Some(x) => num_crlfs += x,
                    None => return false,
                }
            }
            if let Some(x) = remainder.find("\r\n") {
                num_crlfs -= 1;
                validating_index += x + 2;
                remainder = self.data[validating_index..].to_owned();
            }
        }
        true
    }

    fn parse_simple_string(&mut self) -> RespType {
        if !self.check_next_substring("+") {
            panic!("Failed to find simple string indicator byte (+)");
        }
        self.index += 1;
        let simple_string = self.read_to_crlf();
        RespType::SimpleString(simple_string)
    }

    fn parse_bulk_string(&mut self) -> RespType {
        if self.data.chars().nth(self.index).unwrap() != '$' {
            panic!("Failed to find bulk string indicator byte $");
        }
        self.index += 1;
        let length: i32 = self
            .read_to_crlf()
            .parse()
            .expect("Could not convert Bulk String length to i32");
        if length == -1 {
            return RespType::BulkString(None);
        } else if length <= 0 {
            panic!("Expected bulk string length to be greater than 0");
        }
        let bulk_string = self.read_to_crlf();
        if bulk_string.len() != length as usize {
            panic!(
                "{}",
                format!(
                    "Bulk string length: {} did not match provided length: {}",
                    bulk_string.len(),
                    length
                )
            );
        }
        RespType::BulkString(Some(bulk_string))
    }

    fn check_next_substring(&self, sequence: &str) -> bool {
        let remainder = &self.data[self.index..];
        if sequence.len() > remainder.len() {
            return false;
        } else if &remainder[self.index..(self.index + sequence.len())] == sequence {
            return true;
        }
        false
    }

    fn reset_data(&mut self) {
        self.data = self.data[self.index..].to_string();
        self.index = 0;
    }

    fn read_to_crlf(&mut self) -> String {
        let remainder = &self.data[self.index..];
        let crlf_index: usize = remainder
            .find("\r\n")
            .expect("Could not find CRLF while reading to CRLF");
        self.index += crlf_index + 2;
        remainder[0..crlf_index].to_owned()
    }
}
