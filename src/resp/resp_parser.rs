use super::RespType;
use crate::redis::command::Command;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::sync::RwLock;

pub struct RespParser {
    pub raw_data: String,
    length: usize,
    index: usize,
    stream: Arc<RwLock<TcpStream>>,
}

impl RespParser {
    pub fn new(raw_data: String, stream: Arc<RwLock<TcpStream>>) -> RespParser {
        let mut instance = RespParser {
            raw_data: raw_data.clone(),
            length: raw_data.len(),
            index: 0,
            stream,
        };
        instance.initialize();
        instance
    }

    fn initialize(&mut self) {}

    // Determine the command, and parse the num of arguments
    pub async fn parse_command(&mut self) -> Option<Command> {
        assert!(self.index == 0);
        // first check for the asterisk
        let num_args = match self.find_num_args_in_array().await {
            Some(x) => x,
            None => return None,
        };
        if !self.validate_array_length(num_args).await {
            return None;
        }
        let command_name = self.process_bulk_string().await;
        let mut data: Vec<RespType> = Vec::new();
        for _ in 0..num_args {
            if self.index >= self.length {
                panic!("Index is at or past length of array while parsing arguments");
            }
            match self.raw_data.chars().nth(self.index).unwrap() {
                '$' => data.push(self.process_bulk_string().await),
                '+' => data.push(self.process_simple_string().await),
                _ => panic!("Unsupported RESP data type encountered"),
            }
        }
        let command = match command_name {
            RespType::BulkString(x) => Command::string_to_command(x.unwrap(), data),
            RespType::SimpleString(x) => Command::string_to_command(x, data),
            _ => panic!("Expected string following initial array indicating command"),
        };

        Some(command)
    }

    fn check_next_chars(&self, substring: &str) -> bool {
        let substring_len = substring.len();
        if self.index + substring_len <= self.length {
            return &self.raw_data[self.index..self.index + substring.len()] == substring;
        }
        false
    }

    fn expect_next_char(&self, expected: char) -> Result<(), &'static str> {
        if self.check_next_chars(expected.to_string().as_str()) {
            Ok(())
        } else {
            Err("Expected Char not found")
        }
    }

    // Command processing functions assume the command indicator byte is present (i.e. they panic
    // if it isn't present instead of reading the stream)
    pub async fn process_simple_string(&mut self) -> RespType {
        println!("Raw Data: {}", &self.raw_data);
        self.expect_next_char('+')
            .expect("Failed to find ping indicator byte (+)");
        self.index += 1;
        let simple_string = self.read_data_till_crlf().await.to_string();
        self.reset_data();
        RespType::SimpleString(simple_string)
    }

    fn reset_data(&mut self) {
        self.raw_data = self.raw_data[self.index..].to_string();
        self.index = 0;
        self.length = self.raw_data.len();
    }

    pub async fn process_bulk_string(&mut self) -> RespType {
        self.expect_next_char('$')
            .expect("Failed to find bulk string indicator byte");
        self.index += 1;
        let length: i32 = self
            .read_data_till_crlf()
            .await
            .parse()
            .expect("Failed to convert length of bulk string to usize");
        if length == -1 {
            return RespType::BulkString(None);
        }
        let bulk_string = self.read_data_till_crlf().await.to_string();
        if bulk_string.len() as i32 != length {
            panic!("Provided length for bulk string is incorrect");
        }
        self.reset_data();
        RespType::BulkString(Some(bulk_string))
    }

    async fn read_data_till_crlf(&mut self) -> String {
        let index_snapshot = self.index;
        let mut remainder = self.raw_data[self.index..].to_string();
        while self.index >= self.length || !remainder.contains("\r\n") {
            match self.read_from_stream().await {
                Ok(_) => (),
                Err(_) => panic!("Stream connection ended while reading to CRLF"),
            }
            remainder = self.raw_data[self.index..].to_string();
        }
        if let Some(x) = remainder.find("\r\n") {
            self.index = x + 2;
            return self.raw_data[index_snapshot..x].to_string();
        } else {
            panic!("Could not find CRLF");
        }
    }

    async fn read_from_stream(&mut self) -> Result<(), ()> {
        let mut stream = self.stream.write().await;
        let mut buffer: [u8; 1024] = [0; 1024];

        let stream_data = match stream.read(&mut buffer).await {
            Ok(0) => None,
            Ok(_) => {
                Some(String::from_utf8(buffer.to_vec()).expect("Expected valid utf-8 sequence"))
            }
            Err(e) => panic!("Stream returned error: {:?}", e),
        };
        if let Some(ref x) = stream_data {
            let len = x.len();
            self.length += len;
            self.raw_data += x;
            println!("====== Receiving new transmission ======");
            println!("{}", x);
            println!("====== End of new transmission ======");
            Ok(())
        } else {
            Err(())
        }
    }

    async fn validate_array_length(&mut self, num_args: usize) -> bool {
        // We count how deep in an array we are, anything within that doesn't count
        // iterate through th string, each time we counter an asterisk
        // For each array we find, we add num_args to the number of neccesary crlfs to find
        // once we
        // The parameter is the number we parse for initially and we just add to that as we go,
        // finishing when we run out of data or when the nums_args amount goes to 0
        // The goal of this is to make sure the array is large enough to extract everything we need
        if num_args == 0 {
            return true;
        }
        let index_snapshot = self.index;
        let mut num_crlfs = num_args;
        let mut remainder = String::from(&self.raw_data[self.index..self.length]);
        while num_crlfs > 0 || self.index >= self.length {
            match self.expect_next_char('*') {
                Ok(_) => match self.find_num_args_in_array().await {
                    Some(x) => num_crlfs += x,
                    None => return false,
                },
                Err(_) => (),
            };
            if let Some(x) = remainder.as_str().find("\r\n") {
                num_crlfs -= 1;
                // We might have to do an
                self.index = x + 2;
                remainder = self.raw_data[self.index..].to_string();
            }
        }
        self.index = index_snapshot;
        true
    }

    async fn find_num_args_in_array(&mut self) -> Option<usize> {
        let mut remainder = self.raw_data[self.index..].to_string();
        while !remainder.contains("*") {
            match self.read_from_stream().await {
                Ok(_) => (),
                Err(_) => return None,
            }
            remainder = self.raw_data[self.index..].to_string();
        }
        if !remainder.contains("*") {
            panic!("Could not find asterisk even after reading stream")
        }
        self.index += 1;
        remainder = self.raw_data[self.index..].to_string();
        while !remainder.contains("\r\n") {
            match self.read_from_stream().await {
                Ok(_) => (),
                Err(_) => return None,
            }
        }
        let crlf_index = self.raw_data.find("\r\n").unwrap();
        let num_args: usize = self.raw_data[self.index..crlf_index]
            .parse()
            .expect("Could not parse nums arg into usize");
        while num_args > 0 && self.length <= (crlf_index + 2) {
            match self.read_from_stream().await {
                Ok(_) => (),
                Err(_) => return None,
            }
        }
        self.index = crlf_index + 2;
        Some(num_args)
    }
}
