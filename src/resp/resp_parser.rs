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
        RespParser {
            raw_data: raw_data.clone(),
            length: raw_data.len(),
            index: 0,
            stream,
        }
    }

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
        self.reset_data();
        Some(command)
    }

    fn check_next_chars(&self, substring: &str) -> bool {
        let substring_len = substring.len();
        if self.index + substring_len <= self.length {
            println!(
                "Expected char: {}",
                &self.raw_data[self.index..self.index + substring.len()]
            );
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

    fn reset_data(&mut self) {
        self.raw_data = self.raw_data[self.index..].to_string();
        self.raw_data = self.raw_data.trim().to_string();
        self.index = 0;
        self.length = self.raw_data.len();
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

    pub async fn process_bulk_string(&mut self) -> RespType {
        assert!(self.index == 0);
        println!(
            "Data at the point of bulk string processing: {}",
            &self.raw_data
        );

        match self.expect_next_char('$') {
            Ok(_) => (),
            Err(_) => match self.read_from_stream().await {
                Ok(_) => (),
                Err(_) => panic!("Stream ended while reading for bulk string"),
            },
        }
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
        println!("Calculated length to be: {}", length);
        let bulk_string = self.read_data_till_crlf().await.to_string();
        /* if bulk_string.len() as i32 != length {
            panic!("Provided length for bulk string is incorrect");
        } */
        self.reset_data();
        println!("Data after bulk string processing: {}", &self.raw_data);
        RespType::BulkString(Some(bulk_string))
    }

    async fn read_data_till_crlf(&mut self) -> String {
        let index_snapshot = self.index;
        println!(
            "Snapshot and index + 1: {}",
            &self.raw_data[self.index..=self.index + 1]
        );
        let mut remainder = self.raw_data[self.index..].to_string();
        // TODO this might lead to infinite blocking if the stream isn't sending anything when
        // index is too high
        while self.index >= self.length || !remainder.contains("\r\n") {
            match self.read_from_stream().await {
                Ok(_) => (),
                Err(_) => panic!("Stream connection ended while reading to CRLF"),
            }
            remainder = self.raw_data[self.index..].to_string();
        }
        let crlf_index = remainder.find("\r\n").unwrap();
        self.index = crlf_index + 2;
        self.raw_data[index_snapshot..=crlf_index].to_owned()
    }

    pub async fn read_from_stream(&mut self) -> Result<(), &'static str> {
        let mut stream = self.stream.write().await;
        let mut buffer: [u8; 1024] = [0; 1024];

        match stream.read(&mut buffer).await {
            Ok(0) => {
                // No more data to read, might indicate a closed connection depending on your context
                Err("No data read; stream may be closed")
            }
            Ok(bytes_read) => {
                // Correctly handle the number of bytes read
                let valid_data = &buffer[..bytes_read];
                let stream_data = String::from_utf8_lossy(valid_data).to_string();
                self.raw_data += &stream_data;
                self.length += stream_data.len(); // Update length based on string length, not bytes read

                println!("====== Receiving new transmission ======");
                println!("{}", stream_data);
                println!("====== End of new transmission ======");

                Ok(())
            }
            Err(_) => {
                // Return a more descriptive error
                Err("Error reading from stream")
            }
        }
    }

    pub async fn process_rdb_file(&mut self) -> Vec<u8> {
        assert!(self.index == 0);
        println!("Data at the point of RDB processing: {}", &self.raw_data);

        match self.expect_next_char('$') {
            Ok(_) => (),
            Err(_) => match self.read_from_stream().await {
                Ok(_) => (),
                Err(_) => panic!("Stream ended while reading for RDB"),
            },
        }
        self.expect_next_char('$')
            .expect("Failed to find bulk string indicator byte while reading RDB");
        self.index += 1;
        let length: i32 = self
            .read_data_till_crlf()
            .await
            .parse()
            .expect("Failed to convert length of bulk string to usize");
        if length <= 0 {
            panic!("Expected length of RDB file to be > 0");
        }
        println!("Calculated length to be: {}", length);

        // Determining bounds of RDB file
        let mut raw_bytes = self.raw_data[self.index..self.length]
            .to_owned()
            .into_bytes();
        /* for (i, byte) in raw_bytes.clone().into_iter().enumerate() {
            if i >= (raw_bytes.len() - 1) {
                // TODO: I really need to figure out when to sto reading and realize a instance is
                // dead
                println!("Reading from stream as index is too high");
                match self.read_from_stream().await {
                    Ok(_) => (),
                    Err(_) => panic!("Stream ended while reading for more of RDB"),
                };
                continue;
            }
            if (i + 1) < raw_bytes.len()
                && byte == "\r".as_bytes()[0]
                && raw_bytes[i + 1] == "\n".as_bytes()[0]
            {
                println!("Found the CRLF at the end of the RDB");
                rdb_file = raw_bytes[0..i].to_vec();
                end_rdb_index = i + 2;
                break;
            }
        } */
        // We are checking for 88 bytes but we need 88 chars
        while String::from_utf8_lossy(&raw_bytes).len() < (length as usize - 1) {
            match self.read_from_stream().await {
                Ok(_) => (),
                Err(_) => panic!("Stream ended while reading for more of RDB"),
            };
            raw_bytes = self.raw_data[self.index..self.length]
                .to_owned()
                .into_bytes();
        }
        let rdb_file = raw_bytes[0..length as usize].to_vec();
        // resetting data
        let remaining_string =
            String::from_utf8_lossy(&raw_bytes[length as usize..raw_bytes.len()]);
        self.raw_data = remaining_string.to_string();
        self.index = 0;
        self.length = self.raw_data.len();

        /* if rdb_file.len() as i32 != length {
            panic!("Provided length for RDB file is incorrect");
        } */
        println!("Data after processing RDB file: {}", &self.raw_data);
        rdb_file
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
