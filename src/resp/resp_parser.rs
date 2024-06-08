use super::RespType;
use crate::redis::command::Command;

pub struct RespParser {
    raw_data: String,
    length: usize,
    num_args: usize,
    index: usize,
}

impl RespParser {
    pub fn new(raw_data: String) -> RespParser {
        let mut instance = RespParser {
            raw_data: raw_data.clone(),
            num_args: 0,
            length: raw_data.len(),
            index: 0,
        };
        instance.initialize();
        instance
    }

    fn initialize(&mut self) {
        self.index = match self.raw_data.find('*') {
            Some(x) => x + 1,
            None => panic!("Could not find asterisk"),
        };
        self.num_args = self.raw_data[self.index..=self.index]
            .parse::<usize>()
            .expect("Could not convert num_args argument into usize")
            - 1;
        self.index = match self.raw_data.find("\r\n") {
            Some(x) => x + 2,
            None => panic!("Could not find CLRF in command"),
        };
    }

    // Determine the command, and parse the num of arguments
    pub fn parse_command(&mut self) -> Command {
        let command_name = self.process_bulk_string();
        let mut data: Vec<RespType> = Vec::new();
        for _ in 0..self.num_args {
            if self.index >= self.length {
                panic!("Index is at or past length of array while parsing arguments");
            }
            match self.raw_data.chars().nth(self.index).unwrap() {
                '$' => data.push(self.process_bulk_string()),
                '+' => data.push(self.process_simple_string()),
                _ => panic!("Unsupported RESP data type encountered"),
            }
        }
        let command = match command_name {
            RespType::BulkString(x) => Command::string_to_command(x.unwrap(), data),
            RespType::SimpleString(x) => Command::string_to_command(x, data),
            _ => panic!("Expected string following initial array indicating command"),
        };

        command
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

    fn process_simple_string(&mut self) -> RespType {
        self.expect_next_char('+')
            .expect("Failed to find ping indicator byte (+)");
        self.index += 1;
        let simple_string = self.read_data_till_crlf().to_string();
        RespType::SimpleString(simple_string)
    }

    fn process_bulk_string(&mut self) -> RespType {
        self.expect_next_char('$')
            .expect("Failed to find bulk string indicator byte");
        self.index += 1;
        let length: i32 = self
            .read_data_till_crlf()
            .parse()
            .expect("Failed to convert length of bulk string to usize");
        if length == -1 {
            return RespType::BulkString(None);
        }
        let bulk_string = self.read_data_till_crlf().to_string();
        if bulk_string.len() as i32 != length {
            panic!("Provided length for bulk string is incorrect");
        }
        RespType::BulkString(Some(bulk_string))
    }

    pub fn read_data_till_crlf(&mut self) -> &str {
        let curr_data = &self.raw_data[self.index..];
        let terminal_index = match curr_data.find("\r\n") {
            Some(x) => self.index + x,
            None => panic!("Could not find carriage return"),
        };
        let data_string = &self.raw_data[self.index..terminal_index];
        if terminal_index < self.length - 2 {
            self.index = terminal_index + 2;
        } else {
            self.index = self.length;
        }
        &data_string
    }
}
