#[derive(Debug)]
pub enum Command {
    Ping,
    Echo(String),
    Set(String, String, Option<u64>),
    Get(String),
    Info(String),
}

impl Command {
    fn handle_set(args: Vec<RespType>) -> Command {
        let len_args = args.len();
        let mut strings: [String; 2] = Default::default();
        for (index, arg) in args.iter().take(2).enumerate() {
            match arg {
                RespType::BulkString(x) => {
                    strings[index] = String::from(x.as_ref().unwrap());
                }
                RespType::SimpleString(x) => {
                    strings[index] = String::from(x);
                }
                _ => panic!("Arguments for SET need to be strings"),
            }
        }
        let mut curr_arg_index = 2;
        if len_args == 2 {
            Command::Set(strings[0].clone(), strings[1].clone(), None)
        } else if len_args == 4 {
            let optional_argument_name = match &args[curr_arg_index] {
                RespType::BulkString(x) => String::from(x.as_ref().unwrap()),
                RespType::SimpleString(x) => x.to_owned(),
                _ => panic!("Third argument to SET must be a string"),
            };
            curr_arg_index += 1;
            println!("{}", optional_argument_name.to_lowercase().as_str());
            let optionl_arg = match optional_argument_name.to_lowercase().as_str() {
                "px" => match &args[curr_arg_index] {
                    RespType::BulkString(x) => String::from(x.as_ref().unwrap()),
                    RespType::SimpleString(x) => x.to_owned(),
                    _ => panic!("PX argument for SET must be a string"),
                },
                _ => panic!("Unsupported optional argument name to SET"),
            };

            Command::Set(
                strings[0].clone(),
                strings[1].clone(),
                Some(optionl_arg.parse::<u64>().unwrap()),
            )
        } else {
            panic!("Inappropriate Number of arguments for the SET command");
        }
    }
    fn string_to_command(command: String, args: Vec<RespType>) -> Command {
        let len_args = args.len();
        match command.to_lowercase().as_str() {
            "echo" => {
                if len_args != 1 {
                    panic!("Inappropriate Number of arguments for ECHO command");
                }
                match &args[0] {
                    RespType::BulkString(x) => Command::Echo(String::from(x.as_ref().unwrap())),
                    _ => panic!("Expect echo command to have a bulkstring as an argument"),
                }
            }
            "ping" => {
                if len_args != 0 {
                    panic!("Inappropriate Number of arguments for the PING command");
                }
                Command::Ping
            }
            "set" => Command::handle_set(args),
            "info" => {
                if len_args != 1 {
                    panic!("Inappropriate Number of arguments for the SET command");
                }
                match &args[0] {
                    RespType::SimpleString(x) => Command::Info(x.to_string()),
                    RespType::BulkString(x) => Command::Info(String::from(x.as_ref().unwrap())),
                    _ => panic!("Argument for INFO needs to be a string"),
                }
            }
            "get" => {
                if len_args != 1 {
                    panic!("Inappropriate Number of arguments for the SET command");
                }
                match &args[0] {
                    RespType::SimpleString(x) => Command::Get(x.to_string()),
                    RespType::BulkString(x) => Command::Get(String::from(x.as_ref().unwrap())),
                    _ => panic!("Argument for GET needs to be a string"),
                }
            }

            other @ _ => panic!("No support for command type: {}", other),
        }
    }
}

#[derive(Debug)]
enum RespType {
    Integer(i64),
    SimpleString(String),
    Error(String),
    BulkString(Option<String>),
    Array(Vec<RespType>),
}

pub struct RespParser {
    pub raw_data: String,
    length: usize,
    num_args: usize,
    pub command: Command,
    index: usize,
}

impl RespParser {
    pub fn new(raw_data: String) -> RespParser {
        let mut instance = RespParser {
            raw_data: raw_data.clone(),
            num_args: 0,
            command: Command::Ping,
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
            _ => panic!("Expected string following initial array"),
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

    fn read_data_till_crlf(&mut self) -> &str {
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
