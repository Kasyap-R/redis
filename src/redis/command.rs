use crate::resp::RespType;

#[derive(Debug)]
pub enum Command {
    Ping,
    Echo(String),
    Set(String, String, Option<u64>),
    Get(String),
    Info(String),
    ReplConf(String, String),
    Psync(String, String),
}

impl Command {
    pub fn is_write(&self) -> bool {
        match self {
            Self::Ping => false,
            Self::Echo(_) => false,
            Self::Set(_, _, _) => true,
            Self::Get(_) => false,
            Self::Info(_) => false,
            Self::ReplConf(_, _) => false,
            Self::Psync(_, _) => false,
        }
    }
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

    fn handle_replconf(args: Vec<RespType>) -> Command {
        let len_args = args.len();
        let mut strings: [String; 2] = Default::default();
        if len_args != 2 {
            panic!("Inappropriate Number of arguments for the REPLCONF command");
        }
        for (index, arg) in args.iter().take(2).enumerate() {
            match arg {
                RespType::BulkString(x) => {
                    strings[index] = String::from(x.as_ref().unwrap());
                }
                RespType::SimpleString(x) => {
                    strings[index] = String::from(x);
                }
                _ => panic!("Arguments for REPLCONF need to be strings"),
            }
        }
        Command::ReplConf(strings[0].clone(), strings[1].clone())
    }
    pub fn string_to_command(command: String, args: Vec<RespType>) -> Command {
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
                    panic!("Inappropriate Number of arguments for the GET command");
                }
                match &args[0] {
                    RespType::SimpleString(x) => Command::Get(x.to_string()),
                    RespType::BulkString(x) => Command::Get(String::from(x.as_ref().unwrap())),
                    _ => panic!("Argument for GET needs to be a string"),
                }
            }
            "replconf" => Command::handle_replconf(args),
            "psync" => {
                if len_args != 2 {
                    panic!("Inappropriate Number of arguments for the PSYNC command");
                }
                let mut strings: [String; 2] = Default::default();
                for (index, arg) in args.iter().take(2).enumerate() {
                    match arg {
                        RespType::BulkString(x) => {
                            strings[index] = String::from(x.as_ref().unwrap());
                        }
                        RespType::SimpleString(x) => {
                            strings[index] = String::from(x);
                        }
                        _ => panic!("Arguments for REPLCONF need to be strings"),
                    }
                }
                Command::Psync(strings[0].clone(), strings[1].clone())
            }

            other @ _ => panic!("No support for command type: {}", other),
        }
    }
}
