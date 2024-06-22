use crate::resp::RespType;

#[derive(Debug)]
pub enum Command {
    Ping,
    Echo(String),
    Set(String, String, Option<u64>),
    Get(String),
    Info(String),
    ReplConf(String, Option<String>),
    Psync(String, String),
    Wait(i32, i32),
    ConfigGet(String),
    Keys(String),
}

impl Command {
    pub fn is_write(&self) -> bool {
        match self {
            Command::Set(_, _, _) => true,
            _ => false,
        }
    }
}

// Public
pub fn args_to_command(command_name: &str, args: Vec<RespType>) -> Command {
    match command_name.to_lowercase().as_str() {
        "echo" => create_echo(args),
        "ping" => create_ping(args),
        "set" => create_set(args),
        "info" => create_info(args),
        "get" => create_get(args),
        "replconf" => create_replconf(args),
        "psync" => create_psync(args),
        "wait" => create_wait(args),
        "config" => create_config(args),
        "keys" => create_key(args),
        other @ _ => panic!("No support for command type: {}", other),
    }
}

// Private
fn turn_arg_to_string(arg: &RespType) -> Option<String> {
    match arg {
        RespType::BulkString(x) => Some(String::from(x.as_ref().unwrap())),
        RespType::SimpleString(x) => Some(String::from(x)),
        _ => None,
    }
}

fn create_set(args: Vec<RespType>) -> Command {
    match &args.len() {
        2 | 4 => (),
        _ => panic!("Number of args for SET is wrong"),
    }

    let mut string_args = Vec::new();
    for arg in args.iter().take(2) {
        match turn_arg_to_string(arg) {
            Some(x) => string_args.push(x),
            None => panic!("First two arguments for SET need to be strings"),
        }
    }
    let optional_arg = if args.len() == 4 {
        match turn_arg_to_string(&args[2]) {
            Some(x) if x == "px" => (),
            _ => panic!("Expected third argument to SET to be px"),
        }
        match turn_arg_to_string(&args[3]) {
            Some(x) => match x.parse::<u64>() {
                Ok(val) => Some(val),
                Err(e) => panic!("Failed to convert px argument of SET to u64: {}", e),
            },
            None => panic!("Expected px argument to be provided as a string"),
        }
    } else {
        None
    };

    Command::Set(string_args[0].clone(), string_args[1].clone(), optional_arg)
}

fn create_get(args: Vec<RespType>) -> Command {
    match &args.len() {
        1 => (),
        _ => panic!("Number of arguments for GET is wrong"),
    };
    let arg_value = match turn_arg_to_string(&args[0]) {
        Some(x) => x,
        None => panic!("Expected GET argument to be a string"),
    };
    Command::Get(arg_value)
}

fn create_echo(args: Vec<RespType>) -> Command {
    match &args.len() {
        1 => (),
        _ => panic!("Number of arguments for ECHO is wrong"),
    };
    let arg_value = match &args[0] {
        RespType::BulkString(x) => String::from(x.as_ref().unwrap()),
        _ => panic!("Expect echo command to have a bulkstring as an argument"),
    };
    Command::Echo(arg_value)
}

fn create_replconf(args: Vec<RespType>) -> Command {
    match &args.len() {
        1 | 2 => (),
        _ => panic!("Number of arguments for REPLCONF is wrong"),
    };
    let arg1 = match turn_arg_to_string(&args[0]) {
        Some(x) => x,
        None => panic!("Expected first argument for REPLCONF to be a string"),
    };
    let optional_arg = if args.len() == 2 {
        match turn_arg_to_string(&args[1]) {
            Some(x) => Some(x),
            None => panic!("Expected second argument for REPLCFONF to be a string"),
        }
    } else {
        None
    };
    Command::ReplConf(arg1.clone(), optional_arg)
}

fn create_psync(args: Vec<RespType>) -> Command {
    match &args.len() {
        2 => (),
        _ => panic!("Number of arguments for PSYNC is wrong"),
    };
    let mut string_args = Vec::new();
    for arg in args.iter().take(2) {
        match turn_arg_to_string(arg) {
            Some(x) => string_args.push(x),
            None => panic!("First two arguments for SET need to be strings"),
        }
    }
    Command::Psync(string_args[0].clone(), string_args[1].clone())
}

fn create_ping(args: Vec<RespType>) -> Command {
    match &args.len() {
        0 => (),
        _ => panic!("Number of arguments for PING is wrong"),
    };
    Command::Ping
}

fn create_info(args: Vec<RespType>) -> Command {
    match &args.len() {
        1 => (),
        _ => panic!("Number of arguments for INFO is wrong"),
    };
    let arg_value = match turn_arg_to_string(&args[0]) {
        Some(x) => x,
        None => panic!("Expected first argument for INFO to be a string"),
    };
    Command::Info(arg_value)
}

fn create_wait(args: Vec<RespType>) -> Command {
    match &args.len() {
        2 => (),
        _ => panic!("Number of arguments for WAIT is wrong"),
    };
    let mut arg_values: Vec<i32> = Vec::new();
    for arg in args {
        match turn_arg_to_string(&arg) {
            Some(x) => match x.parse::<i32>() {
                Ok(x) => arg_values.push(x),
                Err(_) => panic!("Could not parse WAIT arguments to i32"),
            },
            None => panic!("Arguments to WAIT should be strings"),
        }
    }
    Command::Wait(arg_values[0].clone(), arg_values[1].clone())
}

fn create_config(args: Vec<RespType>) -> Command {
    match &args.len() {
        2 => (),
        _ => panic!("Number of arguments for CONFIG GET is wrong"),
    }
    match turn_arg_to_string(&args[0]) {
        Some(x) if x.as_str().to_lowercase() == "get" => (),
        _ => panic!("Expected CONFIG to be followed by GET"),
    };
    let arg_value = match turn_arg_to_string(&args[1]) {
        Some(x) => x,
        None => panic!("Expected argument for CONFIG GET to be a string"),
    };

    Command::ConfigGet(arg_value)
}

fn create_key(args: Vec<RespType>) -> Command {
    match &args.len() {
        1 => (),
        _ => panic!("Number of arguments for KEYS is wrong"),
    }
    let arg_value = match turn_arg_to_string(&args[0]) {
        Some(x) => x,
        None => panic!("Expected argument for KEYS to be a string"),
    };
    Command::Keys(arg_value)
}
