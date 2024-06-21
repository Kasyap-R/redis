use crate::redis::RedisState;
use std::{env, path::PathBuf};

pub struct Config {
    pub port: String,
    pub role: RedisState,
    pub master_replid: Option<String>,
    pub master_repl_offset: Option<String>,
    pub master_port: Option<String>,
    pub master_host: Option<String>,
    pub rdb_dir: Option<PathBuf>,
    pub rdb_filename: Option<PathBuf>,
}

enum ConfigParseError {
    NoArgFound,
}

impl Config {
    pub fn parse() -> Self {
        let args: Vec<String> = env::args().collect();
        let mut config = Config {
            port: String::from("6379"),
            role: RedisState::Master,
            master_replid: None,
            master_repl_offset: None,
            master_port: None,
            master_host: None,
            rdb_dir: None,
            rdb_filename: None,
        };
        let mut index = 0;
        while index < args.len() {
            match args[index].as_str() {
                "--port" => match read_next_arg(&args, &mut index) {
                    Ok(x) => {
                        config.port = x;
                    }
                    Err(ConfigParseError::NoArgFound) => {
                        panic!("Error: --port requires a value");
                    }
                },
                "--replicaof" => match read_next_arg(&args, &mut index) {
                    Ok(x) => {
                        let parts: Vec<&str> = x.split(" ").collect();
                        config.master_host = Some(parts[0].to_string());
                        config.master_port = Some(parts[1].to_string());
                        config.role = RedisState::Replica;
                    }
                    Err(ConfigParseError::NoArgFound) => {
                        panic!("Error: --replicaof requires two values");
                    }
                },
                "--dir" => match read_next_arg(&args, &mut index) {
                    Ok(x) => {
                        config.rdb_dir = Some(PathBuf::from(x));
                    }
                    Err(ConfigParseError::NoArgFound) => {
                        panic!("Error: --dir requires a value");
                    }
                },
                "--dbfilename" => match read_next_arg(&args, &mut index) {
                    Ok(x) => config.rdb_filename = Some(PathBuf::from(x)),
                    Err(ConfigParseError::NoArgFound) => {
                        panic!("Error: --dbfilename requires a value");
                    }
                },
                _ => {}
            }
            index += 1; // Move to the next argument
        }
        if config.role == RedisState::Master {
            config.master_replid = Some(String::from("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"));
            config.master_repl_offset = Some(String::from("0"));
        }
        config
    }
}

fn read_next_arg(args: &Vec<String>, curr_index: &mut usize) -> Result<String, ConfigParseError> {
    if *curr_index + 1 >= args.len() {
        return Err(ConfigParseError::NoArgFound);
    }
    *curr_index += 1;
    Ok(args[*curr_index].clone())
}
