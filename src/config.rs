use std::env;

pub struct Config {
    pub port: String,
    pub role: String,
}

impl Config {
    pub fn parse() -> Self {
        let args: Vec<String> = env::args().collect();
        let mut config = Config {
            port: String::from("6379"),
            role: String::from("master"),
        };
        let mut index = 0;
        while index < args.len() {
            match args[index].as_str() {
                "--port" => {
                    if index + 1 < args.len() {
                        config.port = args[index + 1].to_owned();
                        index += 1; // Skip the next argument since it's the value for --port
                    } else {
                        panic!("Error: --port requires a value");
                    }
                }
                "--replicaof" => {
                    if index + 1 < args.len() {
                        config.role = args[index + 1].to_owned();
                        index += 1; // Skip the next argument since it's the value for --port
                    } else {
                        panic!("Error: --replicaof requires a value");
                    }
                }
                _ => {}
            }
            index += 1; // Move to the next argument
        }
        config
    }
}
