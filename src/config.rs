use std::env;

pub struct Config {
    pub port: String,
    pub role: String,
    pub master_replid: Option<String>,
    pub master_repl_offset: Option<String>,
    pub master_port: Option<String>,
    pub master_host: Option<String>,
}

impl Config {
    pub fn parse() -> Self {
        let args: Vec<String> = env::args().collect();
        let mut config = Config {
            port: String::from("6379"),
            role: String::from("master"),
            master_replid: None,
            master_repl_offset: None,
            master_port: None,
            master_host: None,
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
                        let parts: Vec<&str> = args[index + 1].split(" ").collect();
                        config.master_host = Some(parts[0].to_string());
                        config.master_port = Some(parts[1].to_string());
                        config.role = String::from("slave");
                        index += 1; // Skip the next argument since it's the value for --port
                    } else {
                        panic!("Error: --replicaof requires two values");
                    }
                }
                _ => {}
            }
            index += 1; // Move to the next argument
        }
        if config.role == "master" {
            config.master_replid = Some(String::from("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"));
            config.master_repl_offset = Some(String::from("0"));
        }
        config
    }
    pub fn is_master(&self) -> bool {
        self.role == "master"
    }
}
