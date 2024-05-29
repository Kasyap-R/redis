use std::collections::HashMap;
// Uncomment this block to pass the first stage
use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;

pub mod resp_parser;
use crate::resp_parser::*;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        let _handle = thread::spawn(move || match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                let mut buf = [0; 512];
                let mut database: HashMap<String, String> = HashMap::new();
                loop {
                    let num_bytes = stream.read(&mut buf).unwrap();
                    if num_bytes == 0 {
                        break;
                    }
                    println!(
                        "{}",
                        String::from_utf8(buf.to_vec()).expect("Invalid UTF-8 sequence")
                    );
                    let mut parser = RespParser::new(
                        String::from_utf8(buf.to_vec()).expect("Invalid UTF-8 sequence"),
                    );
                    let command = parser.parse_command();
                    match command {
                        Command::Echo(x) => {
                            let mut response = "$".to_string();
                            response.push_str(&format!("{}\r\n{}\r\n", x.len(), x));
                            stream.write(response.as_bytes()).unwrap();
                        }
                        Command::Ping => {
                            let response = "+PONG\r\n".to_string();
                            stream.write(response.as_bytes()).unwrap();
                        }
                        Command::Set(x, y) => {
                            database.insert(x, y);
                            let response = "+OK\r\n".to_string();
                            stream.write(response.as_bytes()).unwrap();
                        }
                        Command::Get(x) => {
                            let mut response = "".to_string();
                            match database.get(&x) {
                                Some(y) => response.push_str(&format!("${}\r\n{}\r\n", y.len(), y)),
                                None => response.push_str(&format!("$-1\r\n")),
                            }
                            stream.write(response.as_bytes()).unwrap();
                        }
                        _ => panic!("Unsupported Command"),
                    };
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        });
    }
}
