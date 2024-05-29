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

    /* let mut parser = RespParser::new(String::from("*2\r\n$4\r\nEcho\r\n$3\r\nhey\r\n"));
    println!("{}", parser.parse_command()); */

    for stream in listener.incoming() {
        let _handle = thread::spawn(move || match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                let mut buf = [0; 512];
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
