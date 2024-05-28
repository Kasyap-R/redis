// Uncomment this block to pass the first stage
use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        let _handle = thread::spawn(move || {
            match stream {
                Ok(mut stream) => {
                    println!("accepted new connection");
                    let mut buf = [0; 512];
                    loop {
                        // num_bytes is the num of bytes sent
                        let num_bytes = stream.read(&mut buf).unwrap();
                        if num_bytes == 0 {
                            break;
                        }
                        stream.write(b"+PONG\r\n").unwrap();
                    }
                }
                Err(e) => {
                    println!("error: {}", e);
                }
            }
        });
    }
}
