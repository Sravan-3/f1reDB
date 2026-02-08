// TCP server
use std::net::{TcpListener, TcpStream};
use std::io::{Read, Write};
use crate::protocol;

pub fn start(address: &str){

    let listener  = TcpListener::bind(address).expect("Failed to bind");

    println!("f1reDB is listing on {}", address);

    for stream in listener.incoming(){
        match stream {
            Ok(stream) => {
                handle_client(stream)
            }
            Err(e) => {
                eprintln!("Connection failed: {}", e);
            }
        }
    }

}

pub fn handle_client(mut stream: TcpStream){

    let mut buffer =[0; 1024];
    let mut pending= Vec::new();

    loop{

        let n = match stream.read(&mut buffer) {

            Ok(0) => return,
            Ok(n) => n,
            Err(_) => return
        };

        pending.extend_from_slice(&buffer[..n]);

        while let Some(pos) = pending.iter().position(|&b| b == b'\n') {

            let line = pending.drain(..=pos).collect::<Vec<u8>>();
            let line = String::from_utf8_lossy(&line);

            match protocol::parse_line(&line) {
                Ok(cmd) => {
                    println!("Parsed: {:?}", cmd);
                    stream.write_all(b"OK\n").unwrap();
                }

                Err(e) => {
                    let msg = format!("ERROR {}\n", e);
                    stream.write_all(msg.as_bytes()).unwrap();
                }
            }
        }
    }


}


