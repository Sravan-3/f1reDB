// TCP server
use std::net::{TcpListener, TcpStream};
use std::io::{Read, Write};

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

    loop{
        match stream.read(&mut buffer) {

            Ok(0) => return,

            Ok(n) => {
                let request = String::from_utf8_lossy(&buffer[..n]); 
                println!("Received: {}", request);

                stream.write_all(b"OK\n").unwrap();
            }
            Err(_) => {
                return
            }
        }
    }


}


