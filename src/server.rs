// TCP server
use std::net::{TcpListener, TcpStream};
use std::io::{Read, Write};
use crate::protocol::{self, Command};
use crate::db::SharedDb;
use std::sync::{Arc};
use crate::db::sstable;

pub fn start(address: &str, db: SharedDb){

    let listener  = TcpListener::bind(address).expect("Failed to bind");

    println!("f1reDB is listing on {}", address);

    for stream in listener.incoming(){
        match stream {
            Ok(stream) => {
                let db = Arc::clone(&db);
                std::thread::spawn(move || {
                    handle_client(stream, db);
                });
            }
            Err(e) => {
                eprintln!("Connection failed: {}", e);
            }
        }
    }
}

pub fn handle_client(mut stream: TcpStream, db: SharedDb){

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
                
                Ok(Command::Set { key, value }) => {

                    let mut db = db.lock().unwrap();

                    db.wal.log_set(&key, &value).unwrap();
                    db.memtable.set(key, value);

                    const MEMTABLE_LIMIT: usize = 4;

                    if db.memtable.len() >= MEMTABLE_LIMIT {
                        let path = sstable::flush(&db.memtable).unwrap();
                        db.sstable.push(path);
                        db.memtable.clear();
                    }

                    stream.write_all(b"OK\n").unwrap();
                }

                Ok(Command::Get { key }) => {

                    let db = db.lock().unwrap();

                    let mut found = false;

                    if let Some(value) = db.memtable.get(&key) {
                        let resp = format!("VALUE {}\n", value);
                        stream.write_all(resp.as_bytes()).unwrap();
                        continue;
                    }

                    for path in db.sstable.iter().rev() {

                        if let Some(value) = sstable::get(path, &key) {
                            let resp = format!("VALUE {}\n", value);
                            stream.write_all(resp.as_bytes()).unwrap();
                            found = true;
                            break;                            
                        }
                    }
                
                    if !found {
                        stream.write_all(b"NOT_FOUND\n").unwrap();
                    }
                }

                Err(e) => {
                    let msg = format!("ERROR {}\n", e);
                    stream.write_all(msg.as_bytes()).unwrap();
                }
            }
        }
    }
}


