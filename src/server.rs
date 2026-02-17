// TCP server
use std::net::{TcpListener, TcpStream};
use std::io::{Read, Write};
use std::path::PathBuf;

use crate::db::memtable::Value;
use crate::protocol::{self, Command};
use crate::db::SharedDb;
use std::sync::{Arc};
use crate::db::sstable;
use crate::db::compaction;
use crate::db::static_vars;

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

pub fn handle_client(mut stream: TcpStream, db_arc: SharedDb){

    let mut buffer =[0; 1024];
    let mut pending= Vec::new();

    stream.write_all(static_vars::BANNER.as_bytes()).expect("Priniting banner failed");

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

                    let mut db = db_arc.write().unwrap();

                    db.wal.log_set(&key, &value).unwrap();
                    db.memtable.set(key, value);

                    drop(db);

                    stream.write_all(b"OK\n").unwrap();

                    let mut db = db_arc.write().unwrap();
                    
                    const MEMTABLE_LIMIT: usize = 4;

                    if db.memtable.len() >= MEMTABLE_LIMIT {
                        
                        let id = db.manifest.allocate_sstable_id();
                        let meta = sstable::flush(&db.memtable, id).unwrap();

                        db.manifest.add_sstable(meta.path.clone());
                        db.manifest.persist("MANIFEST").unwrap();

                        db.sstables.push(meta);
                        db.memtable.clear();
                    }

                    if db.sstables.len() >= 2 && !db.compaction_running {

                        db.compaction_running = true;

                        let old_tables  = db.sstables.clone();
                        let old_path: Vec<PathBuf> = old_tables.iter().map(|m| m.path.clone()).collect();

                        let id = db.manifest.allocate_sstable_id();

                        let db_clone = Arc::clone(&db_arc);

                        drop(db);

                        std::thread::spawn(move ||{

                            let new_table = compaction::compact(
                            old_tables,
                            id,
                            )
                            .unwrap();

                            let mut db = db_clone.write().unwrap();

                            db.sstables.clear();
                            db.sstables.push(new_table.clone());

                            for path in &old_path{
                                db.manifest.remove_sstable(path);
                            }
                            
                            db.manifest.add_sstable(new_table.path.clone());
                            db.manifest.persist("MANIFEST").unwrap();

                            for path in &old_path{
                                let _ = std::fs::remove_file(path);
                            }

                            db.compaction_running = false;
                            
                        });

                        continue;
                    }
                }

                Ok(Command::Get { key }) => {

                    let db = db_arc.read().unwrap();


                    if let Some(value) = db.memtable.get(&key) {
                        match value {
                            Value::Data(v) => {
                                let resp = format!("VALUE {}\n", v);
                                stream.write_all(resp.as_bytes()).unwrap();
                            }
                            Value::Tombstone => {
                                stream.write_all(b"NOT_FOUND\n").unwrap();
                            }
                        }

                        continue;
                    }

                    for meta in db.sstables.iter().rev() {

                        if !meta.bloom.might_contain(&key) {
                            continue;
                        }
                        
                        if let Some(value) = sstable::get(&meta.path, &key) {
                            match value {
                                Value::Data(v) => {
                                    let resp = format!("VALUE {}\n", v);
                                    stream.write_all(resp.as_bytes()).unwrap();
                                }
                                Value::Tombstone => {
                                    stream.write_all(b"NOT_FOUND\n").unwrap();
                                }
                            }
                            break;                            
                        }
                    }
                }

                Ok(Command::Delete { key }) => {

                       let mut db = db_arc.write().unwrap();

                        db.wal.log_delete(&key).unwrap();
                        db.memtable.delete(key);

                        const MEMTABLE_LIMIT: usize = 4;

                        if db.memtable.len() >= MEMTABLE_LIMIT {

                            let id = db.manifest.allocate_sstable_id();
                            let meta = sstable::flush(&db.memtable, id).unwrap();

                            db.manifest.add_sstable(meta.path.clone());
                            db.manifest.persist("MANIFEST").unwrap();

                            db.sstables.push(meta);
                            db.memtable.clear();
                        }

                        if db.sstables.len() >= 2 && !db.compaction_running {

                            db.compaction_running = true;

                        let old_tables  = db.sstables.clone();
                        let old_path: Vec<PathBuf> = old_tables.iter().map(|m| m.path.clone()).collect();

                        let id = db.manifest.allocate_sstable_id();

                        let db_clone = Arc::clone(&db_arc);

                        drop(db);

                        std::thread::spawn(move ||{

                            let new_table = compaction::compact(
                            old_tables,
                            id,
                            )
                            .unwrap();

                            let mut db = db_clone.write().unwrap();

                            db.sstables.clear();
                            db.sstables.push(new_table.clone());

                            for path in &old_path{
                                db.manifest.remove_sstable(path);
                            }
                            
                            db.manifest.add_sstable(new_table.path.clone());
                            db.manifest.persist("MANIFEST").unwrap();

                            for path in &old_path{
                                let _ = std::fs::remove_file(path);
                            }

                            db.compaction_running = false;
                            
                        });

                        stream.write_all(b"OK\n").unwrap();
                        continue;

                        }

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


