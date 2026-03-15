use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::sync::Arc;
use std::path::PathBuf;

use crate::protocol::{self, Command};
use crate::db::SharedDb;
use crate::db::memtable::Value;
use crate::db::sstable;
use crate::db::compaction;
use crate::db::static_vars;

pub async fn start(address: &str, db: SharedDb) {

    let listener = TcpListener::bind(address)
        .await
        .expect("Failed to bind");

    println!("f1reDB is listening on {}", address);

    loop {

        match listener.accept().await {

            Ok((stream, _)) => {

                let db = Arc::clone(&db);

                tokio::spawn(async move {
                    handle_client(stream, db).await;
                });
            }

            Err(e) => {
                eprintln!("Connection failed: {}", e);
            }
        }
    }
}

pub async fn handle_client(mut stream: TcpStream, db_arc: SharedDb) {

    let mut buffer = [0; 1024];
    let mut pending = Vec::new();

    stream
        .write_all(static_vars::BANNER.as_bytes())
        .await
        .expect("Printing banner failed");

    loop {

        let n = match stream.read(&mut buffer).await {

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

                    {
                        let mut db = db_arc.write().unwrap();

                        db.wal.log_set(&key, &value).unwrap();
                        db.memtable.set(key, value);
                    }

                    stream.write_all(b"OK\n").await.unwrap();

                    let mut spawn_compaction = None;

                    {
                        let mut db = db_arc.write().unwrap();

                        const MEMTABLE_LIMIT: usize = 4;

                        if db.memtable.len() >= MEMTABLE_LIMIT {

                            let id = db.manifest.allocate_sstable_id();
                            let meta = sstable::flush(&db.memtable, id).unwrap();

                            db.manifest.add_sstable(meta.path.clone());
                            db.manifest.persist("MANIFEST").unwrap();

                            db.level0.push(meta);
                            db.memtable.clear();
                        }

                        if db.level0.len() >= 4 && !db.compaction_running {

                            db.compaction_running = true;

                            let old_tables = db.level0.clone();

                            let old_paths: Vec<PathBuf> =
                                old_tables.iter().map(|m| m.path.clone()).collect();

                            let id = db.manifest.allocate_sstable_id();

                            spawn_compaction = Some((old_tables, old_paths, id));
                        }
                    }

                    if let Some((old_tables, old_paths, id)) = spawn_compaction {

                        let db_clone = Arc::clone(&db_arc);

                        std::thread::spawn(move || {

                            let new_table =
                                compaction::compact(old_tables, id).unwrap();

                            let mut db = db_clone.write().unwrap();

                            db.level0.clear();

                            for path in &old_paths {
                                db.manifest.remove_sstable(path);
                            }

                            db.manifest.add_sstable(new_table.path.clone());
                            db.manifest.persist("MANIFEST").unwrap();

                            for path in &old_paths {
                                let _ = std::fs::remove_file(path);
                            }

                            db.level1.push(new_table);

                            db.compaction_running = false;
                        });
                    }
                }

                Ok(Command::Get { key }) => {

                    let response = {

                        let mut db = db_arc.write().unwrap();

                        if let Some(value) = db.memtable.get(&key) {

                            match value {

                                Value::Data(v) =>
                                    format!("VALUE {}\n", v),

                                Value::Tombstone =>
                                    "NOT_FOUND\n".to_string()
                            }

                        } else {

                            let mut result = None;

                            let level0_tables = db.level0.clone();

                            for meta in level0_tables.iter().rev() {

                                if !meta.bloom.might_contain(&key) {
                                    continue;
                                }

                                if let Some(value) =
                                    sstable::get(meta, &key, &mut db.block_cache)
                                {
                                    result = Some(value);
                                    break;
                                }
                            }

                            if result.is_none() {

                                let level1_tables = db.level1.clone();

                                for meta in &level1_tables {

                                    if key < meta.min_key
                                        || key > meta.max_key
                                    {
                                        continue;
                                    }

                                    if !meta.bloom.might_contain(&key) {
                                        continue;
                                    }

                                    if let Some(value) =
                                        sstable::get(meta, &key, &mut db.block_cache)
                                    {
                                        result = Some(value);
                                        break;
                                    }
                                }
                            }

                            match result {

                                Some(Value::Data(v)) =>
                                    format!("VALUE {}\n", v),

                                Some(Value::Tombstone) =>
                                    "NOT_FOUND\n".to_string(),

                                None =>
                                    "NOT_FOUND\n".to_string()
                            }
                        }
                    };

                    stream.write_all(response.as_bytes()).await.unwrap();
                }

                Ok(Command::Delete { key }) => {

                    {
                        let mut db = db_arc.write().unwrap();

                        db.wal.log_delete(&key).unwrap();
                        db.memtable.delete(key);
                    }

                    stream.write_all(b"OK\n").await.unwrap();
                }

                Err(e) => {

                    let msg = format!("ERROR {}\n", e);
                    stream.write_all(msg.as_bytes()).await.unwrap();
                }
            }
        }
    }
}