use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::sync::Arc;
use std::path::PathBuf;
use std::fs;

use crate::protocol::{self, Command};
use crate::db::SharedDb;
use crate::db::memtable::Value;
use crate::db::sstable;
use crate::db::compaction;
use crate::db::static_vars;

const MEMTABLE_LIMIT: usize = 1000;
const L0_LIMIT: usize = 8;
const L1_SIZE_LIMIT: u64 = 10 * 1024 * 1024;
const L2_SIZE_LIMIT: u64 = 100 * 1024 * 1024;

fn level_size(level: &Vec<crate::db::sstable::SSTableMeta>) -> u64 {
    level.iter()
        .map(|m| fs::metadata(&m.path).map(|m| m.len()).unwrap_or(0))
        .sum()
}

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

    if stream.write_all(static_vars::BANNER.as_bytes()).await.is_err() {
        return;
    }

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

                    if stream.write_all(b"OK\n").await.is_err() {
                        return;
                    }

                    let mut l0_task = None;
                    let mut l1_task = None;
                    let mut l2_task = None;

                    {
                        let mut db = db_arc.write().unwrap();

                        if db.memtable.len() >= MEMTABLE_LIMIT {

                            let id = db.manifest.allocate_sstable_id();
                            let meta = sstable::flush(&db.memtable, id).unwrap();

                            db.manifest.add_sstable(meta.path.clone());
                            db.manifest.persist("MANIFEST").unwrap();

                            db.level0.push(meta);
                            db.memtable.clear();
                        }

                        if db.level0.len() >= L0_LIMIT && !db.compaction_running {

                            db.compaction_running = true;

                            let source = db.level0.iter().rev().take(3).cloned().collect::<Vec<_>>();
                            let target = db.level1.clone();

                            let (src, tgt) = compaction::pick_overlapping(source, target);

                            let mut all = src.clone();
                            all.extend(tgt.clone());

                            let paths: Vec<PathBuf> =
                                all.iter().map(|m| m.path.clone()).collect();

                            let id = db.manifest.allocate_sstable_id();

                            l0_task = Some((all, tgt, paths, id));
                        }

                        else if level_size(&db.level1) >= L1_SIZE_LIMIT && !db.compaction_running {

                            db.compaction_running = true;

                            let source = db.level1.clone();
                            let target = db.level2.clone();

                            let (src, tgt) = compaction::pick_overlapping(source, target);

                            let mut all = src.clone();
                            all.extend(tgt.clone());

                            let paths: Vec<PathBuf> =
                                all.iter().map(|m| m.path.clone()).collect();

                            let id = db.manifest.allocate_sstable_id();

                            l1_task = Some((all, tgt, paths, id));
                        }

                        else if level_size(&db.level2) >= L2_SIZE_LIMIT && !db.compaction_running {

                            db.compaction_running = true;

                            let all = db.level2.clone();
                            let paths: Vec<PathBuf> =
                                all.iter().map(|m| m.path.clone()).collect();

                            let id = db.manifest.allocate_sstable_id();

                            l2_task = Some((all, paths, id));
                        }
                    }

                    if let Some((all, tgt, paths, id)) = l0_task {

                        let db_clone = Arc::clone(&db_arc);

                        std::thread::spawn(move || {

                            let new_table = compaction::compact(all, id).unwrap();

                            let mut db = db_clone.write().unwrap();

                            db.level0.clear();
                            db.level1.retain(|m| !tgt.iter().any(|x| x.path == m.path));

                            for p in &paths {
                                db.manifest.remove_sstable(p);
                            }

                            db.manifest.add_sstable(new_table.path.clone());
                            db.manifest.persist("MANIFEST").unwrap();

                            for p in &paths {
                                let _ = fs::remove_file(p);
                            }

                            db.level1.push(new_table);

                            db.compaction_running = false;
                        });
                    }

                    if let Some((all, tgt, paths, id)) = l1_task {

                        let db_clone = Arc::clone(&db_arc);

                        std::thread::spawn(move || {

                            let new_table = compaction::compact(all, id).unwrap();

                            let mut db = db_clone.write().unwrap();

                            db.level1.retain(|m| !tgt.iter().any(|x| x.path == m.path));

                            for p in &paths {
                                db.manifest.remove_sstable(p);
                            }

                            db.manifest.add_sstable(new_table.path.clone());
                            db.manifest.persist("MANIFEST").unwrap();

                            for p in &paths {
                                let _ = fs::remove_file(p);
                            }

                            db.level2.push(new_table);

                            db.compaction_running = false;
                        });
                    }

                    if let Some((all, paths, id)) = l2_task {

                        let db_clone = Arc::clone(&db_arc);

                        std::thread::spawn(move || {

                            let new_table = compaction::compact(all, id).unwrap();

                            let mut db = db_clone.write().unwrap();

                            db.level2.clear();

                            for p in &paths {
                                db.manifest.remove_sstable(p);
                            }

                            db.manifest.add_sstable(new_table.path.clone());
                            db.manifest.persist("MANIFEST").unwrap();

                            for p in &paths {
                                let _ = fs::remove_file(p);
                            }

                            db.level2.push(new_table);

                            db.compaction_running = false;
                        });
                    }
                }

                Ok(Command::Get { key }) => {

                    let response = {

                        let mut db = db_arc.write().unwrap();

                        if let Some(value) = db.memtable.get(&key) {

                            match value {
                                Value::Data(v) => format!("VALUE {}\n", v),
                                Value::Tombstone => "NOT_FOUND\n".to_string()
                            }

                        } else {

                            let mut result = None;

                            let level0 = db.level0.clone();

                            for meta in level0.iter().rev() {
                                if !meta.bloom.might_contain(&key) { continue; }
                                if let Some(v) = sstable::get(meta, &key, &mut db.block_cache) {
                                    result = Some(v);
                                    break;
                                }
                            }

                            if result.is_none() {

                                let level1 = db.level1.clone();

                                for meta in &level1 {
                                    if key < meta.min_key || key > meta.max_key { continue; }
                                    if !meta.bloom.might_contain(&key) { continue; }
                                    if let Some(v) = sstable::get(meta, &key, &mut db.block_cache) {
                                        result = Some(v);
                                        break;
                                    }
                                }
                            }

                            if result.is_none() {

                                let level2 = db.level2.clone();

                                for meta in &level2 {
                                    if key < meta.min_key || key > meta.max_key { continue; }
                                    if !meta.bloom.might_contain(&key) { continue; }
                                    if let Some(v) = sstable::get(meta, &key, &mut db.block_cache) {
                                        result = Some(v);
                                        break;
                                    }
                                }
                            }

                            match result {
                                Some(Value::Data(v)) => format!("VALUE {}\n", v),
                                Some(Value::Tombstone) => "NOT_FOUND\n".to_string(),
                                None => "NOT_FOUND\n".to_string()
                            }
                        }
                    };

                    if stream.write_all(response.as_bytes()).await.is_err() {
                        return;
                    }
                }

                Ok(Command::Delete { key }) => {

                    {
                        let mut db = db_arc.write().unwrap();
                        db.wal.log_delete(&key).unwrap();
                        db.memtable.delete(key);
                    }

                    if stream.write_all(b"OK\n").await.is_err() {
                        return;
                    }
                }

                Err(e) => {
                    let msg = format!("ERROR {}\n", e);
                    if stream.write_all(msg.as_bytes()).await.is_err() {
                        return;
                    }
                }
            }
        }
    }
}