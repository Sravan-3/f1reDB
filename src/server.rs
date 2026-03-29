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
use crate::db::wal::Wal;

const L0_SOFT_LIMIT: usize = 12;
const L0_HARD_LIMIT: usize = 20;
const MEMTABLE_LIMIT: usize = 1000;
const L1_LIMIT: usize = 8;

fn maybe_stall(db: &crate::db::Db) {
    if db.level0.len() >= L0_HARD_LIMIT {
        loop {
            if db.level0.len() < L0_SOFT_LIMIT {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    } else if db.level0.len() >= L0_SOFT_LIMIT {
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
}

pub async fn start(address: &str, db: SharedDb) {
    let listener = TcpListener::bind(address).await.expect("Failed to bind");
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

    stream.write_all(static_vars::BANNER.as_bytes()).await.unwrap();

    loop {

        let n = match stream.read(&mut buffer).await {
            Ok(0) => return,
            Ok(n) => n,
            Err(_) => return,
        };

        pending.extend_from_slice(&buffer[..n]);

        while let Some(pos) = pending.iter().position(|&b| b == b'\n') {

            let line = pending.drain(..=pos).collect::<Vec<u8>>();
            let line = String::from_utf8_lossy(&line);

            match protocol::parse_line(&line) {

                Ok(Command::Set { key, value }) => {

                    {
                        let mut db = db_arc.write().unwrap();

                        maybe_stall(&db);

                        Wal::log_set(&db.wal, &key, &value);
                        db.memtable.set(key, value);

                        {
                            let mut m = db.metrics.lock().unwrap();
                            m.set_count += 1;
                        }
                    }

                    stream.write_all(b"OK\n").await.unwrap();

                    let mut l0_task = None;
                    let mut l1_task = None;

                    {
                        let mut db = db_arc.write().unwrap();

                        if db.memtable.len() >= MEMTABLE_LIMIT {

                            let id = db.manifest.allocate_sstable_id();
                            let meta = sstable::flush(&db.memtable, id).unwrap();

                            db.manifest.add_sstable(0, meta.path.clone());
                            db.manifest.persist("MANIFEST").unwrap();

                            db.level0.push(meta);
                            db.memtable.clear();

                            Wal::reset(&db.wal);
                        }

                        if db.level0.len() >= 4 && !db.compaction_running {

                            db.compaction_running = true;

                            let all = db.level0.clone();
                            let paths: Vec<PathBuf> =
                                all.iter().map(|m| m.path.clone()).collect();

                            let id = db.manifest.allocate_sstable_id();

                            l0_task = Some((all, paths, id));
                        }

                        else if db.level1.len() >= L1_LIMIT && !db.compaction_running {

                            db.compaction_running = true;

                            let all = db.level1.clone();
                            let paths: Vec<PathBuf> =
                                all.iter().map(|m| m.path.clone()).collect();

                            let id = db.manifest.allocate_sstable_id();

                            l1_task = Some((all, paths, id));
                        }
                    }

                    if let Some((all, paths, id)) = l0_task {

                        let db_clone = Arc::clone(&db_arc);

                        std::thread::spawn(move || {

                            let new_table = compaction::compact(all, id).unwrap();

                            let mut db = db_clone.write().unwrap();

                            for p in &paths {
                                db.manifest.remove_sstable(0, p);
                            }

                            db.manifest.add_sstable(1, new_table.path.clone());
                            db.manifest.persist("MANIFEST").unwrap();

                            for p in &paths {
                                let _ = std::fs::remove_file(p);
                            }

                            db.level0.clear();
                            db.level1.push(new_table);

                            {
                                let mut m = db.metrics.lock().unwrap();
                                m.compactions += 1;
                            }

                            db.compaction_running = false;
                        });
                    }

                    if let Some((all, paths, id)) = l1_task {

                        let db_clone = Arc::clone(&db_arc);

                        std::thread::spawn(move || {

                            let new_table = compaction::compact(all, id).unwrap();

                            let mut db = db_clone.write().unwrap();

                            for p in &paths {
                                db.manifest.remove_sstable(1, p);
                            }

                            db.manifest.add_sstable(2, new_table.path.clone());
                            db.manifest.persist("MANIFEST").unwrap();

                            for p in &paths {
                                let _ = std::fs::remove_file(p);
                            }

                            db.level1.clear();
                            db.level2.push(new_table);

                            {
                                let mut m = db.metrics.lock().unwrap();
                                m.compactions += 1;
                            }

                            db.compaction_running = false;
                        });
                    }
                }

                Ok(Command::Get { key }) => {

                    let response = {

                        let mut db = db_arc.write().unwrap();

                        {
                            let mut m = db.metrics.lock().unwrap();
                            m.get_count += 1;
                        }

                        if let Some(value) = db.memtable.get(&key) {

                            match value {
                                Value::Data(v) => format!("VALUE {}\n", v),
                                Value::Tombstone => "NOT_FOUND\n".to_string(),
                            }

                        } else {

                            let mut result = None;
                            let mut hit_any = false;

                            let l0 = db.level0.clone();

                            for meta in l0.iter().rev() {
                                if !meta.bloom.might_contain(&key) { continue; }

                                let (val, hit) = sstable::get(meta, &key, &mut db.block_cache);

                                if hit{
                                    hit_any = true;
                                }

                                if let Some(v) = val {
                                    result = Some(v);
                                    break;
                                }
                            }

                            if result.is_none() {

                                let l1 = db.level1.clone();

                                for meta in &l1 {
                                    if key < meta.min_key || key > meta.max_key { continue; }
                                    if !meta.bloom.might_contain(&key) { continue; }

                                    let (val, hit) = sstable::get(meta, &key, &mut db.block_cache);

                                    if hit{
                                        hit_any = true;
                                    }

                                    if let Some(v) = val {
                                        result = Some(v);
                                        break;
                                    }
                                }
                            }

                            if result.is_none() {

                                let l2 = db.level2.clone();

                                for meta in &l2 {
                                    if key < meta.min_key || key > meta.max_key { continue; }
                                    if !meta.bloom.might_contain(&key) { continue; }

                                    let (val, hit) = sstable::get(meta, &key, &mut db.block_cache);

                                     if hit{
                                        hit_any = true;
                                    }   

                                    if let Some(v) = val {
                                        result = Some(v);
                                        break;
                                    }
                                }
                            }

                            {
                                let mut m = db.metrics.lock().unwrap();
                                if hit_any {
                                    m.cache_hits += 1;
                                } else {
                                    m.cache_misses += 1;
                                }
                            }
                            
                            match result {
                                Some(Value::Data(v)) => format!("VALUE {}\n", v),
                                Some(Value::Tombstone) => "NOT_FOUND\n".to_string(),
                                None => "NOT_FOUND\n".to_string(),
                            }
                        }
                    };

                    stream.write_all(response.as_bytes()).await.unwrap();
                }

                Ok(Command::Delete { key }) => {

                    {
                        let mut db = db_arc.write().unwrap();

                        Wal::log_delete(&db.wal, &key);
                        db.memtable.delete(key);

                        {
                            let mut m = db.metrics.lock().unwrap();
                            m.set_count += 1;
                        }
                    }

                    stream.write_all(b"OK\n").await.unwrap();
                }

                Ok(Command::Stats) => {

                    let resp = {

                        let db = db_arc.read().unwrap();
                        let m = db.metrics.lock().unwrap();

                        format!(
                            "SETS {}\nGETS {}\nCACHE_HIT {}\nCACHE_MISS {}\nCOMPACTIONS {}\n",
                            m.set_count,
                            m.get_count,
                            m.cache_hits,
                            m.cache_misses,
                            m.compactions
                        )
                    };

                    stream.write_all(resp.as_bytes()).await.unwrap();
                }

                Err(e) => {
                    let msg = format!("ERROR {}\n", e);
                    stream.write_all(msg.as_bytes()).await.unwrap();
                }
            }
        }
    }
}