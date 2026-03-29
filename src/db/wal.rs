use std::fs::{OpenOptions, File};
use std::io::{Write, BufRead, BufReader};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::db::memtable::MemTable;

const BATCH_SIZE: usize = 64;
const FLUSH_INTERVAL_MS: u64 = 50;

#[derive(Debug)]
pub struct Wal {
    file: File,
    path: String,
    buffer: Vec<String>,
}

impl Wal {

    pub fn open(path: &str) -> std::io::Result<Arc<Mutex<Self>>> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;

        let wal = Arc::new(Mutex::new(Self {
            file,
            path: path.to_string(),
            buffer: Vec::new(),
        }));

        let wal_clone = Arc::clone(&wal);

        thread::spawn(move || {
            loop {
                thread::sleep(Duration::from_millis(FLUSH_INTERVAL_MS));
                if let Ok(mut wal) = wal_clone.lock() {
                    let _ = wal.flush();
                }
            }
        });

        Ok(wal)
    }

    pub fn log_set(wal: &Arc<Mutex<Self>>, key: &str, value: &str) {
        if let Ok(mut w) = wal.lock() {
            w.buffer.push(format!("SET {} {}\n", key, value));
            if w.buffer.len() >= BATCH_SIZE {
                let _ = w.flush();
            }
        }
    }

    pub fn log_delete(wal: &Arc<Mutex<Self>>, key: &str) {
        if let Ok(mut w) = wal.lock() {
            w.buffer.push(format!("DEL {}\n", key));
            if w.buffer.len() >= BATCH_SIZE {
                let _ = w.flush();
            }
        }
    }

    pub fn flush(&mut self) -> std::io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(())
        }

        for entry in &self.buffer {
            self.file.write_all(entry.as_bytes())?;
        }

        self.file.flush()?;
        self.file.sync_all()?;

        self.buffer.clear();

        Ok(())
    }

    pub fn reset(wal: &Arc<Mutex<Self>>) {
        if let Ok(mut w) = wal.lock() {
            let _ = w.flush();

            w.file = OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(&w.path)
                .unwrap();
        }
    }

    pub fn replay(path: &str, memtable: &mut MemTable) -> std::io::Result<()> {

        if !Path::new(path).exists() {
            return Ok(())
        }

        let file = File::open(path)?;
        let reader = BufReader::new(file);

        for line in reader.lines() {

            let line = line?;
            let parts: Vec<&str> = line.split_whitespace().collect();

            if parts.len() == 3 && parts[0] == "SET" {
                memtable.set(parts[1].to_string(), parts[2].to_string());
            }
            else if parts.len() == 2 && parts[0] == "DEL" {
                memtable.delete(parts[1].to_string());
            }
        }

        Ok(())
    }
}