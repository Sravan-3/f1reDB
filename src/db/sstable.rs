use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::db::memtable::MemTable;

fn next_sstable_path() -> PathBuf{

    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();

    PathBuf::from(format!("sstable-{}.db", ts))
}

pub fn flush(memtable: &MemTable) -> std::io::Result<PathBuf>{

    let path = next_sstable_path();
    let file = File::create(&path)?;
    let mut writer = BufWriter::new(file);

    for (key, value) in memtable.iter() {
        writeln!(writer, "{} {}", key, value)?;
    }

    writer.flush()?;

    Ok(path)
}

pub fn get(path: &PathBuf, key: &str) -> Option<String>{

    let file = File::open(path).ok()?;
    let reader = BufReader::new(file);

    for line in reader.lines(){

        if let Ok(line) = line {

            let mut parts = line.split_whitespace();

            let k = parts.next()?;
            let v = parts.next()?;

            if k == key {
                return Some(v.to_string());
            }
        }
    }
    
    None
}