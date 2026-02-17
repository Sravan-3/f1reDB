use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;

use crate::db::bloom::BloomFilter;
use crate::db::memtable::{MemTable, Value};

#[derive(Clone)]
pub struct SSTableMeta{
    pub path: PathBuf,
    pub bloom: BloomFilter,
}

pub fn path_for_id(id: u64) -> PathBuf {
    PathBuf::from(format!("sstable-{}.db", id))
}

pub fn flush(memtable: &MemTable, sstable_id: u64) -> std::io::Result<SSTableMeta>{

    let path = path_for_id(sstable_id);
    let file = File::create(&path)?;
    let mut writer = BufWriter::new(file);

    let mut bloom = BloomFilter::new(1024, 3);

    for (key, value) in memtable.iter() {

        match value {
            Value::Data(v) => {
                writeln!(writer, "{} {}", key, v)?;
            },
            Value::Tombstone => {
                writeln!(writer, "{} __TOMBSTONE__", key)?;
            },
        }

        bloom.insert(key);
    }

    writer.flush()?;

    Ok(SSTableMeta { path, bloom })
}

pub fn get(path: &PathBuf, key: &str) -> Option<Value>{

    let file = File::open(path).ok()?;
    let reader = BufReader::new(file);

    for line in reader.lines(){

        if let Ok(line) = line {

            let mut parts = line.split_whitespace();

            let k = parts.next()?;
            let v = parts.next()?;

            if k == key {
                if v == "__TOMBSTONE__" {
                    return Some(Value::Tombstone);
                } else {
                    return Some(Value::Data(v.to_string()));
                }
            }
        }
    }
    None
}