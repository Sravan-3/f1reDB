use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::io::{Seek, SeekFrom};

use crate::db::bloom::BloomFilter;
use crate::db::memtable::{MemTable, Value};

#[derive(Clone)]
pub struct SSTableMeta{
    pub path: PathBuf,
    pub bloom: BloomFilter,
    pub index: Vec<(String, u64)>, // sparse index
}

pub fn path_for_id(id: u64) -> PathBuf {
    PathBuf::from(format!("sstable-{}.db", id))
}

pub fn flush(memtable: &MemTable, sstable_id: u64) -> std::io::Result<SSTableMeta>{

    use std::io::Seek;

    let path = path_for_id(sstable_id);
    let file = File::create(&path)?;
    let mut writer = BufWriter::new(file);

    let mut bloom = BloomFilter::new(1024, 3);
    let mut index = Vec::new();
    let mut count = 0;

    for (key, value) in memtable.iter() {

        let offset = writer.stream_position()?;

        if count % 100 == 0 {
            index.push((key.clone(), offset));
        }

        match value {
            Value::Data(v) => {
                writeln!(writer, "{} {}", key, v)?;
            },
            Value::Tombstone => {
                writeln!(writer, "{} __TOMBSTONE__", key)?;
            },
        }

        bloom.insert(key);
        count += 1;
    }

    writer.flush()?;

    Ok(SSTableMeta { path, bloom, index })
}

pub fn get(meta: &SSTableMeta, key: &str) -> Option<Value>{

    let file = File::open(&meta.path).ok()?;
    let mut reader = BufReader::new(file);

    // binary search in sparse index
    let pos = meta.index.binary_search_by(|(k, _)| k.as_str().cmp(key));

    let offset = match pos {
        Ok(i) => meta.index[i].1,
        Err(i) => {
            if i == 0 {
                0
            } else {
                meta.index[i - 1].1
            }
        }
    };

    reader.seek(SeekFrom::Start(offset)).ok()?;

    let mut line = String::new();

    while reader.read_line(&mut line).ok()? > 0 {

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

        if k > key {
            break; // sorted property → stop early
        }

        line.clear();
    }

    None
}

pub fn build_sparse_index(path: &PathBuf) -> std::io::Result<Vec<(String, u64)>> {

    let file = File::open(path)?;
    let mut reader = BufReader::new(file);

    let mut index = Vec::new();
    let mut count = 0;

    loop {
        let offset = reader.stream_position()?;

        let mut line = String::new();
        let bytes = reader.read_line(&mut line)?;

        if bytes == 0 {
            break;
        }

        if count % 100 == 0 {
            if let Some(key) = line.split_whitespace().next() {
                index.push((key.to_string(), offset));
            }
        }

        count += 1;
    }

    Ok(index)
}