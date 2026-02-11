use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;

use crate::db::bloom::BloomFilter;
use crate::db::memtable::MemTable;

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
        writeln!(writer, "{} {}", key, value)?;
        bloom.insert(key);
    }

    writer.flush()?;

    Ok(SSTableMeta { path, bloom })
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