use std::collections::BTreeMap;
use std::fs::{File};
use std::io::{BufReader, BufRead, BufWriter, Write};

use crate::db::sstable::{SSTableMeta, path_for_id};
use crate::db::bloom::BloomFilter;
use crate::db::memtable::Value;

pub fn compact(sstables: Vec<SSTableMeta>, new_id: u64) -> std::io::Result<SSTableMeta>{

    let mut merged: BTreeMap<String, Value>  = BTreeMap::new();

    for meta in sstables.iter().rev(){
        
        let file = File::open(&meta.path)?;
        let reader = BufReader::new(file);

        for line in reader.lines() {

            let line = line?;

            let mut parts = line.split_whitespace();

            let key = parts.next().unwrap().to_string();
            let raw_value = parts.next().unwrap().to_string();

            let value = if raw_value == "__TOMBSTONE__" {
                Value::Tombstone
            } else {
                Value::Data(raw_value.to_string())
            };
            
            merged.entry(key).or_insert(value);

        }
    }

    let path = path_for_id(new_id);
    let file = File::create(&path)?;

    let mut writer = BufWriter::new(file);

    let mut bloom = BloomFilter::new(1024, 3);

    for (key, value) in &merged {
        match value {
            Value::Data(v) => {
                writeln!(writer, "{} {}", key, v)?;
                bloom.insert(key);
            }
            Value::Tombstone => {
                // Skip tombstones — permanent deletion
            }
        }
    }

    writer.flush()?;

    Ok(SSTableMeta { path, bloom })

}