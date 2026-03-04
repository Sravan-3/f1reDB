use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, BufRead, BufWriter, Write, Seek};

use crate::db::sstable::{SSTableMeta, path_for_id};
use crate::db::bloom::BloomFilter;
use crate::db::memtable::Value;

pub fn compact(sstables: Vec<SSTableMeta>, new_id: u64) -> std::io::Result<SSTableMeta> {

    let mut merged: BTreeMap<String, Value> = BTreeMap::new();

    // Newest SSTables first
    for meta in sstables.iter().rev() {

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
                Value::Data(raw_value)
            };

            merged.entry(key).or_insert(value);
        }
    }

    let path = path_for_id(new_id);
    let file = File::create(&path)?;

    let mut writer = BufWriter::new(file);
    let mut bloom = BloomFilter::new(1024, 3);
    let mut index = Vec::new();

    let mut count = 0;

    let mut min_key: Option<String> = None;
    let mut max_key: Option<String> = None;

    for (key, value) in &merged {

        match value {

            Value::Data(v) => {

                let offset = writer.stream_position()?;

                if count % 100 == 0 {
                    index.push((key.clone(), offset));
                }

                if min_key.is_none() {
                    min_key = Some(key.clone());
                }

                max_key = Some(key.clone());

                writeln!(writer, "{} {}", key, v)?;

                bloom.insert(key);

                count += 1;
            }

            Value::Tombstone => continue,
        }
    }

    writer.flush()?;

    Ok(SSTableMeta {
        path,
        bloom,
        index,
        min_key: min_key.unwrap(),
        max_key: max_key.unwrap(),
    })
}