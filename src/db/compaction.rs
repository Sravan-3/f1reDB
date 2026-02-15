use std::collections::BTreeMap;
use std::fs::{File};
use std::io::{BufReader, BufRead, BufWriter, Write};

use crate::db::sstable::{SSTableMeta, path_for_id};
use crate::db::bloom::BloomFilter;

pub fn compact(sstables: Vec<SSTableMeta>, new_id: u64) -> std::io::Result<SSTableMeta>{

    let mut merged = BTreeMap::new();

    for meta in sstables.iter().rev(){
        
        let file = File::open(&meta.path)?;
        let reader = BufReader::new(file);

        for line in reader.lines() {

            let line = line?;

            let mut parts = line.split_whitespace();

            let key = parts.next().unwrap().to_string();
            let value = parts.next().unwrap().to_string();
            
            merged.entry(key).or_insert(value);

        }
    }

    let path = path_for_id(new_id);
    let file = File::create(&path)?;

    let mut writer = BufWriter::new(file);

    let mut bloom = BloomFilter::new(1024, 3);

    for (key, value) in &merged {
        writeln!(writer, "{} {}",key, value)?;
        bloom.insert(key);
    }

    writer.flush()?;

    Ok(SSTableMeta { path, bloom })

}