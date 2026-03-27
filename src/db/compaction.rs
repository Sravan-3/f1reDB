use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, BufRead, BufWriter, Write, Seek, SeekFrom, Read};

use crate::db::sstable::{SSTableMeta, path_for_id};
use crate::db::bloom::BloomFilter;
use crate::db::memtable::Value;

const FOOTER_SIZE: u64 = 16;
const BLOCK_SIZE: usize = 4096;

fn overlaps(a: &SSTableMeta, b: &SSTableMeta) -> bool {
    !(a.max_key < b.min_key || a.min_key > b.max_key)
}

pub fn pick_overlapping(
    source: Vec<SSTableMeta>,
    target: Vec<SSTableMeta>
) -> (Vec<SSTableMeta>, Vec<SSTableMeta>) {

    let mut selected_target = Vec::new();

    for t in &target {
        for s in &source {
            if overlaps(s, t) {
                selected_target.push(t.clone());
                break;
            }
        }
    }

    (source, selected_target)
}

pub fn compact(
    sstables: Vec<SSTableMeta>,
    new_id: u64
) -> std::io::Result<SSTableMeta> {

    let mut merged: BTreeMap<String, Value> = BTreeMap::new();

    for meta in sstables.iter().rev() {

        let file = File::open(&meta.path)?;
        let mut reader = BufReader::new(file);

        let file_size = reader.get_ref().metadata()?.len();

        reader.seek(SeekFrom::Start(file_size - FOOTER_SIZE))?;

        let mut buf = [0u8; 8];

        reader.read_exact(&mut buf)?;
        let index_start = u64::from_le_bytes(buf);

        reader.read_exact(&mut buf)?;
        let _index_size = u64::from_le_bytes(buf);

        reader.seek(SeekFrom::Start(0))?;

        let mut bytes_read = 0;
        let mut line = String::new();

        while bytes_read < index_start {

            line.clear();

            let read = reader.read_line(&mut line)?;
            if read == 0 {
                break;
            }

            bytes_read += read as u64;

            let mut parts = line.split_whitespace();

            let key = match parts.next() {
                Some(k) => k.to_string(),
                None => continue,
            };

            let raw_value = match parts.next() {
                Some(v) => v.to_string(),
                None => continue,
            };

            let value = if raw_value == "__TOMBSTONE__" {
                Value::Tombstone
            } else {
                Value::Data(raw_value)
            };

            merged.entry(key).or_insert(value);
        }
    }

    let mut filtered = BTreeMap::new();

    for (k, v) in merged {
        if let Value::Data(_) = v {
            filtered.insert(k, v);
        }
    }

    if filtered.is_empty() {
        let path = path_for_id(new_id);

        let file = File::create(&path)?;
        let mut writer = BufWriter::new(file);

        let index_start = 0u64;
        let index_size = 0u64;

        writer.write_all(&index_start.to_le_bytes())?;
        writer.write_all(&index_size.to_le_bytes())?;
        writer.flush()?;

        return Ok(SSTableMeta {
            path,
            bloom: BloomFilter::new(1024, 3),
            index: Vec::new(),
            min_key: "".to_string(),
            max_key: "".to_string(),
        });
    }

    let path = path_for_id(new_id);
    let file = File::create(&path)?;
    let mut writer = BufWriter::new(file);

    let mut bloom = BloomFilter::new(1024, 3);
    let mut index = Vec::new();

    let mut block_buffer: Vec<String> = Vec::new();
    let mut block_start_key: Option<String> = None;
    let mut current_size = 0;

    let mut min_key: Option<String> = None;
    let mut max_key: Option<String> = None;

    for (key, value) in &filtered {

        if block_start_key.is_none() {
            block_start_key = Some(key.clone());
        }

        let line = match value {
            Value::Data(v) => format!("{} {}\n", key, v),
            _ => continue,
        };

        current_size += line.len();
        block_buffer.push(line);

        if current_size >= BLOCK_SIZE {

            let offset = writer.stream_position()?;
            index.push((block_start_key.clone().unwrap(), offset));

            for l in &block_buffer {
                writer.write_all(l.as_bytes())?;
            }

            block_buffer.clear();
            current_size = 0;
            block_start_key = None;
        }

        if min_key.is_none() {
            min_key = Some(key.clone());
        }

        max_key = Some(key.clone());
        bloom.insert(key);
    }

    if !block_buffer.is_empty() {

        let offset = writer.stream_position()?;
        index.push((block_start_key.unwrap(), offset));

        for l in &block_buffer {
            writer.write_all(l.as_bytes())?;
        }
    }

    let index_start = writer.stream_position()?;

    for (key, offset) in &index {
        writeln!(writer, "{} {}", key, offset)?;
    }

    let index_end = writer.stream_position()?;
    let index_size = index_end - index_start;

    writer.write_all(&index_start.to_le_bytes())?;
    writer.write_all(&index_size.to_le_bytes())?;

    writer.flush()?;

    Ok(SSTableMeta {
        path,
        bloom,
        index,
        min_key: min_key.unwrap(),
        max_key: max_key.unwrap(),
    })
}