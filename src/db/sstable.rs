use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write, Read};
use std::path::PathBuf;
use std::io::{Seek, SeekFrom};

use crate::db::bloom::BloomFilter;
use crate::db::memtable::{MemTable, Value};
use crate::db::block_cache::BlockCache;

const BLOCK_SIZE: usize = 4096;
const FOOTER_SIZE: u64 = 16;

#[derive(Clone)]
pub struct SSTableMeta {
    pub path: PathBuf,
    pub bloom: BloomFilter,
    pub index: Vec<(String, u64)>,
    pub min_key: String,
    pub max_key: String,
}

pub fn path_for_id(id: u64) -> PathBuf {
    PathBuf::from(format!("sstable-{}.db", id))
}

pub fn flush(memtable: &MemTable, sstable_id: u64) -> std::io::Result<SSTableMeta> {

    let path = path_for_id(sstable_id);
    let file = File::create(&path)?;
    let mut writer = BufWriter::new(file);

    let mut bloom = BloomFilter::new(1024, 3);
    let mut index = Vec::new();

    let mut min_key: Option<String> = None;
    let mut max_key: Option<String> = None;

    let mut block_buffer: Vec<String> = Vec::new();
    let mut block_start_key: Option<String> = None;
    let mut current_size: usize = 0;

    for (key, value) in memtable.iter() {

        if block_start_key.is_none() {
            block_start_key = Some(key.clone());
        }

        let line = match value {
            Value::Data(v) => format!("{} {}\n", key, v),
            Value::Tombstone => format!("{} __TOMBSTONE__\n", key),
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

        bloom.insert(key);

        if min_key.is_none() {
            min_key = Some(key.clone());
        }

        max_key = Some(key.clone());
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

pub fn get(meta: &SSTableMeta, key: &str, cache: &mut BlockCache) -> Option<Value> {

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

    let cache_key = (meta.path.clone(), offset);

    if let Some(block) = cache.get(&cache_key) {

        for (k, v) in block {

            if k == key {
                return Some(v);
            }

            if k.as_str() > key {
                break;
            }
        }

        return None;
    }

    let file = File::open(&meta.path).ok()?;
    let mut reader = BufReader::new(file);

    reader.seek(SeekFrom::Start(offset)).ok()?;

    let mut block = Vec::new();
    let mut line = String::new();
    let mut bytes_read = 0;

    while bytes_read < BLOCK_SIZE {

        let read = reader.read_line(&mut line).ok()?;
        if read == 0 {
            break;
        }

        bytes_read += read;

        let mut parts = line.split_whitespace();

        let k = parts.next()?.to_string();
        let v = parts.next()?;

        let value = if v == "__TOMBSTONE__" {
            Value::Tombstone
        } else {
            Value::Data(v.to_string())
        };

        block.push((k.clone(), value.clone()));

        if k == key {
            cache.insert(cache_key, block);
            return Some(value);
        }

        if k.as_str() > key {
            cache.insert(cache_key, block);
            return None;
        }

        line.clear();
    }

    cache.insert(cache_key, block);
    None
}

pub fn build_sparse_index(path: &PathBuf) -> std::io::Result<Vec<(String, u64)>> {

    let file = File::open(path)?;
    let mut reader = BufReader::new(file);

    let file_size = reader.get_ref().metadata()?.len();

    reader.seek(SeekFrom::Start(file_size - FOOTER_SIZE))?;

    let mut buf = [0u8; 8];

    reader.read_exact(&mut buf)?;
    let index_start = u64::from_le_bytes(buf);

    reader.read_exact(&mut buf)?;
    let index_size = u64::from_le_bytes(buf);

    reader.seek(SeekFrom::Start(index_start))?;

    let mut index = Vec::new();
    let mut read_bytes = 0;

    while read_bytes < index_size {

        let mut line = String::new();
        let bytes = reader.read_line(&mut line)?;

        if bytes == 0 {
            break;
        }

        read_bytes += bytes as u64;

        let mut parts = line.split_whitespace();

        let key = parts.next().unwrap().to_string();
        let offset = parts.next().unwrap().parse::<u64>().unwrap();

        index.push((key, offset));
    }

    Ok(index)
}