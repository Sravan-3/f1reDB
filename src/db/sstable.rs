use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::io::{Seek, SeekFrom};

use crate::db::bloom::BloomFilter;
use crate::db::memtable::{MemTable, Value};
use crate::db::block_cache::BlockCache;

#[derive(Clone)]
pub struct SSTableMeta{
    pub path: PathBuf,
    pub bloom: BloomFilter,
    pub index: Vec<(String, u64)>,
    pub min_key: String,
    pub max_key: String,
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

    let mut  min_key: Option<String> = None;
    let mut max_key: Option<String> = None;

    for (key, value) in memtable.iter() {

        let offset = writer.stream_position()?;

        if count % 100 == 0 {
            index.push((key.clone(), offset));
        }

        if min_key.is_none(){
            min_key = Some(key.clone());
        }

        max_key = Some(key.clone());

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

    Ok(SSTableMeta { path, bloom, index, min_key: min_key.unwrap(), max_key: max_key.unwrap()})
}

pub fn get(meta: &SSTableMeta, key: &str, cache: &mut BlockCache) -> Option<Value>{

    let file = File::open(&meta.path).ok()?;
    let mut reader = BufReader::new(file);

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

    reader.seek(SeekFrom::Start(offset)).ok()?;

    let mut block = Vec::new();
    let mut line = String::new();

    while reader.read_line(&mut line).ok()? > 0 {

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