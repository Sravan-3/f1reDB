use std::collections::hash_map::DefaultHasher;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::io::{Seek, SeekFrom, Read};

#[derive(Clone)]
pub struct BloomFilter{
    bits:Vec<bool>,
    k: usize,
}

const FOOTER_SIZE: u64 = 16;

impl BloomFilter {

    pub fn new(size: usize, k:usize) -> Self{
        Self { 
            bits: vec![false; size],
            k 
        }
    }

    fn hash<T: Hash>(&self, item: &T, i: usize) -> usize{

        let mut hasher = DefaultHasher::new();
        item.hash(&mut hasher);
        i.hash(&mut hasher);
        (hasher.finish() as usize) % self.bits.len()
    }

    pub fn insert<T: Hash>(&mut self, item: &T){

        for i in 0..self.k {
            let idx = self.hash(item, i);
            self.bits[idx] = true;
        }
    }

    pub fn might_contain<T: Hash>(&self, item: &T) -> bool{
        
        for i in 0..self.k{

            let idx = self.hash(item, i);

            if !self.bits[idx] {
                return false;
            }
        }

        true
    }

    pub fn build_from_sstable(path: &Path) -> std::io::Result<Self> {

        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        let file_size = reader.get_ref().metadata()?.len();

        reader.seek(SeekFrom::Start(file_size - FOOTER_SIZE))?;

        let mut buf = [0u8; 8];

        reader.read_exact(&mut buf)?;
        let index_start = u64::from_le_bytes(buf);

        reader.read_exact(&mut buf)?;
        let _index_size = u64::from_le_bytes(buf);

        reader.seek(SeekFrom::Start(0))?;

        let mut bloom = BloomFilter::new(1024, 3);

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

            if let Some(key) = parts.next() {
                bloom.insert(&key);
            }
        }

        Ok(bloom)
    }
}
