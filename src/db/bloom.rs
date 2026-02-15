use std::collections::hash_map::DefaultHasher;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader};
use std::path::Path;

#[derive(Clone)]
pub struct BloomFilter{
    bits:Vec<bool>,
    k: usize,
}

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
        let reader = BufReader::new(file);
        
        let mut bloom = BloomFilter::new(1024, 3);

        for line in reader.lines() {
            let line = line?;
            let mut parts = line.split_whitespace();

            if let Some(key) = parts.next() {
                bloom.insert(&key);
            }
        }

        Ok(bloom)
    }

}
