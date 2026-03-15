use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;

use crate::db::memtable::Value;

type BlockKey = (PathBuf, u64);

pub struct BlockCache {
    map: HashMap<BlockKey, Vec<(String, Value)>>,
    order: VecDeque<BlockKey>,
    capacity: usize,
}

impl BlockCache {
    
    pub fn new(capacity: usize) -> Self {

        Self { 
            map: HashMap::new(), 
            order: VecDeque::new(), 
            capacity,
        }

    }

    pub fn get(&mut self, key: &(PathBuf, u64)) -> Option<Vec<(String, Value)>> {

        if let Some(block) = self.map.get(key){

            if let Some(pos) = self.order.iter().position(|k| k == key){
                self.order.remove(pos);
            }

            self.order.push_back(key.clone());

            return  Some(block.clone());
        }

        None
    }

    pub fn insert(&mut self, key: BlockKey, block: Vec<(String, Value)>) {

        if self.map.contains_key(&key) {
            return;
        }

        if self.map.len() >= self.capacity {

            if let Some(old) = self.order.pop_front() {
                self.map.remove(&old);
            }
        }

        self.order.push_back(key.clone());
        self.map.insert(key, block);
    }

}