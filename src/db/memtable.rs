use std::collections::BTreeMap;

#[derive(Debug)]
pub struct MemTable{
    map: BTreeMap<String, String>
}

impl MemTable {
    
    pub fn new() -> MemTable{
        MemTable { 
            map: BTreeMap::new() 
        }
    }

    pub fn set(&mut self, key: String, value: String){
        self.map.insert(key, value);
    }

    pub fn get(&self, key: &str) -> Option<String>{
        return self.map.get(key).cloned();
    }
}

