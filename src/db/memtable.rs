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

    pub fn iter(&self) -> impl Iterator<Item = (&String, &String)> { 
        self.map.iter()
    }

    pub fn len(&self) -> usize{
        self.map.len()
    }

    pub fn clear(&mut self){
        self.map.clear();
    }

}

