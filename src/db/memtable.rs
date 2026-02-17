use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub enum Value {
    Data(String),
    Tombstone,
}

#[derive(Debug)]
pub struct MemTable{
    map: BTreeMap<String, Value>,
}

impl MemTable {
    
    pub fn new() -> MemTable{
        MemTable { 
            map: BTreeMap::new() 
        }
    }

    pub fn set(&mut self, key: String, value: String){
        self.map.insert(key, Value::Data(value));
    }

    pub fn delete(&mut self, key: String){
        self.map.insert(key, Value::Tombstone);
    }

    pub fn get(&self, key: &str) -> Option<Value>{
        return self.map.get(key).cloned();
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &Value)> { 
        self.map.iter()
    }

    pub fn len(&self) -> usize{
        self.map.len()
    }

    pub fn clear(&mut self){
        self.map.clear();
    }

}
