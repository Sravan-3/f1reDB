use std::fs::{OpenOptions, File};
use std::io::{Write, BufRead, BufReader};
use std::path::Path;
use crate::db::memtable::MemTable;

#[derive(Debug)]
pub struct Wal{
    file: File
}

impl Wal {

    pub fn open(path: &str) -> std::io::Result<Self> {
        let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;

        Ok(Self{file})
    }

    pub fn log_set(&mut self, key: &str, value: &str) -> std::io::Result<()>{
        writeln!(self.file, "SET {} {}", key, value)?;
        self.file.flush()?;
        Ok(())
    }

    pub fn log_delete(&mut self, key: &str) -> std::io::Result<()> {
        writeln!(self.file, "DEL {}", key)?;
        self.file.flush()?;
        Ok(())
    }

    pub fn replay(path: &str, memtable:&mut MemTable) -> std::io::Result<()>{

        if !Path::new(path).exists(){
            return Ok(())
        }

        let file = File::open(path)?;
        let reader = BufReader::new(file);

        for line in reader.lines(){

            let line = line?;
            let parts: Vec<&str> = line.split_whitespace().collect();

            if parts.len() == 3 && parts[0] == "SET" {
                memtable.set(parts[1].to_string(), parts[2].to_string());
            }
            else if parts.len() == 2 && parts[0] == "DEL" {
                memtable.delete(parts[1].to_string());
            }
        }

        Ok(())
    }

}