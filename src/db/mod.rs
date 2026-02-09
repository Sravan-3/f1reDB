// src/db/mod.rs
pub mod memtable;
pub mod wal;
pub mod sstable;
pub mod bloom;  

use std::{path::PathBuf, sync::{Arc, Mutex}};
use memtable::MemTable;
use wal::Wal;


pub struct Db{
    pub memtable: MemTable,
    pub wal: Wal,
    pub sstable: Vec<PathBuf>
}

pub type SharedDb = Arc<Mutex<Db>>;

pub fn open_db() -> SharedDb {

    let wal_path =  "wal.log";

    let mut memtable = MemTable::new();

    Wal::replay(wal_path, &mut memtable).expect("WAL Replay failed");

    let wal = Wal::open(wal_path).expect("Failed to open wal");

    Arc::new(Mutex::new(Db{
            memtable,
            wal, 
            sstable: Vec::new(),
        }))
}


