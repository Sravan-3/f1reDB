// src/db/mod.rs
pub mod memtable;
pub mod wal;
pub mod sstable;
pub mod bloom;
pub mod compaction;
pub mod manifest;

use std::{sync::{Arc, Mutex}};
use memtable::MemTable;
use wal::Wal;
use crate::db::{bloom::BloomFilter, manifest::Manifest, sstable::SSTableMeta};


pub struct Db{
    pub memtable: MemTable,
    pub wal: Wal,
    pub sstables: Vec<SSTableMeta>,
    pub manifest: Manifest,
    pub compaction_running: bool,
}

pub type SharedDb = Arc<Mutex<Db>>;

pub fn open_db() -> SharedDb {

    let wal_path =  "wal.log";

    let mut memtable = MemTable::new();

    Wal::replay(wal_path, &mut memtable).expect("WAL Replay failed");

    let wal = Wal::open(wal_path).expect("Failed to open wal");

    let manifest = Manifest::load("MANIFEST").expect("Failed to load MANIFEST");

    let mut sstables = Vec::new();

    for path in &manifest.sstables {
        let bloom = BloomFilter::build_from_sstable(path).unwrap();
        sstables.push(SSTableMeta {
            path: path.clone(),
            bloom,
        });
    }

    Arc::new(Mutex::new(Db{
            memtable,
            wal, 
            sstables,
            manifest,
            compaction_running: false
        }))
}


