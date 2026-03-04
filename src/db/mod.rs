// src/db/mod.rs
pub mod memtable;
pub mod wal;
pub mod sstable;
pub mod bloom;
pub mod compaction;
pub mod manifest;
pub mod static_vars;

use std::{sync::{Arc, RwLock}};
use memtable::MemTable;
use wal::Wal;
use crate::db::{bloom::BloomFilter, manifest::Manifest, sstable::SSTableMeta};


pub struct Db{
    pub memtable: MemTable,
    pub wal: Wal,
    pub level0: Vec<SSTableMeta>,
    pub level1: Vec<SSTableMeta>,
    pub manifest: Manifest,
    pub compaction_running: bool,
}

pub type SharedDb = Arc<RwLock<Db>>;

pub fn open_db() -> SharedDb {

    let wal_path =  "wal.log";

    let mut memtable = MemTable::new();

    Wal::replay(wal_path, &mut memtable).expect("WAL Replay failed");

    let wal = Wal::open(wal_path).expect("Failed to open wal");

    let manifest = Manifest::load("MANIFEST").expect("Failed to load MANIFEST");

    let level0 = Vec::new();
    let mut level1 = Vec::new();

    for path in &manifest.sstables {
        let bloom = BloomFilter::build_from_sstable(path).unwrap();
        let index = sstable::build_sparse_index(path).unwrap();

        let min_key = index.first().map(|(k, _)| k.clone()).unwrap_or_default();
        let max_key = index.last().map(|(k, _)| k.clone()).unwrap_or_default();

        level1.push(SSTableMeta {
            path: path.clone(),
            bloom,
            index,
            min_key,
            max_key,
        });
    }

    Arc::new(RwLock::new(Db{
            memtable,
            wal, 
            level0,
            level1,
            manifest,
            compaction_running: false
        }))
}


