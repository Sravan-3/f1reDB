// src/db/mod.rs
pub mod memtable;
pub mod wal;
pub mod sstable;
pub mod bloom;
pub mod compaction;
pub mod manifest;
pub mod static_vars;
pub mod block_cache;

use std::{
    sync::{Arc, RwLock}
};

use memtable::MemTable;
use wal::Wal;

use crate::db::bloom::BloomFilter;
use crate::db::manifest::Manifest;
use crate::db::sstable::SSTableMeta;
use crate::db::block_cache::BlockCache;

pub struct Db {
    pub memtable: MemTable,
    pub wal: Wal,
    pub level0: Vec<SSTableMeta>,
    pub level1: Vec<SSTableMeta>,
    pub level2: Vec<SSTableMeta>,
    pub manifest: Manifest,
    pub compaction_running: bool,
    pub block_cache: BlockCache,
}

pub type SharedDb = Arc<RwLock<Db>>;

pub fn open_db() -> SharedDb {

    let wal_path = "wal.log";

    let mut memtable = MemTable::new();

    Wal::replay(wal_path, &mut memtable)
        .expect("WAL Replay failed");

    let wal = Wal::open(wal_path)
        .expect("Failed to open wal");

    let manifest = Manifest::load("MANIFEST")
        .expect("Failed to load MANIFEST");

    let level0 = Vec::new();
    let mut level1 = Vec::new();
    let level2 = Vec::new();

    for path in &manifest.sstables {

        let bloom = match BloomFilter::build_from_sstable(path) {
            Ok(b) => b,
            Err(_) => continue,
        };

        let index = match crate::db::sstable::build_sparse_index(path) {
            Ok(i) => i,
            Err(_) => continue,
        };

        if index.is_empty() {
            continue;
        }

        let min_key = index.first().unwrap().0.clone();
        let max_key = index.last().unwrap().0.clone();

        level1.push(SSTableMeta {
            path: path.clone(),
            bloom,
            index,
            min_key,
            max_key,
        });
    }

    Arc::new(RwLock::new(Db {
        memtable,
        wal,
        level0,
        level1,
        level2,
        manifest,
        compaction_running: false,
        block_cache: BlockCache::new(128),
    }))
}