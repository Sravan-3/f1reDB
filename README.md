# f1reDB 🔥

A **log-structured merge-tree (LSM-tree) key-value store** implemented in Rust. f1reDB is built from first principles — no embedded storage libraries. Every component of the storage engine (MemTable, WAL, SSTable, Bloom filter, compaction) is hand-rolled to deeply understand how production databases like RocksDB and LevelDB work internally.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Storage Engine Internals](#storage-engine-internals)
  - [MemTable](#memtable)
  - [Write-Ahead Log (WAL)](#write-ahead-log-wal)
  - [SSTable Format](#sstable-format)
  - [Bloom Filter](#bloom-filter)
  - [Sparse Index](#sparse-index)
  - [Block Cache](#block-cache)
  - [Compaction](#compaction)
  - [Manifest](#manifest)
- [Write Path](#write-path)
- [Read Path](#read-path)
- [Concurrency Model](#concurrency-model)
- [Crash Recovery](#crash-recovery)
- [Wire Protocol](#wire-protocol)
- [Project Structure](#project-structure)
- [Running f1reDB](#running-f1redb)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                        TCP Client                           │
└───────────────────────────┬─────────────────────────────────┘
                            │  text protocol over TCP
                            ▼
┌─────────────────────────────────────────────────────────────┐
│               Async TCP Server  (Tokio)                     │
│         spawns one task per connection                      │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    Command Parser                           │
│          SET / GET / DEL / STATS → typed enum               │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                      Database Core                          │
│                  Arc<RwLock<DbState>>                       │
│                                                             │
│   ┌──────────────────┐      ┌──────────────────────────┐    │
│   │  Write-Ahead Log │      │        MemTable          │    │
│   │  (WAL)           │      │  BTreeMap<String, Value> │    │
│   │  append-only,    │      │  sorted, in-memory       │    │
│   │  batched flush   │      │  write buffer            │    │
│   └──────────────────┘      └────────────┬─────────────┘    │
│                                          │ flush threshold  │
│                                          ▼                  │
│                              ┌───────────────────────┐      │
│                              │       SSTable         │      │
│                              │  sorted, immutable,   │      │
│                              │  disk-resident file   │      │
│                              └───────────┬───────────┘      │
│                                          │                  │
│              ┌───────────────────────────┼────────────────┐ │
│              ▼                           ▼                ▼ │
│         ┌─────────┐               ┌─────────┐       ┌──────┐│
│         │ Level 0 │  compaction   │ Level 1 │       │  L2  ││
│         │ overlap │ ──────────>   │ no-over │  ───> │      ││
│         └─────────┘               └─────────┘       └──────┘│
└─────────────────────────────────────────────────────────────┘
```

---

## Storage Engine Internals

### MemTable

The MemTable is an **in-memory write buffer** backed by a `BTreeMap<String, Value>`.

- All writes land here first (after the WAL).
- `BTreeMap` keeps keys sorted, which makes flushing to an SSTable a sequential scan — no re-sorting needed.
- Once the MemTable exceeds a size threshold, it's flushed to a new Level 0 SSTable.
- During a flush, a new empty MemTable is swapped in so writes aren't blocked.

```
Value enum:
  Value::Data(String)   → live key-value pair
  Value::Tombstone      → logical delete marker
```

Tombstones are required because deletes in an LSM tree can't immediately remove data from disk — older SSTables may still contain the key. Tombstones propagate downward through compaction and are only dropped when they reach the bottom level with no older versions beneath.

---

### Write-Ahead Log (WAL)

The WAL is an **append-only file** that guarantees durability before acknowledging a write.

**Write flow:**
1. Serialize the operation (`SET key value` / `DEL key`) to the WAL buffer.
2. Periodically flush the buffer to disk (batched I/O).
3. Insert into MemTable.
4. Return `OK` to client.

**Why batching?** Fsyncing on every write is expensive. Batching trades a small durability window for significantly higher write throughput — the same tradeoff RocksDB and Postgres make with `group commit`.

**WAL reset:** After the MemTable is successfully flushed to an SSTable and the Manifest is updated, the WAL is truncated. The SSTable is now the durable record; the WAL entries for those keys are no longer needed.

**Recovery:** On startup, f1reDB replays the WAL sequentially to reconstruct the MemTable state, recovering any writes that hadn't been flushed to an SSTable before the crash.

---

### SSTable Format

SSTables (Sorted String Tables) are **immutable, disk-resident files** produced by flushing the MemTable or by compaction merging lower-level SSTables.

**On-disk layout:**

```
┌─────────────────────────────────────┐
│         Data Blocks                 │
│  [key_len | key | val_len | value]  │  ← sorted key-value entries
│  [key_len | key | TOMBSTONE]        │  ← tombstone entry
│  ...                                │
├─────────────────────────────────────┤
│         Sparse Index                │
│  [key | offset]  (every N entries)  │  ← one entry per block interval
│  ...                                │
├─────────────────────────────────────┤
│  Bloom Filter (serialized bitset)   │
├─────────────────────────────────────┤
│             Footer                  │
│  [index_offset | index_size |       │
│   bloom_offset | bloom_size |       │
│   min_key | max_key]                │
└─────────────────────────────────────┘
```

- The **footer** is read first on file open to locate the index and bloom filter.
- The **`min_key` / `max_key`** in the footer enable range filtering — if the target key falls outside this range, the SSTable is skipped entirely without reading the index or bloom filter.
- Entries are written in sorted order, enabling binary search via the sparse index.

---

### Bloom Filter

Each SSTable carries a **Bloom filter** — a space-efficient probabilistic data structure that answers: *"Is this key definitely not in this file?"*

- **False positives** are possible (the filter may claim a key is present when it isn't).
- **False negatives** are impossible (if the filter says absent, the key is guaranteed absent).
- Allows skipping SSTables on the read path with no disk I/O beyond reading the filter itself (which is held in memory after file open).

The filter is serialized into the SSTable at flush time and loaded into memory on file open.

---

### Sparse Index

A full index (one entry per key) would be memory-prohibitive for large SSTables. The sparse index stores **one entry every N keys**, trading some seek precision for drastically reduced memory footprint.

**Lookup algorithm:**
```
1. Binary search the sparse index for the largest indexed key ≤ target key
2. Seek to the corresponding file offset
3. Linear scan forward through the data block until key is found or passed
```

This gives O(log N) index search + O(block_size) scan — effectively O(log N) for small, fixed block sizes.

---

### Block Cache

The block cache is an **LRU cache** that stores recently accessed SSTable data blocks in memory.

- Reads check the cache before issuing a disk read.
- Cache entries are keyed by `(sstable_id, block_offset)`.
- Significantly reduces I/O for hot keys and repeated scans on the same SSTable.
- Shared across all concurrent read operations via `Arc<RwLock<BlockCache>>`.

---

### Compaction

Compaction is the background process that **merges and rewrites SSTables** to reclaim space, drop stale versions, and resolve tombstones.

**Level structure:**

| Level | Key range property | Notes |
|---|---|---|
| Level 0 | Overlapping | Multiple L0 files may contain the same key. Ordered by recency — newer files take precedence. |
| Level 1 | Non-overlapping | Produced by merging L0 files. Each key appears in exactly one L1 SSTable. |
| Level 2 | Non-overlapping, fully compacted | Tombstones dropped here if no older data exists beneath. |

**Compaction algorithm:** When Level 0 accumulates more than a threshold number of SSTables, a background task performs a **k-way merge** across all L0 files, producing non-overlapping Level 1 SSTables. The same process applies between L1 and L2.

**Backpressure / write stall:** If the Level 0 file count reaches a hard limit, incoming writes are stalled until compaction reduces the count. This bounds read amplification — without stalling, a read would need to check an unbounded number of L0 files.

**Why compaction is necessary in LSM trees:**
- Deletes are lazy (tombstones). Without compaction, deleted keys accumulate indefinitely.
- Multiple versions of the same key accumulate across SSTables. Compaction retains only the latest.
- Read amplification grows with L0 file count. Compaction bounds this at the cost of write amplification.

---

### Manifest

The Manifest is a **persistent metadata file** that tracks which SSTable files exist at each level and their key ranges.

- Updated atomically after every flush and every compaction.
- On startup, the Manifest is read before WAL replay to reconstruct the full engine state: which SSTables are live, which level they occupy, and their `min_key`/`max_key` bounds.
- Prevents orphaned SSTable files from being read after a crash mid-compaction. If a new SSTable isn't in the Manifest, it doesn't exist from the engine's perspective.

---

## Write Path

```
SET key value
      │
      ▼
Backpressure check ──→ stall if L0 file count ≥ hard_limit
      │
      ▼
Acquire write lock on DbState
      │
      ▼
Serialize op to WAL buffer → background fsync thread flushes to disk
      │
      ▼
Insert (key, Value::Data) into MemTable
      │
      ▼
Release write lock → return OK to client
      │
      ▼  (async, off the hot path)
MemTable.size() ≥ flush_threshold?
      │
      ├── yes → freeze MemTable
      │          iterate BTreeMap in sorted order
      │          write SSTable file (data blocks + index + bloom + footer)
      │          update Manifest (add file to L0)
      │          reset WAL
      │          swap in new empty MemTable
      │
      └── L0 file count ≥ compaction_threshold?
               │
               └── yes → spawn background compaction task
                         k-way merge L0 → L1 → L2
```

---

## Read Path

```
GET key
    │
    ▼
Acquire read lock on DbState
    │
    ▼
MemTable.get(key)
    ├── Value::Data(v)    → return VALUE v
    ├── Value::Tombstone  → return NOT_FOUND
    └── None              → continue
    │
    ▼
For each SSTable in Level 0 (newest → oldest):
    │
    ├── footer.min_key > key or footer.max_key < key → skip
    │
    ├── bloom_filter.contains(key) == false → skip
    │
    ├── block_cache.get(sstable_id, block_offset) → hit? return VALUE
    │
    └── sparse_index.binary_search(key)
             → seek to offset
             → scan data block
             ├── Value::Data(v)    → cache block → return VALUE v
             ├── Value::Tombstone  → return NOT_FOUND
             └── key not found     → continue to next SSTable
    │
    ▼
Repeat for Level 1, then Level 2
    │
    ▼
return NOT_FOUND
```

**Read amplification worst case:** O(L0\_count + L1\_count + L2\_count) SSTable checks. In practice, bloom filters and range checks eliminate most candidates before any block I/O.

---

## Concurrency Model

f1reDB uses Rust's ownership system to eliminate data races at compile time, with Tokio providing async concurrency on top.

| Primitive | Location | Purpose |
|---|---|---|
| `Arc<RwLock<DbState>>` | Database core | Multiple concurrent readers; exclusive writers for MemTable mutations |
| `tokio::spawn` | TCP server | Each TCP connection runs in its own async task |
| `tokio::sync::Mutex` | WAL | Serializes WAL buffer appends across concurrent write tasks |
| `Arc<RwLock<BlockCache>>` | Block cache | Shared across all read tasks; writer lock only on cache eviction/insert |
| Background `tokio::spawn` | Compaction | Runs independently; acquires write lock only when swapping in new SSTable list |

The write lock on `DbState` is held only for the MemTable insert — not for the WAL fsync, which happens asynchronously outside the critical section. This minimizes lock contention under concurrent write load.

---

## Crash Recovery

f1reDB is designed to survive crashes at any point in the write path without data loss.

| Crash point | State on disk | Recovery behavior |
|---|---|---|
| After WAL append, before MemTable insert | WAL has the entry; MemTable does not | WAL replay re-inserts the entry into MemTable |
| After MemTable insert, before flush | WAL has the entry; no SSTable written | WAL replay reconstructs MemTable; flush happens normally |
| During SSTable flush | Partial SSTable on disk; Manifest not yet updated | Partial file ignored (not in Manifest); WAL replay recovers the data |
| After flush, before Manifest update | SSTable written; Manifest stale | SSTable treated as orphaned; WAL replay re-flushes the data |
| After Manifest update | SSTable live; WAL entries now redundant | WAL truncated; SSTable is the canonical record |

**Recovery sequence on startup:**
```
1. Read Manifest → reconstruct level metadata and SSTable file list
2. Open and validate each SSTable listed in Manifest (read footer, load bloom filter)
3. Replay WAL sequentially → re-insert entries not yet flushed to an SSTable
4. Begin accepting TCP connections
```

---

## Wire Protocol

f1reDB uses a **line-oriented text protocol** over TCP — human-readable and trivially debuggable with `netcat` or `telnet`.

### Request format

```
COMMAND [arg1] [arg2]\n
```

### Commands

| Command | Arguments | Behavior |
|---|---|---|
| `SET` | `<key> <value>` | Writes `key → value`. Overwrites if key exists. Appends to WAL, inserts into MemTable. |
| `GET` | `<key>` | Checks MemTable → L0 → L1 → L2. Returns first match or `NOT_FOUND`. |
| `DEL` | `<key>` | Writes a tombstone. Key reads as `NOT_FOUND` until compaction drops the tombstone. |
| `STATS` | *(none)* | Returns engine metrics: set count, get count, block cache hit/miss ratio, compaction count. |

### Responses

| Response | Condition |
|---|---|
| `OK` | Write acknowledged (WAL flushed, MemTable updated) |
| `VALUE <data>` | Key found; `<data>` is the raw value string |
| `NOT_FOUND` | Key absent in all levels, or latest version is a tombstone |
| `ERROR <message>` | Parse error or internal engine error |

### Example session

```
$ nc 127.0.0.1 55000

SET user:1 alice
OK

GET user:1
VALUE alice

DEL user:1
OK

GET user:1
NOT_FOUND

STATS
SETS 381
GETS 75
CACHE_HIT 41
CACHE_MISS 7
COMPACTIONS 0
```

---

## Project Structure

```
src/
├── main.rs              # Entry point; initializes Tokio runtime, starts TCP server
├── server.rs            # Async TCP listener; spawns one task per accepted connection
├── protocol.rs          # Command tokenization and response serialization
└── db/
    ├── mod.rs           # Database core; owns DbState, coordinates all subsystems
    ├── memtable.rs      # In-memory BTreeMap write buffer; flush logic
    ├── wal.rs           # Append-only write-ahead log; batched fsync; replay on startup
    ├── sstable.rs       # SSTable writer + reader; footer parsing; sparse index; key lookup
    ├── bloom.rs         # Bloom filter; bit array + hash functions; serialize/deserialize
    ├── compaction.rs    # Background compaction; k-way merge; level promotion
    ├── manifest.rs      # Persistent SSTable registry; atomic update on flush/compaction
    ├── block_cache.rs   # LRU block cache; keyed by (sstable_id, block_offset)
    ├── metrics.rs       # Counters for compaction runs, cache hits/misses, write stalls
    └── static_vars.rs   
```

---

## Running f1reDB

**Prerequisites:** Rust toolchain via `rustup`

```bash
# Build and start the server
cargo run

# Listens at 127.0.0.1:55000

# Connect interactively
nc 127.0.0.1 55000

# Pipe commands non-interactively
echo -e "SET foo bar\nGET foo" | nc 127.0.0.1 55000
```

**Verify crash recovery:**

```bash
# Terminal 1: start server
cargo run

# Terminal 2: write a key
echo "SET lang rust" | nc 127.0.0.1 55000
# → OK

# Kill Terminal 1 (Ctrl+C), restart:
cargo run

# Terminal 2: read after restart
echo "GET lang" | nc 127.0.0.1 55000
# → VALUE rust   (recovered via WAL replay)
```

---

## Tech Stack

| Component | Technology | Notes |
|---|---|---|
| Language | Rust (stable) | Ownership model eliminates data races at compile time |
| Async runtime | Tokio | Multi-threaded work-stealing scheduler |
| Concurrency | `Arc` + `RwLock` | Shared mutable state without garbage collection |
| Storage format | Custom SSTable | Hand-rolled binary format: data blocks, sparse index, bloom filter, footer |
| Durability | WAL + Manifest | Append-only log + atomic metadata file for crash safety |
| Read optimization | Bloom filter + sparse index + block cache | Three-layer stack to minimize disk I/O |

---

> ⚠️ **Disclaimer:** f1reDB is an educational implementation of database internals. Not hardened for production use.

---

Built by **Sravan** — studying LSM-tree storage engines, Rust systems programming, and database architecture from the ground up.