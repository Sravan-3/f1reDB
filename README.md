
---

# 🔥 f1reDB

**f1reDB** is a lightweight **LSM-tree based key-value database** written in Rust.
It is built step-by-step to learn **database internals, storage engines, networking, and concurrency** in Rust.

The design is inspired by modern storage engines such as **RocksDB and LevelDB**, but implemented from scratch for educational purposes.

**Current Stage:** Async database server + LSM-tree storage engine.

---

# ✅ Features

### Networking

* Async TCP server using **Tokio**
* Concurrent client handling with **async tasks**
* Lightweight text protocol

### Storage Engine

* **MemTable** (in-memory write buffer using `BTreeMap`)
* **Write-Ahead Log (WAL)** for durability
* **SSTables** for persistent sorted storage
* **Bloom filters** for fast negative lookups
* **Sparse index** for efficient disk seeks
* **Tombstones** for deletes
* **LSM Levels** (Level0 → Level1)
* **Background compaction**

### Reliability

* Crash-safe writes via WAL
* Automatic **WAL replay** on restart
* Manifest file to track SSTables

---

# 🏗 Architecture Overview

```
                +-------------+
Client  ----->  |  TCP Server |
                +------+------+
                       |
                Command Parser
                       |
                +------+------+
                |   Database   |
                +------+------+
                       |
       +---------------+---------------+
       |                               |
   Write-Ahead Log                 MemTable
      (durability)              (in-memory buffer)
                                       |
                                Flush threshold
                                       |
                                    SSTable
                                       |
                         +-------------+-------------+
                         |                           |
                       Level 0                    Level 1
                    (overlapping)              (sorted ranges)
                         |
                    Compaction
```

---

# ✍️ Write Path

```
SET key value
    ↓
Append to WAL
    ↓
Insert into MemTable
    ↓
MemTable threshold reached
    ↓
Flush → SSTable
    ↓
Add to Level0
    ↓
Background compaction (L0 → L1)
```

---

# 📖 Read Path

```
GET key
   ↓
MemTable
   ↓
Level0 SSTables (newest first)
   ↓
Bloom Filter Check
   ↓
Sparse Index Seek
   ↓
Level1 SSTables
   ↓
NOT_FOUND
```

Read optimizations used:

* Bloom filters
* Range filtering (`min_key`, `max_key`)
* Sparse index seeking

---

# 🔌 Protocol

### Commands

```
SET <key> <value>\n
GET <key>\n
DEL <key>\n
```

### Responses

```
OK\n
VALUE <value>\n
NOT_FOUND\n
ERROR <message>\n
```

---

# 📁 Project Structure

```
src/
├── main.rs          # Application entry point
├── server.rs        # Async TCP server (Tokio)
├── protocol.rs      # Command parser
└── db/
    ├── mod.rs       # Database initialization
    ├── memtable.rs  # In-memory table
    ├── wal.rs       # Write-Ahead Log
    ├── sstable.rs   # SSTable storage
    ├── bloom.rs     # Bloom filter implementation
    ├── compaction.rs# LSM compaction logic
    ├── manifest.rs  # SSTable tracking
    └── static_vars.rs
```

---

# ▶️ How to Run

### Build & Run

```
cargo run
```

Server starts at:

```
127.0.0.1:55000
```

---

# 🔗 Connect Using Netcat

```
nc 127.0.0.1 55000
```

---

# 💬 Example Session

```
SET name fire
OK

GET name
VALUE fire

DEL name
OK

GET name
NOT_FOUND
```

---

# 💾 Crash Recovery Demo

1. Start the server
2. Run:

```
SET lang rust
```

3. Stop server (`Ctrl+C`)
4. Restart server
5. Run:

```
GET lang
```

Result:

```
VALUE rust
```

The value is recovered using **WAL replay**.

---

# ⚡ Storage Design

### MemTable

```
BTreeMap<String, Value>
```

Keeps keys sorted to simplify SSTable flushing.

### SSTable

On-disk sorted key-value table containing:

* key-value entries
* Bloom filter
* sparse index
* key range metadata

### Bloom Filter

Used to quickly determine if a key **cannot exist** in an SSTable.

This avoids unnecessary disk reads.

### Sparse Index

Stores key → offset mappings every N rows.

Lookup algorithm:

```
Binary search sparse index
      ↓
Seek file offset
      ↓
Scan small range
```

---

# 🧰 Tech Stack

| Component      | Technology          |
| -------------- | ------------------- |
| Language       | Rust                |
| Networking     | Tokio async runtime |
| Concurrency    | `Arc<RwLock<Db>>`   |
| MemTable       | `BTreeMap`          |
| Disk Storage   | SSTables            |
| Crash Recovery | Write-Ahead Log     |

---

# 🗺 Roadmap

Planned improvements:

* Block-based SSTables
* Block cache
* Multi-level LSM tree (L2–L6)
* Range queries
* Async compaction scheduler
* Manifest recovery improvements
* Binary protocol
* Metrics and monitoring

---

# 🎯 Motivation

f1reDB is a learning project focused on understanding:

* LSM-tree storage engines
* Database architecture
* Rust concurrency and ownership
* Async networking
* Crash recovery and durability

The goal is to **build a small but real database engine from scratch**.

---

# ⚠️ Disclaimer

f1reDB is **not production-ready**.

This project is intended for **educational purposes** to explore database internals and systems programming in Rust.

---

# 👤 Author

Built by **Sravan Gandla** 🚀
Learning Rust, databases, and backend systems engineering.
