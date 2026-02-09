# 🔥 f1reDB

**f1reDB** is a simple key-value database written in Rust, inspired by LSM-tree based systems like RocksDB.

This project is built step by step to learn systems programming, networking, and database internals in Rust.

**Current Stage:** In-memory database + Write-Ahead Log (WAL)

--------------------------------------------------

## ✅ Features (Implemented)

- TCP server using Rust standard library
- Simple text-based protocol
- Concurrent client handling
- In-memory MemTable using `BTreeMap`
- Write-Ahead Log (WAL) for crash safety
- Automatic recovery from WAL on restart

--------------------------------------------------

## 🏗 Architecture Overview

```
Client
  |
 TCP
  |
Server
  |
  +-- Write-Ahead Log (wal.log)
  |
  +-- MemTable (BTreeMap)
```

--------------------------------------------------

## ✍️ Write Path

```
SET key value
 → append to WAL
 → write to MemTable
```

--------------------------------------------------

## 📖 Read Path

```
GET key
 → lookup in MemTable
```

--------------------------------------------------

## 🔌 Protocol

### Commands

```
SET <key> <value>\n
GET <key>\n
```

### Responses

```
OK\n
VALUE <value>\n
NOT_FOUND\n
ERROR <message>\n
```

--------------------------------------------------

## 📁 Project Structure

```
src/
├── main.rs          # Program entry point
├── lib.rs           # Library root
├── server.rs        # TCP server & client handling
├── protocol.rs      # Command parsing
└── db/
    ├── mod.rs       # Database wiring
    ├── memtable.rs  # In-memory storage
    └── wal.rs       # Write-Ahead Log
```

--------------------------------------------------

## ▶️ How to Run

### Build & Run

```
cargo run
```

The server starts on:

```
127.0.0.1:3838
```

--------------------------------------------------

## 🔗 Connect Using Netcat

```
nc 127.0.0.1 3838
```

### Example Session

```
SET name fire
OK
GET name
fire
```

--------------------------------------------------

## 💾 Crash Recovery Demo

1. Start server
2. Run:
   ```
   SET lang rust
   ```
3. Stop server (Ctrl+C)
4. Restart server
5. Run:
   ```
   GET lang
   ```

Result:

```
rust
```

Data is recovered from the Write-Ahead Log.

--------------------------------------------------

## 🧰 Tech Stack

- **Language:** Rust
- **Networking:** `std::net::TcpListener`
- **Concurrency:** `Arc<Mutex<T>>`
- **Storage:**
  - MemTable: `BTreeMap`
  - WAL: append-only file

--------------------------------------------------

## 🗺 Roadmap

Planned next steps:

- SSTable flush from MemTable
- Immutable on-disk tables
- Bloom filters for read optimization
- Compaction
- Binary file formats
- Performance improvements

--------------------------------------------------

## 🎯 Motivation

This project is built as a learning exercise to understand:

- How databases work internally
- Rust ownership and concurrency
- Networking at a low level
- Crash safety and durability

--------------------------------------------------

## ⚠️ Disclaimer

f1reDB is NOT production-ready.

It is an educational project focused on learning system design and Rust internals.

--------------------------------------------------

## 👤 Author

Built by Sravan 🚀  
Learning Rust and backend systems engineering.
