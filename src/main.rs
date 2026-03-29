use tokio::signal;
use std::sync::Arc;

use f1redb::server;
use f1redb::db::{self, SharedDb};
use f1redb::db::sstable;

#[tokio::main]
async fn main() {

    let db = db::open_db();

    let db_clone = Arc::clone(&db);

    tokio::spawn(async move {
        server::start("127.0.0.1:55000", db_clone).await;
    });

    signal::ctrl_c().await.unwrap();

    println!("\nShutting down gracefully...");

    shutdown(db);
}

fn shutdown(db_arc: SharedDb) {

    let mut db = db_arc.write().unwrap();

    if db.memtable.len() > 0 {

        let id = db.manifest.allocate_sstable_id();

        let meta = sstable::flush(&db.memtable, id)
            .expect("flush failed");

        db.manifest.add_sstable(0, meta.path.clone());
        db.manifest.persist("MANIFEST").unwrap();

        db.level0.push(meta);
        db.memtable.clear();
    }

    {
        let mut wal = db.wal.lock().unwrap();
        let _ = wal.flush();
    }

    println!("Shutdown complete");
}