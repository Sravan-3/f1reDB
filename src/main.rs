use f1redb::server;
use f1redb::db;

#[tokio::main]
async fn main() {

    let db = db::open_db();
    server::start("127.0.0.1:6379", db).await;
}