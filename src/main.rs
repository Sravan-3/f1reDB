use f1redb::server;
use f1redb::db;

fn main() {

    let db = db::open_db();
    server::start("127.0.0.1:3838", db);

}