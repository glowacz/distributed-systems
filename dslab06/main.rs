use std::time::Duration;

use tokio::time::timeout;

mod public_test;
mod solution;

async fn write() {
    let root_storage_dir = std::env::temp_dir().join("stable_storage_data");
    tokio::fs::create_dir(&root_storage_dir).await.unwrap();

    let mut storage = solution::build_stable_storage(root_storage_dir.clone()).await;
    // timeout(Duration::from_nanos(1), panic!("system crash"));
    storage.put("key", "value".as_bytes()).await.unwrap();


    tokio::fs::remove_dir_all(root_storage_dir).await.unwrap();
}

async fn read() {
    let root_storage_dir = std::env::temp_dir().join("stable_storage_data");

    let storage = solution::build_stable_storage(root_storage_dir.clone()).await;
    let value = String::from_utf8(storage.get("key").await.unwrap()).unwrap();
    println!("Recovered value: '{value}'");

    // let mut storage = storage;
    // let removed = storage.remove("key").await;
    // println!("Removed the value? {removed:?}");
}

#[tokio::main]
// async fn main() {
//     let root_storage_dir = std::env::temp_dir().join("stable_storage_data");
//     tokio::fs::create_dir(&root_storage_dir).await.unwrap();

//     {
//         let mut storage = solution::build_stable_storage(root_storage_dir.clone()).await;
//         storage.put("key", "value".as_bytes()).await.unwrap();
//     } // "crash"

//     {
//         let storage = solution::build_stable_storage(root_storage_dir.clone()).await;
//         let value = String::from_utf8(storage.get("key").await.unwrap()).unwrap();
//         println!("Recovered value: '{value}'");

//         let mut storage = storage;
//         let removed = storage.remove("key").await;
//         println!("Removed the value? {removed:?}");
//     }

//     tokio::fs::remove_dir_all(root_storage_dir).await.unwrap();
// }
async fn main() {
    write().await;
    // read().await;
}
