use std::{fs::read, io::Error, path::{Path, PathBuf}};

use base64::{Engine, engine::general_purpose};
use sha2::{Digest, Sha256};
use tokio::{fs::File, io::{AsyncReadExt, AsyncWriteExt}};
// You can add here other imports from std or crates listed in Cargo.toml.

// You can add any private types, structs, consts, functions, methods, etc., you need.
// As always, you should not modify the public interfaces.

const CHECKSUM_LEN: usize = 44;
const MAX_KEY_LEN: usize = 255;
const MAX_VALUE_LEN: usize = 65535;

#[async_trait::async_trait]
pub trait StableStorage: Send + Sync {
    /// Stores `value` under `key`.
    ///
    /// Detailed requirements are specified in the description of the assignment.
    async fn put(&mut self, key: &str, value: &[u8]) -> Result<(), String>;

    /// Retrieves value stored under `key`.
    ///
    /// Detailed requirements are specified in the description of the assignment.
    async fn get(&self, key: &str) -> Option<Vec<u8>>;

    /// Removes `key` and the value stored under it.
    ///
    /// Detailed requirements are specified in the description of the assignment.
    async fn remove(&mut self, key: &str) -> bool;
}

/// Creates a new instance of stable storage.
pub async fn build_stable_storage(root_storage_dir: PathBuf) -> Box<dyn StableStorage> {
    // unimplemented!()
    Box::new(    
        MyStableStorage {
            root_storage_dir
    })
}

struct MyStableStorage {
    root_storage_dir: PathBuf,
}

impl MyStableStorage {
    fn new(root_storage_dir: PathBuf) -> Self {
        MyStableStorage { root_storage_dir }
    }

    pub fn calculate_checksum(&self, value: &[u8]) -> [u8; CHECKSUM_LEN] {
        let mut hasher= Sha256::new();
        hasher.update(value);
        let hash_bytes = hasher.finalize();
        let base64_checksum = general_purpose::STANDARD.encode(hash_bytes);
        base64_checksum.as_bytes().try_into().expect("Something strange happened, Base64 checksum should be 44 bytes...")
    }
}

#[async_trait::async_trait]
impl StableStorage for MyStableStorage {
    async fn put(&mut self, key: &str, value: &[u8]) -> Result<(), String> {
        if key.len() > MAX_KEY_LEN {
            return Err(String::from("The provided key is too long (exceeds 255 bytes)!!!"));
        }
        if value.len() > MAX_VALUE_LEN {
            return Err(String::from("The provided value is too long (exceeds 65535 bytes)!!!"));            
        }

        let base64_checksum = self.calculate_checksum(value);
        // println!("length of checksum is {}", base64_checksum.len());

        let mut tmp_path = PathBuf::new();
        tmp_path.push(self.root_storage_dir.clone());
        tmp_path.push("tmp_".to_owned() + key);

        let file_res = File::create(tmp_path).await;
        if let Err(e) = file_res {
            return Err(e.to_string());
        }

        let mut file = file_res.unwrap();
        
        if let Err(e) = file.write_all(value).await {
            return Err(e.to_string());
        }

        if let Err(e) = file.write_all(&base64_checksum).await {
            return Err(e.to_string());
        }

        return Ok(())
    }

    async fn get(&self, key: &str) -> Option<Vec<u8>> {
        let mut tmp_path = PathBuf::new();
        tmp_path.push(self.root_storage_dir.clone());
        tmp_path.push("tmp_".to_owned() + key);

        let mut bytes = Vec::new();
        
        let file_res = File::open(tmp_path).await;
        if let Err(_e) = file_res {
            return None;
        }

        let read_res = file_res.unwrap().read_to_end(&mut bytes).await;
        if let Err(_e) = read_res {
            return  None;
        }

        let value_len = bytes.len() - CHECKSUM_LEN;
        let value = &bytes[..value_len];
        let read_checksum = &bytes[value_len..];

        let calc_checksum = self.calculate_checksum(value);

        if read_checksum != calc_checksum {
            println!("\n===================\nChecksum wrong!!!\n=====================");
            return None;
        }

        println!("read value is {:?}", value);

        return Some(Vec::from(value));
    }

    async fn remove(&mut self, key: &str) -> bool {
        false
    }
}
