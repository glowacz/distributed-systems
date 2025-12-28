use std::{path::PathBuf};

use base64::{Engine, engine::general_purpose};
use sha2::{Digest, Sha256};
use tokio::{fs::{File, create_dir_all, read_dir, remove_file}, io::{AsyncReadExt, AsyncWriteExt}};

const CHECKSUM_LEN: usize = 44;
const MAX_KEY_LEN: usize = 255;
const MAX_VALUE_LEN: usize = 65535;

#[async_trait::async_trait]
pub trait StableStorage: Send + Sync {
    /// Stores `value` under `key`.
    ///
    /// Detailed requirements are specified in the description of the assignment.
    async fn put(&self, key: &str, value: &[u8]) -> Result<(), String>;

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
    let storage = MyStableStorage::new(root_storage_dir).await;
    Box::new(storage)
}

pub async fn build_my_stable_storage(root_storage_dir: PathBuf) -> MyStableStorage {
    MyStableStorage::new(root_storage_dir).await
}

pub struct MyStableStorage {
    root_storage_dir: PathBuf,
}

impl MyStableStorage {
    async fn new(root_storage_dir: PathBuf) -> Self {
        let storage = MyStableStorage { root_storage_dir };
        storage.process_tmp_files().await;
        storage
    }

    async fn process_tmp_files(&self) {
        let tmp_dir_path = self.root_storage_dir.join("tmp");
        create_dir_all(&tmp_dir_path).await.unwrap();

        let mut tmp_dir_entries = read_dir(&tmp_dir_path).await.unwrap();
        let tmp_dir = File::open(&tmp_dir_path).await.unwrap();
        let root_dir = File::open(&self.root_storage_dir).await.unwrap();

        while let Some(tmp_dir_entry) = tmp_dir_entries.next_entry().await.unwrap() {
            let mut bytes = Vec::new();
            let tmp_file_path = tmp_dir_entry.path();
            File::open(&tmp_file_path).await.unwrap().read_to_end(&mut bytes).await.unwrap();

            if bytes.len() >= CHECKSUM_LEN {
                let value_len = bytes.len() - CHECKSUM_LEN;
                let value = &bytes[..value_len];
                let read_checksum = &bytes[value_len..];

                let calc_checksum = self.calculate_checksum(value);

                if read_checksum == calc_checksum {
                    // good tmp file, but didn't manage to flush main file
                    let file_name = tmp_dir_entry.file_name();
                    let file_path = self.root_storage_dir.join(file_name);
                    let mut file = File::create(file_path).await.unwrap();
                    file.write_all(value).await.unwrap();
                    file.sync_data().await.unwrap();
                    root_dir.sync_data().await.unwrap();
                }
                // else can't do anything, bad tmp file
            }

            remove_file(&tmp_file_path).await.unwrap();
            tmp_dir.sync_data().await.unwrap();
        }
    }

    fn calculate_checksum(&self, value: &[u8]) -> [u8; CHECKSUM_LEN] {
        let mut hasher= Sha256::new();
        hasher.update(value);
        let hash_bytes = hasher.finalize();
        let base64_checksum = general_purpose::STANDARD.encode(hash_bytes);
        base64_checksum.as_bytes().try_into().expect("Something strange happened, Base64 checksum should be 44 bytes...")
    }

    async fn sync_dir(&self) -> Result<(), String> {
        let dir_res = File::open(self.root_storage_dir.clone()).await;
        if let Err(e) = dir_res {
            return Err(e.to_string());
        }
        let dir = dir_res.unwrap();

        if let Err(e) = dir.sync_data().await {
            return Err(e.to_string());
        };

        return Ok(());
    }

    fn create_path(&self, key: &str) -> PathBuf {
        let mut path = PathBuf::new();
        path.push(self.root_storage_dir.clone());
        path.push(key);
        return path
    }

    fn create_tmp_path(&self, key: &str) -> PathBuf {
        let mut path = PathBuf::new();
        path.push(self.root_storage_dir.join("tmp"));
        path.push(key);
        return path
    }

    async fn write_and_sync(&self, path: PathBuf, value: &[u8], checksum: Option<&[u8]>) -> Result<(), String> {
        let file_res = File::create(path).await;
        if let Err(e) = file_res {
            return Err(e.to_string());
        }

        let mut file = file_res.unwrap();
        
        if let Err(e) = file.write_all(value).await {
            return Err(e.to_string());
        }

        if let Some(s) = checksum {
            if let Err(e) = file.write_all(s).await {
                return Err(e.to_string());
            }
        }

        if let Err(e) = file.sync_data().await {
            return Err(e.to_string());
        }

        return self.sync_dir().await;
    }
}

#[async_trait::async_trait]
impl StableStorage for MyStableStorage {
    async fn put(&self, key: &str, value: &[u8]) -> Result<(), String> {
        if key.len() > MAX_KEY_LEN {
            return Err(String::from("The provided key is too long (exceeds 255 bytes)!!!"));
        }
        if value.len() > MAX_VALUE_LEN {
            return Err(String::from("The provided value is too long (exceeds 65535 bytes)!!!"));            
        }

        let checksum = self.calculate_checksum(value);

        let tmp_path = self.create_tmp_path(key);

        if let Err(e) = self.write_and_sync(tmp_path.clone(), value, Some(&checksum)).await {
            return Err(e);
        }

        let path = self.create_path(key);

        if let Err(e) = self.write_and_sync(path, value, None).await {
            return Err(e);
        }

        if let Err(e) = remove_file(tmp_path).await {
            return Err(e.to_string());
        }

        return self.sync_dir().await;
    }

    async fn get(&self, key: &str) -> Option<Vec<u8>> {
        let mut bytes = Vec::new();
        let path = self.create_path(key);

        let mut file = match File::open(path).await {
            Ok(file) => file,
            Err(_) => return None,
        };
        
        file.read_to_end(&mut bytes).await.unwrap();

        return Some(bytes);
    }

    async fn remove(&mut self, key: &str) -> bool {        
        let path = self.create_path(key);
        let file_res = File::open(path.clone()).await;
        if let Ok(_v) = file_res {
            if let Err(_e) = remove_file(path).await {
                return false;
            }
        }
        else {
            return false;
        }

        return true;
    }
}