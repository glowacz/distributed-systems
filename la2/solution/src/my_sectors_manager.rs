use crate::sectors_manager_public::SectorsManager;
use crate::{SectorIdx, SectorVec};
use std::path::PathBuf;
use crate::stable_storage::{MyStableStorage, StableStorage, build_my_stable_storage};
use serde_big_array::Array;
use tokio::{fs::{File, create_dir_all, read_dir, remove_file}, io::{AsyncReadExt, AsyncWriteExt}};
use std::collections::HashMap;

pub struct MySectorsManager {
    path: PathBuf,
    storage: MyStableStorage,
    // idx_map: HashMap<SectorIdx, String>,
}

impl MySectorsManager {
    pub async fn new(path: PathBuf) -> Self {
        let mut manager = Self { 
            path: path.clone(),
            storage: build_my_stable_storage(path.clone()).await,
            // idx_map: HashMap::new(),
        };

        // manager.create_idx_map().await;

        manager
    }

    // pub async fn create_idx_map(&mut self) {
    //     let mut dir_entries = read_dir(&self.path).await.unwrap();
    //     // let dir: File = File::open(&self.path).await.unwrap();

    //     while let Some(dir_entry) = dir_entries.next_entry().await.unwrap() {
    //         let file_path = dir_entry.path();

    //         if file_path.is_file() {
    //             if let Some(file_name) = file_path.file_name() {
    //                 if let Some(name_str) = file_name.to_str() {
    //                     let idx_opt  = name_str
    //                         .split_once('_')
    //                         .map(|(prefix, _)| prefix)
    //                         .and_then(|idx_str| idx_str.parse::<u64>().ok());

    //                     if let Some(idx) = idx_opt {
    //                         self.idx_map.insert(idx, name_str.to_string());
    //                     }
    //                 }
    //             }
    //         }
    //     }
    // }

    pub async fn get_filename_from_idx(&self, search_idx: SectorIdx) -> Option<String> {
        let mut dir_entries = read_dir(&self.path).await.unwrap();
        // let dir: File = File::open(&self.path).await.unwrap();

        while let Some(dir_entry) = dir_entries.next_entry().await.unwrap() {
            let file_path = dir_entry.path();

            if file_path.is_file() {
                if let Some(file_name) = file_path.file_name() {
                    if let Some(name_str) = file_name.to_str() {
                        let idx_opt  = name_str
                            .split_once('_')
                            .map(|(prefix, _)| prefix)
                            .and_then(|idx_str| idx_str.parse::<u64>().ok());

                        if let Some(idx) = idx_opt {
                            if idx == search_idx {
                                return Some(name_str.to_string());
                            }
                        }
                    }
                }
            }
        }

        return None;
    }
}

#[async_trait::async_trait]
impl SectorsManager for MySectorsManager {
    async fn read_data(&self, idx: SectorIdx) -> SectorVec {
        let file_name = match self.get_filename_from_idx(idx).await {
            None => {
                return SectorVec(Box::new(Array([0u8; 4096])));
            },
            Some(name) => name,
        };
        let data_vec = self.storage.get(file_name.as_str()).await.unwrap();
        let data_array = data_vec.try_into().unwrap();
        let wrapped_array = Array(data_array);
        SectorVec(Box::new(wrapped_array))
    }

    async fn read_metadata(&self, idx: SectorIdx) -> (u64, u8) {
        // let file_name = self.idx_map.get(&idx).unwrap();
        let file_name_opt = self.get_filename_from_idx(idx).await;
        match file_name_opt {
            None => return (0, 0),
            Some(file_name) => {
                let file_name_vec = file_name.split('_').collect::<Vec<&str>>();
                // comment in lib.rs says that we will have exclusive access to our directory
                // so unwrap should be safe here
                (file_name_vec[1].parse::<u64>().unwrap(), file_name_vec[2].parse::<u8>().unwrap())       
            }
        }
    }

    async fn write(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8)) {
        let file_name = format!("{}_{}_{}", idx, sector.1, sector.2);
        // self.idx_map.insert(idx, file_name.clone());
        let data = sector.0.0.as_slice().to_vec();
        let _ = &self.storage.put(&file_name, &data).await.unwrap();
    }
}