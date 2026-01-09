use crate::sectors_manager_public::SectorsManager;
use crate::{SECTOR_SIZE, SectorIdx, SectorVec};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;
use crate::stable_storage::{MyStableStorage, StableStorage, build_my_stable_storage};
use log::{debug};
use serde_big_array::Array;
use tokio::fs::read_dir;
use tokio::sync::{RwLock};
use tokio::time::sleep;
pub struct MySectorsManager {
    path: PathBuf,
    storage: RwLock<MyStableStorage>,
    idx_map: RwLock<HashMap<SectorIdx, (String, u64, u8)>>,
}

impl MySectorsManager {
    pub async fn new(path: PathBuf) -> Self {
        Self { 
            path: path.clone(),
            storage: RwLock::new(build_my_stable_storage(path.clone()).await),
            idx_map: RwLock::new(HashMap::new())
        }
    }

    pub async fn startup(&mut self) {
        let mut dir_entries = read_dir(&self.path).await.unwrap();

        while let Some(dir_entry) = dir_entries.next_entry().await.unwrap() {
            let file_path = dir_entry.path();

            if file_path.is_file() {
                if let Some(file_name) = file_path.file_name() {
                    if let Some(name_str) = file_name.to_str() {
                        let file_name_vec = name_str.split('_').collect::<Vec<&str>>();
                        let (ts, wr) = (file_name_vec[1].parse::<u64>().unwrap(), file_name_vec[2].parse::<u8>().unwrap()); 
                        
                        let idx_opt  = name_str
                            .split_once('_')
                            .map(|(prefix, _)| prefix)
                            .and_then(|idx_str| idx_str.parse::<u64>().ok());

                        if let Some(idx) = idx_opt {
                            if let Some((old_name, old_ts, old_wr)) = self.idx_map.read().await.get(&idx) {
                                if (ts, wr) > (*old_ts, *old_wr) {
                                    self.storage.write().await.remove(&old_name).await;
                                    self.idx_map.write().await.insert(idx, (name_str.to_string(), ts, wr));
                                }
                            }
                            else {
                                self.idx_map.write().await.insert(idx, (name_str.to_string(), ts, wr));
                            }
                        }
                    }
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl SectorsManager for MySectorsManager {
    async fn read_data(&self, idx: SectorIdx) -> SectorVec {
        let file_name = match self.idx_map.read().await.get(&idx).cloned() {
            None => {
                debug!("There was no data for sector {} (it will be written for the first time)", idx);
                return SectorVec(Box::new(Array([0u8; SECTOR_SIZE])));
            },
            Some((name, _ts, _wr)) => name,
        };
        let mut res = self.storage.read().await.get(file_name.as_str()).await;
        while let None = res {
            sleep(Duration::from_millis(100)).await;
            res = self.storage.read().await.get(file_name.as_str()).await;
        }
        let data_vec = res.unwrap();
        let data_array = data_vec.try_into().unwrap();
        let wrapped_array = Array(data_array);
        SectorVec(Box::new(wrapped_array))
    }

    async fn read_metadata(&self, idx: SectorIdx) -> (u64, u8) {
        let file_name_opt = self.idx_map.read().await.get(&idx).cloned();
        match file_name_opt {
            None => return (0, 0),
            Some((file_name, _ts, _wr)) => {
                let file_name_vec = file_name.split('_').collect::<Vec<&str>>();
                // comment in lib.rs says that we will have exclusive access to our directory
                // so unwrap should be safe here
                (file_name_vec[1].parse::<u64>().unwrap(), file_name_vec[2].parse::<u8>().unwrap())       
            }
        }
    }

    async fn write(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8)) {
        let old_file_name_opt = self.idx_map.read().await.get(&idx).cloned();

        // println!("Write sector {idx};   Was there sth there eariler? {}", old_file_name_opt != None);
        
        let file_name = format!("{}_{}_{}", idx, sector.1, sector.2);
        // self.idx_map.insert(idx, file_name.clone());
        let data = sector.0.0.as_slice().to_vec();
        
        // println!("Writing sector {idx} under file name {file_name}...");
        let mut res = self.storage.read().await.put(&file_name, &data).await;
        while let Err(_e) = res {
            sleep(Duration::from_millis(100)).await;
            res = self.storage.read().await.put(&file_name, &data).await;
        }

        self.idx_map.write().await.insert(idx, (file_name, sector.1, sector.2));
        // println!("Write sector {idx}, new file written");

        if let Some((old_file_name, _ts, _wr)) = old_file_name_opt {
            let mut res =  self.storage.write().await.remove(&old_file_name).await;
            while false == res {
                sleep(Duration::from_millis(100)).await;
                res = self.storage.write().await.remove(&old_file_name).await;
            }
            // println!("Write sector {idx}, old file removed");
        }
    }
}