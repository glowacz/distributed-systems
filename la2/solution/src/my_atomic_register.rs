use std::{collections::HashMap, sync::Arc};

use log::{trace};
use serde_big_array::Array;
use uuid::Uuid;

use crate::{AtomicRegister, Broadcast, ClientCommandResponse, ClientRegisterCommand, ClientRegisterCommandContent, OperationReturn, RegisterClient, SECTOR_SIZE, SectorVec, SectorsManager, SystemCommandHeader, SystemRegisterCommand, SystemRegisterCommandContent};

struct AtomicRegisterData {
    self_rank: u8,
    wr: u8,
    ts: u64,
    val: SectorVec, // keeping this in memory should allow to sometimes avoid the disk (caching)
    // hopefully managing consistency will not be too hard
    // since we will do recovery after a crash
    // and from this point on 
    readlist: HashMap<u8, (u64, u8, SectorVec)>, // (ts, ws, val)
    acklist: HashMap<u8, bool>,
    reading: bool,
    writing: bool,
    writeval: SectorVec,
    readval: SectorVec,
    write_phase: bool,
    op_id: Uuid,
}
pub struct MyAtomicRegister {
    // process_id: u8,
    sector_idx: u64,
    client: Arc<dyn RegisterClient>,
    sectors_manager: Arc<dyn SectorsManager>,
    n: u8,
    callbacks: HashMap<Uuid, Box<
    dyn FnOnce(ClientCommandResponse) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
        + Send
        + Sync>>,
    request_ids: HashMap<Uuid, u64>,
    data: AtomicRegisterData,
}

impl MyAtomicRegister {
    pub async fn new(
        self_ident: u8,
        sector_idx: u64,
        client: Arc<dyn RegisterClient>,
        sectors_manager: Arc<dyn SectorsManager>,
        n: u8,
    ) -> Self {
        let (ts, wr) = sectors_manager.read_metadata(sector_idx).await;
        let val = sectors_manager.read_data(sector_idx).await;
        // no need to store(wr, ts, val) here, they would be empty
        // so we can just omit the disk in this case

        Self {
            // process_id: self_ident,
            sector_idx,
            client,
            sectors_manager,
            n,
            callbacks: HashMap::new(),
            request_ids: HashMap::new(),
            data: AtomicRegisterData {
                self_rank: self_ident,
                wr,
                ts,
                val,
                readlist: HashMap::new(),
                acklist: HashMap::new(),
                reading: false,
                writing: false,
                writeval: SectorVec(Box::new(serde_big_array::Array([0u8; SECTOR_SIZE]))),
                readval: SectorVec(Box::new(serde_big_array::Array([0u8; SECTOR_SIZE]))),
                write_phase: false,
                op_id: Uuid::new_v4(),
            }
        }
    }

    fn highest(&self) -> (u64, u8, SectorVec) {
        let mut max_ts = 0;
        let mut max_wr = 0;
        let mut readval: SectorVec = SectorVec(Box::new(Array([0u8; SECTOR_SIZE])));
        for (_proc_id, (ts, wr, val)) in self.data.readlist.clone() {
            if ts > max_ts || (ts == max_ts && wr > max_wr) {
                max_ts = ts;
                max_wr = wr;
                // val_opt = Some(val.clone());
                readval = val.clone();
            }
        }
        // (max_ts, max_wr, val_opt.unwrap())
        (max_ts, max_wr, readval)
    }

    async fn store(&mut self, ts: u64, wr: u8, val: &SectorVec) {
        self.data.wr = wr;
        self.data.ts = ts;
        self.data.val = val.clone();
        self.sectors_manager.write(self.sector_idx, &(val.clone(), ts, wr)).await;
    }
}

#[async_trait::async_trait]
impl AtomicRegister for MyAtomicRegister {
    async fn client_command(
        &mut self,
        cmd: ClientRegisterCommand,
        success_callback: Box<
            dyn FnOnce(ClientCommandResponse) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
                + Send
                + Sync,
        >,
    ) {
        trace!("[AR CLASS {}]: received CLIENT command {}", 
          self.data.self_rank, cmd);

        self.data.op_id = Uuid::new_v4();
        self.data.readlist.clear();
        self.data.acklist.clear();

        self.callbacks.insert(self.data.op_id, success_callback);
        // or maybe there should be just one callback possible at a time?
        self.request_ids.insert(self.data.op_id, cmd.header.request_identifier);

        match cmd.content {
            ClientRegisterCommandContent::Read => {
                self.data.reading = true;
            },
            ClientRegisterCommandContent::Write { data } => {
                self.data.writeval = data;
                self.data.writing = true;
            },
        }

        let command = SystemRegisterCommand { 
            header: SystemCommandHeader { 
                process_identifier: self.data.self_rank, 
                msg_ident: self.data.op_id,
                sector_idx: self.sector_idx
            },
            content: SystemRegisterCommandContent::ReadProc
        };

        self.client.broadcast(
            Broadcast { cmd: Arc::new(command) }
        ).await;
    }

    async fn system_command(&mut self, cmd: SystemRegisterCommand) {
        trace!("[AR CLASS {}]: received SYSTEM command {}", 
          self.data.self_rank, cmd);

        match cmd.content {
            SystemRegisterCommandContent::ReadProc => {
                let command = SystemRegisterCommand {
                    header: SystemCommandHeader {
                        process_identifier: self.data.self_rank,
                        msg_ident: cmd.header.msg_ident,
                        sector_idx: cmd.header.sector_idx
                    },
                    content: SystemRegisterCommandContent::Value {
                        timestamp: self.data.ts,
                        write_rank: self.data.wr,
                        sector_data: self.data.val.clone()
                    }
                };
                // trace!("[AR CLASS {}]: BEFORE sending reply to ReadProc to {}", self.data.self_rank, cmd.header.process_identifier);
                self.client.send( 
                    crate::Send {
                        cmd: Arc::new(command),
                        target: cmd.header.process_identifier
                }).await;
                trace!("[AR CLASS {}]: AFTER sending reply to ReadProc to {}", self.data.self_rank, cmd.header.process_identifier);

            },
            SystemRegisterCommandContent::Value {
                timestamp, 
                write_rank, 
                sector_data 
            } => {
                trace!("[AR CLASS {}]: got Value from {}", self.data.self_rank, cmd.header.process_identifier);
                // if cmd.header.msg_ident != self.data.op_id || cmd.header.process_identifier == self.data.self_rank {
                if cmd.header.msg_ident != self.data.op_id {
                    return;
                }
                self.data.readlist.insert(cmd.header.process_identifier, (timestamp, write_rank, sector_data));
                trace!("[AR CLASS {}]: inserted {} into readlist with (ts: {}, wr: {})", self.data.self_rank, 
                    cmd.header.process_identifier, timestamp, write_rank);

                if self.data.readlist.len() as u8 > self.n / 2 && (self.data.reading || self.data.writing) {
                     // >= because (ts, wr, val) from self is not in readlist
                    // self.data.readlist.insert(self.data.self_rank, (self.data.ts, self.data.wr, self.data.val.clone()));
                    let (maxts, rr, readval) = self.highest();
                    self.data.readval = readval.clone();
                    self.data.readlist.clear();
                    self.data.acklist.clear();
                    self.data.write_phase = true;

                    let header = SystemCommandHeader {
                        process_identifier: self.data.self_rank,
                        msg_ident: cmd.header.msg_ident,
                        sector_idx: cmd.header.sector_idx
                    };

                    if self.data.reading {
                        let command = SystemRegisterCommand {
                            header,
                            content: SystemRegisterCommandContent::WriteProc {
                                timestamp: maxts,
                                write_rank: rr,
                                data_to_write: readval
                            }
                        };
                        self.client.broadcast(
                            Broadcast { cmd: Arc::new(command) }
                        ).await;
                    } else {
                        let (ts, wr, val) = (maxts + 1, self.data.self_rank, self.data.writeval.clone());
                        self.store(ts, wr, &val).await;
                        // self.data.ts = ts;
                        // self.data.wr = wr;
                        // self.data.val = val;

                        let command = SystemRegisterCommand {
                            header,
                            content: SystemRegisterCommandContent::WriteProc {
                                timestamp: maxts + 1,
                                write_rank: self.data.self_rank,
                                data_to_write: self.data.writeval.clone()
                            }
                        };
                        self.client.broadcast(
                            Broadcast { cmd: Arc::new(command) }
                        ).await;
                    }
                
                }
            },
            SystemRegisterCommandContent::WriteProc { 
                timestamp, 
                write_rank, 
                data_to_write 
            } => {
                trace!("[AR CLASS {}]: got WriteProc from {}", self.data.self_rank, cmd.header.process_identifier);

                if (self.data.ts, self.data.wr) < (timestamp, write_rank) {
                    let (ts, wr, val) = (timestamp, write_rank, data_to_write);
                    self.store(ts, wr, &val).await;
                }

                let command = SystemRegisterCommand {
                    header: SystemCommandHeader {
                        process_identifier: self.data.self_rank,
                        msg_ident: cmd.header.msg_ident,
                        sector_idx: cmd.header.sector_idx
                    },
                    content: SystemRegisterCommandContent::Ack
                };
                self.client.send( 
                    crate::Send {
                        cmd: Arc::new(command),
                        target: cmd.header.process_identifier
                }).await;
            },
            SystemRegisterCommandContent::Ack => {
                trace!("[AR CLASS {}]: got Ack from {}", self.data.self_rank, cmd.header.process_identifier);
                if cmd.header.msg_ident != self.data.op_id 
                    // || cmd.header.process_identifier == self.data.self_rank
                    || !self.data.write_phase {
                        return;
                }
                self.data.acklist.insert(cmd.header.process_identifier, true);

                if self.data.acklist.len() as u8 > self.n / 2 { // >= because self ack is not in acklist
                    self.data.acklist.clear();
                    self.data.write_phase = false;

                    if self.data.reading {
                        // let req_id = self.request_ids.get(cmd.header.msg_ident.clone().as_ref()).unwrap_or(&0);
                        // println!("[READ] operation {req_id} completed (on {})", self.data.self_rank);

                        self.data.reading = false;
                        let response = ClientCommandResponse {
                            status: crate::StatusCode::Ok,
                            request_identifier: self.request_ids.remove(&cmd.header.msg_ident).unwrap_or_default(),
                            op_return: OperationReturn::Read {
                                read_data: self.data.readval.clone()
                            },
                        };
                        let callback = self.callbacks.remove(&cmd.header.msg_ident).unwrap();
                        callback(response).await;
                    }
                    else {
                        // let req_id = self.request_ids.get(cmd.header.msg_ident.clone().as_ref()).unwrap_or(&0);
                        // println!("[WRITE] operation sector {}, id {req_id} completed (on {})", self.sector_idx, self.data.self_rank);

                        self.data.writing = false;
                        let response = ClientCommandResponse {
                            status: crate::StatusCode::Ok,
                            request_identifier: self.request_ids.remove(&cmd.header.msg_ident).unwrap_or_default(),
                            op_return: OperationReturn::Write,
                        };
                        let callback = self.callbacks.remove(&cmd.header.msg_ident).unwrap();
                        callback(response).await;
                        
                    }
                }
            },
        }
    }
}