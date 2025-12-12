use crate::{AtomicRegister, ClientCommandResponse, ClientRegisterCommand, RegisterClient, SectorVec, SectorsManager, SystemRegisterCommand, atomic_register_public};

struct AtomicRegisterData {
    self_rank: u8,
    wr: u8,
    ts: u64,
    val: SectorVec, // keeping this in memory should allow to sometimes avoid the disk (caching)
    // hopefully managing consistency will not be too hard
    // since we will do recovery after a crash
    // and from this point on 
    readlist: Vec<(u64, u8, SectorVec)>, // (ts, ws, val)
    ackcnt: u8,
    reading: bool,
    writing: bool,
    writeval: Option<SectorVec>,
    readval: Option<SectorVec>,
    write_phase: bool
}
pub struct MyAtomicRegister {
    process_id: u8,
    sector_idx: u64,
    client: Box<dyn RegisterClient + Send + Sync>,
    sectors_manager: Box<dyn SectorsManager + Send + Sync>,
    n: u8,
    data: AtomicRegisterData,
}

impl MyAtomicRegister {
    pub async fn new(
        process_id: u8,
        sector_idx: u64,
        client: Box<dyn RegisterClient + Send + Sync>,
        sectors_manager: Box<dyn SectorsManager + Send + Sync>,
        n: u8,
    ) -> Self {
        let (ts, wr) = sectors_manager.read_metadata(sector_idx).await;
        let val = sectors_manager.read_data(sector_idx).await;
        Self {
            process_id,
            sector_idx,
            client,
            sectors_manager,
            n,
            data: AtomicRegisterData {
                self_rank: process_id,
                wr,
                ts,
                val,
                readlist: Vec::new(),
                ackcnt: 0,
                reading: false,
                writing: false,
                writeval: None,
                readval: None,
                write_phase: false
            }
        }
    }
}

#[async_trait::async_trait]
impl AtomicRegister for MyAtomicRegister {
    async fn client_command(
        &mut self,
        _cmd: ClientRegisterCommand,
        _success_callback: Box<
            dyn FnOnce(ClientCommandResponse) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
                + Send
                + Sync,
        >,
    ) {
        unimplemented!()
    }

    async fn system_command(&mut self, _cmd: SystemRegisterCommand) {
        unimplemented!()
    }
}