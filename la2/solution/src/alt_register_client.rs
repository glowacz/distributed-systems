// use crate::domain::*;
// use crate::sectors_manager_public::build_sectors_manager;
// use crate::transfer_public::DecodingError;
// use crate::{
//     AtomicRegister, ClientRegisterCommand, RegisterClient, RegisterCommand, SectorIdx,
//     SectorsManager, SystemRegisterCommand, build_atomic_register,
// };
// use bincode::config::standard;
// use bincode::{decode_from_slice, encode_to_vec};
// use dashmap::DashMap;
// use hmac::{Hmac, Mac};
// use sha2::Sha256;
// use std::future::Future;
// use std::io::{Error, ErrorKind};
// use std::pin::Pin;
// use std::sync::Arc;
// use std::time::Duration;
// use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
// use tokio::net::{TcpListener, TcpStream};
// use tokio::sync::mpsc;
// use tokio::time::sleep;

// // --- Constants ---
// const TCP_CONNECT_RETRY_DELAY: Duration = Duration::from_millis(500);
// const WORKER_IDLE_TIMEOUT: Duration = Duration::from_secs(10);
// const CHANNEL_CAPACITY: usize = 1024;

// // --- Type Definitions ---
// type HmacSha256 = Hmac<Sha256>;

// enum WorkerCommand {
//     Client {
//         cmd: ClientRegisterCommand,
//         callback: Box<
//             dyn FnOnce(ClientCommandResponse) -> Pin<Box<dyn Future<Output = ()> + Send>>
//                 + Send
//                 + Sync,
//         >,
//     },
//     System(SystemRegisterCommand),
// }

// // --- Helper: Serialization for Client Responses ---
// // We need this because transfer_public only provides serialization for RegisterCommand.
// async fn serialize_response(
//     resp: &ClientCommandResponse,
//     writer: &mut (dyn AsyncWrite + Send + Unpin),
//     hmac_key: &[u8],
// ) -> std::io::Result<()> {
//     let config = standard().with_big_endian().with_fixed_int_encoding();
//     let payload = encode_to_vec(resp, config)
//         .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;

//     let mut mac = HmacSha256::new_from_slice(hmac_key)
//         .map_err(|_| Error::new(ErrorKind::InvalidInput, "Invalid HMAC key"))?;
//     mac.update(&payload);
//     let tag = mac.finalize().into_bytes();

//     let len = (payload.len() + tag.len()) as u64;
//     writer.write_all(&len.to_be_bytes()).await?;
//     writer.write_all(&payload).await?;
//     writer.write_all(&tag).await?;
//     Ok(())
// }

// // --- Helper: Generic Message Reader ---
// // Reads frame (Len + Payload + HMAC) and attempts verification with provided keys.
// async fn read_and_verify_message(
//     reader: &mut (dyn AsyncRead + Send + Unpin),
//     system_key: &[u8; 64],
//     client_key: &[u8; 32],
// ) -> Result<(RegisterCommand, bool), DecodingError> {
//     let mut len_buf = [0u8; 8];
//     reader.read_exact(&mut len_buf).await.map_err(DecodingError::IoError)?;
//     let msg_len = u64::from_be_bytes(len_buf) as usize;

//     if msg_len < 32 {
//         return Err(DecodingError::InvalidMessageSize);
//     }

//     let mut buf = vec![0u8; msg_len];
//     reader.read_exact(&mut buf).await.map_err(DecodingError::IoError)?;

//     let (payload, received_hmac) = buf.split_at(msg_len - 32);

//     // Try System Key
//     let mut sys_mac = HmacSha256::new_from_slice(system_key).unwrap();
//     sys_mac.update(payload);
//     if sys_mac.verify_slice(received_hmac).is_ok() {
//         // Decode
//         let config = standard().with_big_endian().with_fixed_int_encoding();
//         let (msg, _): (RegisterCommand, usize) =
//             decode_from_slice(payload, config).map_err(DecodingError::BincodeError)?;
//         return Ok((msg, true)); // True = System
//     }

//     // Try Client Key
//     let mut cli_mac = HmacSha256::new_from_slice(client_key).unwrap();
//     cli_mac.update(payload);
//     if cli_mac.verify_slice(received_hmac).is_ok() {
//         let config = standard().with_big_endian().with_fixed_int_encoding();
//         let (msg, _): (RegisterCommand, usize) =
//             decode_from_slice(payload, config).map_err(DecodingError::BincodeError)?;
//         return Ok((msg, false)); // False = Client
//     }

//     // Auth failed
//     Err(DecodingError::IoError(Error::new(ErrorKind::PermissionDenied, "HMAC verification failed")))
// }

// // --- Register Client Implementation ---
// struct MyRegisterClient {
//     self_rank: u8,
//     // Maps Target Rank -> Sender
//     senders: DashMap<u8, mpsc::Sender<SystemRegisterCommand>>,
//     // Loopback to self
//     self_tx: mpsc::Sender<WorkerCommand>,
// }

// #[async_trait::async_trait]
// impl RegisterClient for MyRegisterClient {
//     async fn send(&self, msg: crate::register_client_public::Send) {
//         if msg.target == self.self_rank {
//             let _ = self.self_tx.send(WorkerCommand::System((*msg.cmd).clone())).await;
//             return;
//         }

//         if let Some(tx) = self.senders.get(&msg.target) {
//             // We ignore send errors; StubbornLink implies we try our best, 
//             // but if the channel is full or closed (system shutdown), we drop.
//             // The background task handles the stubbornness of the TCP connection.
//             let _ = tx.send((*msg.cmd).clone()).await;
//         }
//     }

//     async fn broadcast(&self, msg: crate::register_client_public::Broadcast) {
//         for entry in self.senders.iter() {
//              let _ = entry.value().send((*msg.cmd).clone()).await;
//         }
//         // Don't forget self
//         let _ = self.self_tx.send(WorkerCommand::System((*msg.cmd).clone())).await;
//     }
// }

// // --- Stubborn Sender Task ---
// async fn stubborn_sender_task(
//     host: String,
//     port: u16,
//     hmac_key: [u8; 64],
//     mut rx: mpsc::Receiver<SystemRegisterCommand>,
// ) {
//     let addr = format!("{}:{}", host, port);
    
//     // We hold the message here if we fail to send it, so we can retry.
//     let mut pending_msg: Option<SystemRegisterCommand> = None;

//     loop {
//         // 1. Establish Connection
//         let mut stream = match TcpStream::connect(&addr).await {
//             Ok(s) => s,
//             Err(_) => {
//                 sleep(TCP_CONNECT_RETRY_DELAY).await;
//                 continue;
//             }
//         };

//         // 2. Send Loop
//         loop {
//             // Get message: either pending from retry, or new one
//             let cmd = match pending_msg.take() {
//                 Some(m) => m,
//                 None => match rx.recv().await {
//                     Some(m) => m,
//                     None => return, // Channel closed, exit
//                 },
//             };

//             // Serialize
//             // We reuse the public serializer logic but need to wrap it into RegisterCommand::System
//             let reg_cmd = RegisterCommand::System(cmd.clone());
            
//             // We buffer serialization to ensure we don't write partial garbage to socket if serialization fails (unlikely)
//             // But serialize_register_command writes directly to writer. 
//             // If it fails on I/O, we drop connection.
//             match crate::transfer_public::serialize_register_command(&reg_cmd, &mut stream, &hmac_key).await {
//                 Ok(_) => {
//                     // Success, loop back for next message
//                 },
//                 Err(_) => {
//                     // Failure, save message to retry, break inner loop to reconnect
//                     pending_msg = Some(cmd);
//                     break; 
//                 }
//             }
//         }
//     }
// }

// // --- Main Entry Point ---

// pub async fn run_register_process(config: Configuration) {
//     let self_rank = config.public.self_rank;
    
//     // 1. Setup Storage
//     let sectors_manager = build_sectors_manager(config.public.storage_dir.clone()).await;
    
//     // 2. Setup Networking Channels
//     let (self_tx, mut self_rx) = mpsc::channel(CHANNEL_CAPACITY);
//     let peer_senders = DashMap::new();

//     // Spawn stubborn senders for other processes
//     for (idx, (host, port)) in config.public.tcp_locations.iter().enumerate() {
//         let target_rank = (idx + 1) as u8;
//         if target_rank == self_rank {
//             continue;
//         }

//         let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
//         peer_senders.insert(target_rank, tx);
        
//         let h_key = config.hmac_system_key;
//         let h = host.clone();
//         let p = *port;
//         tokio::spawn(async move {
//             stubborn_sender_task(h, p, h_key, rx).await;
//         });
//     }

//     let register_client = Arc::new(MyRegisterClient {
//         self_rank,
//         senders: peer_senders,
//         self_tx: self_tx.clone(),
//     });

//     // 3. Active Registers Management
//     // Map SectorIdx -> Sender to Worker Task
//     let active_registers: Arc<DashMap<SectorIdx, mpsc::Sender<WorkerCommand>>> = Arc::new(DashMap::new());

//     // 4. Setup Self-Loopback Handler
//     // Since "messages sent to self should skip TCP", we handle them here via channel
//     {
//         let active_registers = active_registers.clone();
//         let sectors_manager = sectors_manager.clone();
//         let register_client = register_client.clone();
//         let process_count = config.public.tcp_locations.len() as u8;

//         tokio::spawn(async move {
//             while let Some(cmd) = self_rx.recv().await {
//                 dispatch_command(
//                     cmd,
//                     &active_registers,
//                     self_rank,
//                     &sectors_manager,
//                     &register_client,
//                     process_count
//                 ).await;
//             }
//         });
//     }

//     // 5. Bind TCP Listener
//     let (bind_host, bind_port) = &config.public.tcp_locations[(self_rank - 1) as usize];
//     let listener = TcpListener::bind(format!("{}:{}", bind_host, bind_port))
//         .await
//         .expect("Failed to bind TCP listener");

//     // 6. Accept Loop
//     loop {
//         let (socket, _) = match listener.accept().await {
//             Ok(v) => v,
//             Err(_) => continue,
//         };

//         let config_clone = Configuration {
//             public: PublicConfiguration {
//                 tcp_locations: config.public.tcp_locations.clone(),
//                 storage_dir: config.public.storage_dir.clone(),
//                 self_rank: config.public.self_rank,
//                 n_sectors: config.public.n_sectors,
//             },
//             hmac_system_key: config.hmac_system_key,
//             hmac_client_key: config.hmac_client_key,
//         };
        
//         let regs = active_registers.clone();
//         let sm = sectors_manager.clone();
//         let rc = register_client.clone();
//         let pc = config.public.tcp_locations.len() as u8;

//         tokio::spawn(async move {
//             handle_connection(socket, config_clone, regs, sm, rc, pc).await;
//         });
//     }
// }

// // --- Connection Handler ---
// async fn handle_connection(
//     mut socket: TcpStream,
//     config: Configuration,
//     active_registers: Arc<DashMap<SectorIdx, mpsc::Sender<WorkerCommand>>>,
//     sectors_manager: Arc<dyn SectorsManager>,
//     register_client: Arc<dyn RegisterClient>,
//     process_count: u8,
// ) {
//     let (mut rd, mut wr) = socket.split();
//     // We need a shared writer for callbacks (Client responses)
//     let writer = Arc::new(tokio::sync::Mutex::new(wr));

//     loop {
//         // Read and authenticate
//         let (cmd_enum, is_system) = match read_and_verify_message(&mut rd, &config.hmac_system_key, &config.hmac_client_key).await {
//             Ok(res) => res,
//             Err(_) => break, // Disconnect on error
//         };

//         match cmd_enum {
//             RegisterCommand::System(sys_cmd) => {
//                 if !is_system { break; } // Protocol violation: System cmd signed with Client key (unlikely if strictly typed, but good safety)
                
//                 let worker_cmd = WorkerCommand::System(sys_cmd);
//                 dispatch_command(worker_cmd, &active_registers, config.public.self_rank, &sectors_manager, &register_client, process_count).await;
//             }
//             RegisterCommand::Client(cli_cmd) => {
//                 if is_system { break; } // Protocol violation

//                 let req_id = cli_cmd.header.request_identifier;
//                 let writer_clone = writer.clone();
//                 let client_key = config.hmac_client_key;

//                 let callback = Box::new(move |resp: ClientCommandResponse| {
//                     Box::pin(async move {
//                         let mut locked_writer = writer_clone.lock().await;
//                         // Ignore errors on write; if client disconnects, we can't do anything.
//                         let _ = serialize_response(&resp, &mut *locked_writer, &client_key).await;
//                     }) as Pin<Box<dyn Future<Output = ()> + Send>>
//                 });

//                 let worker_cmd = WorkerCommand::Client {
//                     cmd: cli_cmd,
//                     callback,
//                 };

//                 dispatch_command(worker_cmd, &active_registers, config.public.self_rank, &sectors_manager, &register_client, process_count).await;
//             }
//         }
//     }
// }

// // --- Dispatcher Logic ---
// async fn dispatch_command(
//     cmd: WorkerCommand,
//     active_registers: &Arc<DashMap<SectorIdx, mpsc::Sender<WorkerCommand>>>,
//     self_rank: u8,
//     sectors_manager: &Arc<dyn SectorsManager>,
//     register_client: &Arc<dyn RegisterClient>,
//     process_count: u8,
// ) {
//     let sector_idx = match &cmd {
//         WorkerCommand::Client { cmd, .. } => cmd.header.sector_idx,
//         WorkerCommand::System(cmd) => cmd.header.sector_idx,
//     };

//     // Retry loop for sending to worker (in case of race where worker is removing itself)
//     loop {
//         // Get existing sender or create new worker
//         let sender = active_registers.entry(sector_idx).or_insert_with(|| {
//             let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
            
//             // Spawn Worker
//             let sm = sectors_manager.clone();
//             let rc = register_client.clone();
//             let regs = active_registers.clone();
            
//             tokio::spawn(async move {
//                 register_worker(self_rank, sector_idx, rc, sm, process_count, rx, regs).await;
//             });
            
//             tx
//         }).clone();

//         match sender.send(cmd).await {
//             Ok(_) => break, // Sent successfully
//             Err(e) => {
//                 // Channel closed. The worker died/timed out.
//                 // The worker should have removed itself, but we might have a stale handle if we were holding the ref.
//                 // Or we got the handle just as it closed.
//                 // Remove from map and retry loop to create new one.
//                 active_registers.remove(&sector_idx);
//                 cmd = e.0; // Retrieve the command to retry
//             }
//         }
//     }
// }

// // --- Atomic Register Worker ---
// async fn register_worker(
//     self_rank: u8,
//     sector_idx: SectorIdx,
//     register_client: Arc<dyn RegisterClient>,
//     sectors_manager: Arc<dyn SectorsManager>,
//     process_count: u8,
//     mut rx: mpsc::Receiver<WorkerCommand>,
//     active_registers: Arc<DashMap<SectorIdx, mpsc::Sender<WorkerCommand>>>,
// ) {
//     // 1. Build Register
//     let mut register = build_atomic_register(
//         self_rank,
//         sector_idx,
//         register_client,
//         sectors_manager,
//         process_count,
//     ).await;

//     // 2. Event Loop
//     loop {
//         // Wait for command with timeout
//         let msg = match tokio::time::timeout(WORKER_IDLE_TIMEOUT, rx.recv()).await {
//             Ok(Some(m)) => m,
//             Ok(None) => break, // Channel closed
//             Err(_) => {
//                 // Timeout: Cleanup
//                 // Critical Section: Remove self from map to prevent new commands routed to dying worker
//                 // We drop the receiver only after removing.
//                 // But we are holding `rx` here.
//                 // Strategy: Remove from map. Then check if new messages came in race?
//                 // Actually, if we remove from map, `Dispatcher` creates new channel.
//                 // So safe to exit.
//                 if active_registers.remove(&sector_idx).is_some() {
//                     break;
//                 } else {
//                     // Should not happen unless someone else removed us
//                     break;
//                 }
//             }
//         };

//         match msg {
//             WorkerCommand::Client { cmd, callback } => {
//                 register.client_command(cmd, callback).await;
//             }
//             WorkerCommand::System(cmd) => {
//                 register.system_command(cmd).await;
//             }
//         }
//     }
// }