use std::collections::{HashMap, VecDeque};
use std::{path::PathBuf, sync::Arc};

use log::{error, info, trace,};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::select;
use tokio::sync::mpsc::Sender;
use tokio::sync::{Mutex, RwLock};

use crate::{ClientCommandResponse, build_sectors_manager, sectors_manager_public, serialize_client_response};
use crate::{Broadcast, Configuration, RegisterClient, RegisterCommand, SystemRegisterCommand, my_atomic_register::MyAtomicRegister, register_client_public, serialize_register_command};
use crate::atomic_register_public::AtomicRegister;

struct SharedState {
    hmac_system_key: [u8; 64],
    hmac_client_key: [u8; 32],
    tcp_locations: Vec<(String, u16)>,
    have_connection: Vec<Mutex<bool>>,
    tcp_writers: Arc<RwLock<HashMap<(String, u16), Arc<Mutex<OwnedWriteHalf>>>>>,
    main_cmd_senders: Arc<RwLock<HashMap<u64, Sender<(RegisterCommand, Option<(String, u16)>)>>>>,
    client_cmd_senders: Arc<RwLock<HashMap<u64, Sender<(RegisterCommand, Option<(String, u16)>)>>>>,
    sectors_manager: Arc<dyn sectors_manager_public::SectorsManager>
}

#[derive(Clone)]
pub struct MyRegisterClient {
    self_rank: u8,
    _n_sectors: u64,
    self_addr: (String, u16),

    state: Arc<SharedState>
}

// TODO: change the level of every log to trace! (maybe except new connection)

impl MyRegisterClient {
    pub async fn new(conf: Configuration) -> Self {
        let self_addr = conf.public.tcp_locations[(conf.public.self_rank - 1) as usize].clone();
        let sectors_manager = build_sectors_manager(conf.public.storage_dir).await;
        let have_connection: Vec<Mutex<bool>> = (0..conf.public.tcp_locations.len())
            .map(|_| Mutex::new(false))
            .collect();

        let state = Arc::new(SharedState {
            hmac_system_key: conf.hmac_system_key,
            hmac_client_key: conf.hmac_client_key,
            tcp_locations: conf.public.tcp_locations,
            have_connection,
            tcp_writers: Arc::new(RwLock::new(HashMap::new())),
            main_cmd_senders: Arc::new(RwLock::new(HashMap::new())),
            client_cmd_senders: Arc::new(RwLock::new(HashMap::new())),
            sectors_manager
        });

        Self {
            self_rank: conf.public.self_rank,
            _n_sectors: conf.public.n_sectors,
            self_addr,
            state
        }
    }

    pub async fn reply_to_client(&self, cmd: ClientCommandResponse, ip: String, port: u16) -> std::io::Result<()> {
        // TODO should it be stubborn as well ???
        let hmac_key = self.state.hmac_client_key;
        
        let single_mut = {
            let read_guard = self.state.tcp_writers.read().await;
            match read_guard.get(&(ip, port)) {
                Some(arc_single_mut) => arc_single_mut.clone(),
                None => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::NotFound, 
                        "Client not found"
                    ));
                }
            }
            // guard for the whole map dropped here, won't block further
        };

        let self_rank = self.self_rank;
        let _ = tokio::spawn( async move {
            info!("[MyRegisterClient {}] Before replying to client", self_rank);
            let mut tcp_writer = single_mut.lock().await;
            let _ = serialize_client_response(&cmd, &mut *tcp_writer, &hmac_key).await;
            info!("\n\n[MyRegisterClient {}] After replying to client\n\n", self_rank);
        });

        return Ok(());
    }

    async fn send_msg(&self, cmd: Arc<SystemRegisterCommand>, target: u8) {
        // TODO make it stubborn
        let (ip, port) = self.state.tcp_locations[(target - 1) as usize].clone();
        let hmac_key = self.state.hmac_system_key;
        let tcp_writers = self.state.tcp_writers.clone();
        let self_arc = Arc::new(self.clone());
        let sectors_manager = self.state.sectors_manager.clone();

        let self_rank = self.self_rank;

        let _ = tokio::spawn( async move {
            let mut have_conn_guard = self_arc.state.have_connection[(target - 1) as usize].lock().await;
            let single_mut = {
                match *have_conn_guard {
                    false => {
                        *have_conn_guard = true;

                        // TODO: change it to stubborn
                        let tcp_stream = TcpStream::connect(format!("{}:{}", ip.clone(), port)).await.unwrap();
                        let (rd, wr) = tcp_stream.into_split();
                        info!("[{}]: connected to node {} ({}{})", self_rank, target, ip.clone(), port);

                        let protected_writer = Arc::new(Mutex::new(wr));

                        let mut map_guard = tcp_writers.write().await;
                        map_guard.insert((ip.clone(), port), protected_writer.clone());

                        start_tcp_reader_task(self_arc.clone(), sectors_manager.clone(), protected_writer.clone(), rd, ip.clone(), port).await;

                        protected_writer
                    }
                    true => {
                        let mutex_opt = tcp_writers.read().await.get(&(ip.clone(), port)).cloned();
                        mutex_opt.unwrap()
                        // single_mut.clone()
                    } 
                }
            };

            let cmd = RegisterCommand::System((*cmd).clone());
            let mut tcp_writer = single_mut.lock().await;
            let _ = serialize_register_command(&cmd, &mut *tcp_writer, &hmac_key).await;
        });
    }
}

#[async_trait::async_trait]
impl RegisterClient for MyRegisterClient {
    async fn send(&self, msg: register_client_public::Send) {
        let _ = self.send_msg(msg.cmd, msg.target).await;
        trace!("[{}]: sending to target {}", self.self_rank, msg.target);
    }

    async fn broadcast(&self, msg: Broadcast) {
        // TODO skip TCP when sending to self
        // and maybe there just is a bettter way to send one/many messages
        for target in 1..=self.state.tcp_locations.len() as u8 {
            // await the creation of the task so that it actually happens,
            // (the finishing of the task is not awaited)
            let _ = self.send_msg(msg.cmd.clone(), target).await;
            trace!("[{}]: broadcasting to target {}", self.self_rank, target);
        }
    }
}

async fn add_peer(state: Arc<SharedState>, ip: String, port: u16, writer: Arc<Mutex<OwnedWriteHalf>>) {
    let key = (ip, port);

    // TODO: have separate tcp_writers for nodes of system and clients
    // this should increase performance with mutexes
    // and maybe will solve a bug
    // also, will make code simpler, no need to look up tcp_locations

    let mut map_guard = state.tcp_writers.write().await;
    map_guard.insert(key, writer);
}

async fn start_ar_worker(client: Arc<MyRegisterClient>, sectors_manager: Arc<dyn sectors_manager_public::SectorsManager>, sector_idx: u64,
            mut rx: tokio::sync::mpsc::Receiver<(RegisterCommand, Option<(String, u16)>)>,
            tx: tokio::sync::mpsc::Sender<(RegisterCommand, Option<(String, u16)>)>,
            mut client_rx: tokio::sync::mpsc::Receiver<(RegisterCommand, Option<(String, u16)>)>
    ) {
    // let client = Arc::new(self);
    // let sectors_manager = sectors_manager.clone();

    let _ = tokio::spawn( async move {
        trace!("[AR worker {}]: Starting for sector {}", client.self_rank, sector_idx);
        // let client = client.clone();
        // let sectors_manager = sectors_manager.clone();
        
        let mut processing_client = false;
        let (tx_client_done, mut rx_client_done) = tokio::sync::mpsc::channel(1000);
        let mut client_wait_queue = VecDeque::new();

        // if there is sth on client queue, start by moving it to main queue
        let res = client_rx.try_recv();
        if let Ok((recv_cmd, writer_opt)) = res {
            trace!("[{}]: Received command on client queue, piping to main",
            client.self_rank);
            let _ = tx.send((recv_cmd, writer_opt)).await;
        }

        let n = client.state.tcp_locations.len() as u8;
        let mut register = MyAtomicRegister::new(
            client.self_rank,
            sector_idx,
            client.clone(),
            // client,
            // sectors_manager.clone(),
            sectors_manager,
            n
        ).await;

        trace!("[AR worker {}]: Starting LOOP for sector {}", client.self_rank, sector_idx);

        // while let Some((recv_cmd, writer_opt)) = rx.recv().await {
        loop {
            trace!("[AR worker {}, {}]: loop", client.self_rank, sector_idx);
            // check if we're processing client request and if there is sth on client queue
            // if NO and YES, move the request from client to main queue

            if !processing_client && !client_wait_queue.is_empty() {
                trace!("[AR worker {}, {}]: getting client request from client wait queue to main queue", client.self_rank, sector_idx);
                let (recv_cmd, writer_opt) = client_wait_queue.pop_front().unwrap();
                let _ = tx.send((recv_cmd, writer_opt)).await;
            }

            select! {
                Some((recv_cmd, ip_port_opt)) = rx.recv() => {
                    trace!("[AR worker {}]: got {} for sector {}", client.self_rank, recv_cmd.clone(), sector_idx);
                    match recv_cmd {
                        RegisterCommand::Client(cmd) => {
                            info!("[AR worker {}, {}]: starting to process client request", client.self_rank, sector_idx);
                            processing_client = true;
                            // let tcp_writer_clone = tcp_writer.clone();
                            let (ip, port) = match ip_port_opt {
                                Some(ip_port) => ip_port,
                                None => {
                                    // error!("Client command received without TCP writer!");
                                    continue;
                                }
                            };
                            let client_clone = client.clone();                    
                            let tx_client_done_clone = tx_client_done.clone();
        
                            let success_callback: Box<
                                dyn FnOnce(ClientCommandResponse) -> Pin<Box<dyn Future<Output = ()> + Send>>
                                    + Send
                                    + Sync,
                            > = Box::new(move |response: ClientCommandResponse| {
                                Box::pin(async move {
                                    trace!("Callback will send response: {:?} to client", response);
                                    let _ = client_clone.reply_to_client(response, ip, port).await;
                                    trace!("Callback SENT response to client");
                                    let _ = tx_client_done_clone.send(()).await;
                                    trace!("Callback sent information that we're done with this client onto the queue");
                                })
                            });
                            register.client_command(cmd, success_callback).await;
                            trace!("[AR worker {}]: fully delivered CLIENT command to AR struct for sector {}", client.self_rank, sector_idx);
                        },
                        RegisterCommand::System(cmd) => {
                            register.system_command(cmd).await;
                            trace!("[{}]: sent SYSTEM command for sector {}", client.self_rank, sector_idx);
                        },
                    }
                }
                _ = rx_client_done.recv() => {
                    info!("[AR worker {}, {}]: finished processing whole client request", client.self_rank, sector_idx);
                    processing_client = false;
                }
                Some((recv_cmd, writer_opt)) = client_rx.recv() => {
                    trace!("[AR worker {}, {}]: received CLIENT request on client queue", client.self_rank, sector_idx);
                    if !processing_client {
                        let _ = tx.send((recv_cmd, writer_opt)).await;
                        trace!("[AR worker {}, {}]: putting CLIENT request on the MAIN queue", client.self_rank, sector_idx);

                    }
                    else {
                        client_wait_queue.push_back((recv_cmd, writer_opt));
                        trace!("[AR worker {}, {}]: putting CLIENT request on the WAIT queue", client.self_rank, sector_idx);

                    }
                }
            }
        }
    });
}

async fn start_tcp_reader_task(client: Arc<MyRegisterClient>, sectors_manager: Arc<dyn sectors_manager_public::SectorsManager>, 
    wr: Arc<Mutex<OwnedWriteHalf>>, mut rd: OwnedReadHalf, 
    client_ip: String, client_port: u16) { // reading from a single client/node
    let _ = tokio::spawn( async move {
        let self_addr = client.self_addr.clone();
        let state = client.state.clone();

        for i in 0.. {
            trace!("[{}:{}]: ready to read and deserialize next message", self_addr.0, self_addr.1);
            let deserialize_res = crate::deserialize_register_command(
                &mut rd, &state.hmac_system_key, &state.hmac_client_key).await;
            // trace!("[{}:{}]: Deserialized message, result {:?}", self_addr.0, self_addr.1, deserialize_res);
            trace!("[{}:{}]: Deserialized message", self_addr.0, self_addr.1);
            match deserialize_res {
                Ok((recv_cmd, hmac_valid)) => {
                    match recv_cmd {
                        RegisterCommand::System(cmd) => {
                            if i == 0 {
                                let (node_ip, node_port) = client.state.tcp_locations[(cmd.header.process_identifier - 1) as usize].clone();
                                add_peer(state.clone(), node_ip, node_port, wr.clone()).await;
                            }
                            
                            if !hmac_valid {
                                error!("[{}:{}]: Received SYSTEM command with invalid HMAC signature, DROPPING CONNECTION. The command: {:?}",
                                self_addr.0, self_addr.1, cmd);
                                let (peer_ip, peer_port) = client.state.tcp_locations[(cmd.header.process_identifier - 1) as usize].clone();
                                let mut write_guard = state.tcp_writers.write().await;
                                write_guard.remove(&(peer_ip.clone(), peer_port));
                                // we don't want to talk with this guy anymore
                                // (though we still allow him to connect again)
                                return;
                            }

                            trace!("[{}:{}]: Received valid SYSTEM command {}",
                                self_addr.0, self_addr.1, cmd);
                            let sector_idx = cmd.header.sector_idx;
                            // the rx_opt is Option (bc it might not be in map) 
                            // of (<Sender<(RegisterCommand, Option<OwnedWriteHalf>)>>, Option<Receiver<...
                            // so, in the map we keep a tx, and potentially rx, of the queue communicating with AR
                            // in the queue, we send pair (RegisterCommand, Option<OwnedWriteHalf>)
                            // Option<OwnedWriteHalf> is for writing a reply to the client
                            // it is an Option, since when sending System commands, 
                            // we won't provide any OwnedWriteHalf for writing a reply

                            // regardless of whether the map has a client queue, 
                            // we should check if the map has the main queue, 
                            // as this determines the task
                            
                            let tx_opt = state.main_cmd_senders.read().await.get(&sector_idx).cloned();
                            let (tx, rx_opt) = match tx_opt {
                                None => {
                                    // TODO: revisit channel capacity
                                    let (tx, rx) = 
                                        tokio::sync::mpsc::channel::<(RegisterCommand, Option<(String, u16)>)>(1000);
                                    state.main_cmd_senders.write().await.insert(sector_idx, tx.clone());
                                    (tx.clone(), Some(rx))
                                },
                                Some(tx) => (tx.clone(), None)
                            };
                            
                            // get or create the client queue
                            let client_tx_opt = state.client_cmd_senders.read().await.get(&sector_idx).cloned();
                            let (_client_tx, client_rx_opt) = match client_tx_opt {
                                None => {
                                    // TODO: revisit channel capacity
                                    let (client_tx, client_rx) = 
                                        tokio::sync::mpsc::channel::<(RegisterCommand, Option<(String, u16)>)>(1000);
                                        state.client_cmd_senders.write().await.insert(sector_idx, client_tx.clone());
                                    (client_tx.clone(), Some(client_rx))
                                },
                                Some(client_tx) => (client_tx, None)
                            };

                            tx.send((RegisterCommand::System(cmd), None)).await.unwrap();
                            // tx.send(RegisterCommand::Client(cmd)).await.unwrap();
                            if let Some(rx) = rx_opt {
                                if let Some(client_rx) = client_rx_opt {
                                    start_ar_worker(client.clone(), sectors_manager.clone(), sector_idx, rx, tx, client_rx).await;
                                }
                            }
                        },
                        RegisterCommand::Client(cmd) => {
                            if i == 0 {
                                add_peer(state.clone(), client_ip.clone(), client_port, wr.clone()).await;
                            }

                            if !hmac_valid {
                                error!("[{}:{}]: Received CLIENT command with invalid HMAC signature, ignoring. The command: {:?}",
                                self_addr.0, self_addr.1, cmd);
                                // let reply_cmd = Arc::new(
                                //     ClientCommandResponse { 
                                //         status: crate::StatusCode::AuthFailure, 
                                //         request_identifier: cmd.header.request_identifier,
                                //         op_return: crate::OperationReturn::Write
                                //     }
                                // );
                                // client.reply_to_client(reply_cmd, wr).await;
                                let mut write_guard = state.tcp_writers.write().await;
                                write_guard.remove(&(client_ip.clone(), client_port));
                                // we don't want to talk with this guy anymore
                                // (though we still allow him to connect again)
                                return;
                            }

                            info!("[{}:{}]: Received valid CLIENT command from ({}, {})\nCommand:{}",
                                self_addr.0, self_addr.1, client_ip, client_port, cmd);
                            let sector_idx = cmd.header.sector_idx;

                            // regardless of whether the map has a client queue, 
                            // we should check if the map has the main queue, 
                            // as this determines the task
                            let tx_opt = state.main_cmd_senders.read().await.get(&sector_idx).cloned();
                            let (tx, rx_opt) = match tx_opt {
                                None => {
                                    // TODO: revisit channel capacity
                                    let (tx, rx) = 
                                        tokio::sync::mpsc::channel::<(RegisterCommand, Option<(String, u16)>)>(1000);
                                    state.main_cmd_senders.write().await.insert(sector_idx, tx.clone());
                                    (tx.clone(), Some(rx))
                                },
                                Some(tx) => (tx.clone(), None)
                            };
                            
                            // get or create the client queue
                            let client_tx_opt = state.client_cmd_senders.read().await.get(&sector_idx).cloned();
                            let (client_tx, client_rx_opt) = match client_tx_opt {
                                None => {
                                    // TODO: revisit channel capacity
                                    let (client_tx, client_rx) = 
                                        tokio::sync::mpsc::channel::<(RegisterCommand, Option<(String, u16)>)>(1000);
                                        state.client_cmd_senders.write().await.insert(sector_idx, client_tx.clone());
                                    (client_tx.clone(), Some(client_rx))
                                },
                                Some(client_tx) => (client_tx, None)
                            };

                            client_tx.send((RegisterCommand::Client(cmd), Some((client_ip.clone(), client_port)))).await.unwrap();
                            trace!("[{}:{}]: Sent command onto the client queue",
                            self_addr.0, self_addr.1);

                            // tx.send(RegisterCommand::Client(cmd)).await.unwrap();
                            if let Some(rx) = rx_opt {
                                if let Some(client_rx) = client_rx_opt{
                                    start_ar_worker(client.clone(), sectors_manager.clone(), sector_idx, rx, tx, client_rx).await;
                                }
                            }
                        }
                    }
                },
                Err(e) => {
                    error!("[{}:{}]: Could not deserialize SYSTEM command: {:?}", self_addr.0, self_addr.1, e);
                    return;
                },
                
            }
        }
    });
}

pub async fn start_tcp_server(client: Arc<MyRegisterClient>, self_addr: (String, u16), _storage_dir: PathBuf) {
    let sectors_manager = client.state.sectors_manager.clone();

    info!("Starting TCP server at {}:{}", self_addr.0, self_addr.1);

    tokio::spawn( async move { // TCP server task (receive messages)
        let socket = TcpListener::bind(
            format!("{}:{}", self_addr.0, self_addr.1)
        ).await.unwrap();

        trace!("Bound to socket {}:{}", self_addr.0, self_addr.1);

        loop {
            let (socket, client_addr) = socket.accept().await.unwrap();
            info!("[{}:{}]: Accepted connection from {}", self_addr.0, self_addr.1, client_addr);
            
            let (rd, wr) = socket.into_split();
            let protected_writer = Arc::new(Mutex::new(wr));
            start_tcp_reader_task(client.clone(), sectors_manager.clone(), protected_writer, rd, client_addr.ip().to_string(), client_addr.port()).await;  
        }
    });
}