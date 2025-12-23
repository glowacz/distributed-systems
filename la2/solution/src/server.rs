use std::collections::{HashMap};
use std::{path::PathBuf, sync::Arc};

use log::{error, info, trace,};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener};
use tokio::sync::mpsc::Sender;
use tokio::sync::{Mutex, RwLock};

use crate::ar_worker::start_ar_worker;
use crate::my_register_client::MyRegisterClient;
use crate::{sectors_manager_public};
use crate::{RegisterCommand};

pub struct SharedState {
    pub hmac_system_key: [u8; 64],
    pub hmac_client_key: [u8; 32],
    pub tcp_locations: Vec<(String, u16)>,
    pub have_connection: Vec<Mutex<bool>>,
    pub tcp_writers: Arc<RwLock<HashMap<(String, u16), Arc<Mutex<OwnedWriteHalf>>>>>, // TODO: this should be Vec<Arc<Mutex<OwnedWriteHalf>>>
    pub main_cmd_senders: Arc<RwLock<HashMap<u64, Sender<(RegisterCommand, Option<Arc<Mutex<OwnedWriteHalf>>>)>>>>,
    pub client_cmd_senders: Arc<RwLock<HashMap<u64, Sender<(RegisterCommand, Option<Arc<Mutex<OwnedWriteHalf>>>)>>>>,
    pub sectors_manager: Arc<dyn sectors_manager_public::SectorsManager>
}

// TODO: change the level of every log to trace! (maybe except new connection)

async fn add_peer(state: Arc<SharedState>, ip: String, port: u16, writer: Arc<Mutex<OwnedWriteHalf>>) {
    let key = (ip, port);

    // TODO: have separate tcp_writers for nodes of system and clients
    // this should increase performance with mutexes
    // and maybe will solve a bug
    // also, will make code simpler, no need to look up tcp_locations

    let mut map_guard = state.tcp_writers.write().await;
    map_guard.insert(key, writer);
}

pub async fn start_tcp_reader_task(client: Arc<MyRegisterClient>, sectors_manager: Arc<dyn sectors_manager_public::SectorsManager>, 
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
                                        tokio::sync::mpsc::channel::<(RegisterCommand, Option<Arc<Mutex<OwnedWriteHalf>>>)>(1000);
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
                                        tokio::sync::mpsc::channel::<(RegisterCommand, Option<Arc<Mutex<OwnedWriteHalf>>>)>(1000);
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
                            // if i == 0 {
                            //     add_peer(state.clone(), client_ip.clone(), client_port, wr.clone()).await;
                            // }

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
                                        tokio::sync::mpsc::channel::<(RegisterCommand, Option<Arc<Mutex<OwnedWriteHalf>>>)>(1000);
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
                                        tokio::sync::mpsc::channel::<(RegisterCommand, Option<Arc<Mutex<OwnedWriteHalf>>>)>(1000);
                                        state.client_cmd_senders.write().await.insert(sector_idx, client_tx.clone());
                                    (client_tx.clone(), Some(client_rx))
                                },
                                Some(client_tx) => (client_tx, None)
                            };

                            client_tx.send((
                                RegisterCommand::Client(cmd), Some(wr.clone()
                            ))).await.unwrap();
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