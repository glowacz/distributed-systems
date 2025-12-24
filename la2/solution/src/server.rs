use std::collections::{HashMap};
use std::{path::PathBuf, sync::Arc};

use log::{error, info, trace,};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener};
use tokio::sync::mpsc::{Sender, channel};
use tokio::sync::{Mutex, RwLock};

use crate::ar_worker::start_ar_worker;
use crate::my_register_client::MyRegisterClient;
use crate::{Ack, ClientCommandResponse, StatusCode, sectors_manager_public, serialize_internal_ack};
use crate::{RegisterCommand};

pub struct SharedState {
    pub hmac_system_key: [u8; 64],
    pub hmac_client_key: [u8; 32],
    pub tcp_locations: Vec<(String, u16)>,
    pub have_connection: Vec<Mutex<bool>>,
    pub tcp_writers: Arc<RwLock<HashMap<u64, Arc<Mutex<OwnedWriteHalf>>>>>, // TODO: this should be Vec<Arc<Mutex<OwnedWriteHalf>>>
    pub main_cmd_senders: Arc<RwLock<HashMap<u64, Sender<(RegisterCommand, u64)>>>>,
    pub client_cmd_senders: Arc<RwLock<HashMap<u64, Sender<(RegisterCommand, u64)>>>>,
    pub sectors_manager: Arc<dyn sectors_manager_public::SectorsManager>
}

// TODO: change the level of every log to trace! (maybe except new connection)

// Helper to safely get or create channels atomically
async fn get_or_create_channels(
    state: &Arc<SharedState>,
    sector_idx: u64
) -> (
    Sender<(RegisterCommand, u64)>, // main_tx
    Sender<(RegisterCommand, u64)>, // client_tx
    Option<tokio::sync::mpsc::Receiver<(RegisterCommand, u64)>>, // main_rx (if created)
    Option<tokio::sync::mpsc::Receiver<(RegisterCommand, u64)>>  // client_rx (if created)
) {
    {
        let main_map = state.main_cmd_senders.read().await;
        let client_map = state.client_cmd_senders.read().await;
        if let (Some(main_tx), Some(client_tx)) = (main_map.get(&sector_idx), client_map.get(&sector_idx)) {
            return (main_tx.clone(), client_tx.clone(), None, None);
        }
    }

    let mut main_map = state.main_cmd_senders.write().await;
    let mut client_map = state.client_cmd_senders.write().await;

    let (main_tx, main_rx) = match main_map.get(&sector_idx) {
        Some(tx) => {
            (tx.clone(), None)
        }
        None => {
            let (tx, rx) = tokio::sync::mpsc::channel::<(RegisterCommand, u64)>(1000);
            main_map.insert(sector_idx, tx.clone());
            (tx, Some(rx))
        }
    };

    let (client_tx, client_rx) = match client_map.get(&sector_idx) {
        Some(tx) => {
            (tx.clone(), None)
        }
        None => {
            let (tx, rx) = tokio::sync::mpsc::channel::<(RegisterCommand, u64)>(1000);
            client_map.insert(sector_idx, tx.clone());
            (tx, Some(rx))
        }
    };

    (main_tx, client_tx, main_rx, client_rx)
}

pub async fn tcp_reader_task(client: Arc<MyRegisterClient>, sectors_manager: Arc<dyn sectors_manager_public::SectorsManager>, 
    wr: Arc<Mutex<OwnedWriteHalf>>, // the wr is either for responding to the client or sending internal ACK
    // mut wr: OwnedWriteHalf, // the wr is either for responding to the client or sending internal ACK
    // writing VAL or ACK from alogrithm is done via the permanent task in MyRegisterClient
    mut rd: OwnedReadHalf,
    client_ip: String, client_port: u16,
    client_id: u64
) { // reading from a single client/node

    let hmac_key = client.state.hmac_system_key;
    
    let _ = tokio::spawn( async move {
        let self_addr = client.self_addr.clone();
        let state = client.state.clone();

        for _i in 0.. {
            trace!("[{}:{}]: ready to read and deserialize next message", self_addr.0, self_addr.1);
            let deserialize_res = crate::deserialize_register_command(
                &mut rd, &state.hmac_system_key, &state.hmac_client_key).await;
            // trace!("[{}:{}]: Deserialized message, result {:?}", self_addr.0, self_addr.1, deserialize_res);
            trace!("[{}:{}]: Deserialized message", self_addr.0, self_addr.1);
            match deserialize_res {
                Ok((recv_cmd, hmac_valid)) => {
                    match recv_cmd {
                        RegisterCommand::System(cmd) => {
                            if !hmac_valid {
                                error!("[{}:{}]: Received SYSTEM command with invalid HMAC signature, DROPPING CONNECTION. The command: {:?}",
                                    self_addr.0, self_addr.1, cmd);
                                // let (peer_ip, peer_port) = client.state.tcp_locations[(cmd.header.process_identifier - 1) as usize].clone();
                                // we don't want to read from this guy anymore
                                // (though we still allow him to connect again)
                                return;
                            }

                            trace!("[{}:{}]: Received valid SYSTEM command {}",
                                self_addr.0, self_addr.1, cmd);
                            
                            let mut writer = wr.lock().await;
                            // let _ = serialize_client_response(&cmd, &mut *tcp_writer, &hmac_key).await;
                            let _ = serialize_internal_ack(&Ack{}, &mut *writer, &hmac_key).await;

                            trace!("[{}:{}]: Sent ACK for command {}",
                                self_addr.0, self_addr.1, cmd);

                            let sector_idx = cmd.header.sector_idx;

                            let (tx, _client_tx, rx_opt, client_rx_opt) = get_or_create_channels(&state, sector_idx).await;

                            tx.send((RegisterCommand::System(cmd), 0)).await.unwrap();
                            
                            if let (Some(rx), Some(client_rx)) = (rx_opt, client_rx_opt) {
                                start_ar_worker(client.clone(), sectors_manager.clone(), sector_idx, rx, tx, client_rx).await;
                            }
                        },
                        RegisterCommand::Client(cmd) => {
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

                                // we don't want to talk read from guy anymore
                                // (though we still allow him to connect again)
                                return;
                            }

                            if _i == 0 {
                                let (to_send_tx, to_send_rx) = channel::<ClientCommandResponse>(1000);
                                client.to_send_client.write().await.insert(client_id, to_send_tx.clone());
                                client.reply_to_client_task(client_id, to_send_rx).await;
                            }

                            info!("[{}:{}]: Received valid CLIENT command from ({}, {})\nCommand:{}",
                                self_addr.0, self_addr.1, client_ip, client_port, cmd);
                            let sector_idx = cmd.header.sector_idx;

                            let (tx, client_tx, rx_opt, client_rx_opt) = get_or_create_channels(&state, sector_idx).await;

                            client_tx.send((
                                RegisterCommand::Client(cmd), client_id 
                            )).await.unwrap();
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
        let mut client_id: u64 = 0;

        trace!("Bound to socket {}:{}", self_addr.0, self_addr.1);
        

        loop {
            client_id += 1;
            let (socket, client_addr) = socket.accept().await.unwrap();
            info!("[{}:{}]: Accepted connection from {}", self_addr.0, self_addr.1, client_addr);
            
            let (rd, wr) = socket.into_split();
            let protected_writer = Arc::new(Mutex::new(wr));
            client.state.tcp_writers.write().await.insert(client_id, protected_writer.clone());

            tcp_reader_task(client.clone(), sectors_manager.clone(), protected_writer, rd, client_addr.ip().to_string(), client_addr.port(), client_id).await;  
        }
    });
}