use std::collections::{VecDeque};
use std::{sync::Arc};

use log::{trace, warn,};
use tokio::select;

use crate::my_register_client::MyRegisterClient;
use crate::{ClientCommandResponse, sectors_manager_public};
use crate::{RegisterCommand, my_atomic_register::MyAtomicRegister};
use crate::atomic_register_public::AtomicRegister;

pub async fn start_ar_worker(client: Arc<MyRegisterClient>, sectors_manager: Arc<dyn sectors_manager_public::SectorsManager>, sector_idx: u64,
    mut rx: tokio::sync::mpsc::Receiver<(RegisterCommand, u64)>,
    tx: tokio::sync::mpsc::Sender<(RegisterCommand, u64)>,
    mut client_rx: tokio::sync::mpsc::Receiver<(RegisterCommand, u64)>
    ) {
    let _ = tokio::spawn( async move {
        trace!("[AR worker {}]: Starting for sector {}", client.self_rank, sector_idx);
        
        let mut processing_client = false;
        let mut client_in_transit = false; 
        
        let (tx_client_done, mut rx_client_done) = tokio::sync::mpsc::channel(10);
        let mut client_wait_queue = VecDeque::new();

        let res = client_rx.try_recv();
        if let Ok((recv_cmd, writer_opt)) = res {
            trace!("[{}]: Received command on client queue, piping to main", client.self_rank);
            let _ = tx.send((recv_cmd, writer_opt)).await;
            client_in_transit = true;
        }

        let n = client.state.tcp_locations.len() as u8;
        let mut register = MyAtomicRegister::new(
            client.self_rank,
            sector_idx,
            client.clone(),
            sectors_manager,
            n
        ).await;

        trace!("[AR worker {}]: Starting LOOP for sector {}", client.self_rank, sector_idx);

        loop {
            trace!("[AR worker {}, {}]: loop", client.self_rank, sector_idx);
            
            if !processing_client && !client_in_transit && !client_wait_queue.is_empty() {
                trace!("[AR worker {}, {}]: getting client request from client wait queue to main queue", client.self_rank, sector_idx);
                let (recv_cmd, writer_opt) = client_wait_queue.pop_front().unwrap();
                let _ = tx.send((recv_cmd, writer_opt)).await;
                client_in_transit = true;
            }

            select! {
                Some((recv_cmd, client_id)) = rx.recv() => {
                    trace!("[AR worker {}]: got {} for sector {}", client.self_rank, recv_cmd.clone(), sector_idx);
                    match recv_cmd {
                        RegisterCommand::Client(cmd) => {
                            trace!("[AR worker {}, {}]: starting to process client request", client.self_rank, sector_idx);
                            
                            client_in_transit = false;
                            
                            if processing_client {
                                warn!("[AR worker]: Race condition caught! Queueing command instead of overwriting.");
                                client_wait_queue.push_back((RegisterCommand::Client(cmd), client_id));
                                continue;
                            }

                            processing_client = true;                  
                            let tx_client_done_clone = tx_client_done.clone();
        
                            let success_callback: Box<
                                dyn FnOnce(ClientCommandResponse) -> Pin<Box<dyn Future<Output = ()> + Send>>
                                    + Send
                                    + Sync,
                            > = Box::new(move |response: ClientCommandResponse| {
                                Box::pin(async move {
                                    let _ = tx_client_done_clone.send((response, client_id)).await;
                                    trace!("Callback sent information that we're done with this client onto the queue");
                                })
                            });
                            register.client_command(cmd.clone(), success_callback).await;
                            trace!("[AR worker {}, {}]: fully delivered CLIENT command to AR struct", client.self_rank, sector_idx);
                        },
                        RegisterCommand::System(cmd) => {
                            register.system_command(cmd).await;
                            trace!("[AR worker {}, {}]: sent SYSTEM command", client.self_rank, sector_idx);
                        },
                    }
                }
                opt = rx_client_done.recv() => {
                    trace!("[AR worker {}, {}]: finished processing whole client request", client.self_rank, sector_idx);
                    processing_client = false;
                    if let Some((response, client_id)) = opt {
                        let _ = client.reply_to_client(response, client_id).await;
                    }
                    else {
                        warn!("\n\n\n[AR worker {}, {}]: there was no response and client_id on rx_client_done!!!\n\n\n", 
                            client.self_rank, sector_idx);
                    }
                }
                Some((recv_cmd, writer_opt)) = client_rx.recv() => {
                    trace!("[AR worker {}, {}]: received CLIENT request on client queue", client.self_rank, sector_idx);
                    
                    if !processing_client && !client_in_transit {
                        let _ = tx.send((recv_cmd, writer_opt)).await;
                        client_in_transit = true;
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