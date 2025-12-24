use std::collections::{VecDeque};
use std::time::Duration;
use std::{sync::Arc};

use log::{info, trace, warn,};
use tokio::net::tcp::{OwnedWriteHalf};
use tokio::select;
use tokio::sync::{Mutex};
use tokio::time::sleep;

use crate::my_register_client::MyRegisterClient;
use crate::{ClientCommandResponse, StatusCode, sectors_manager_public};
use crate::{RegisterCommand, my_atomic_register::MyAtomicRegister};
use crate::atomic_register_public::AtomicRegister;

pub async fn start_ar_worker(client: Arc<MyRegisterClient>, sectors_manager: Arc<dyn sectors_manager_public::SectorsManager>, sector_idx: u64,
    mut rx: tokio::sync::mpsc::Receiver<(RegisterCommand, u64)>,
    tx: tokio::sync::mpsc::Sender<(RegisterCommand, u64)>,
    mut client_rx: tokio::sync::mpsc::Receiver<(RegisterCommand, u64)>
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
                Some((recv_cmd, client_id)) = rx.recv() => {
                    trace!("[AR worker {}]: got {} for sector {}", client.self_rank, recv_cmd.clone(), sector_idx);
                    match recv_cmd {
                        RegisterCommand::Client(cmd) => {
                            info!("[AR worker {}, {}]: starting to process client request", client.self_rank, sector_idx);
                            processing_client = true;
                            // let tcp_writer_clone = tcp_writer.clone();
                            // let writer = match writer_opt {
                            //     Some(w) => w,
                            //     None => continue,
                            // };
                            let client_clone = client.clone();                    
                            let tx_client_done_clone = tx_client_done.clone();
        
                            let success_callback: Box<
                                dyn FnOnce(ClientCommandResponse) -> Pin<Box<dyn Future<Output = ()> + Send>>
                                    + Send
                                    + Sync,
                            > = Box::new(move |response: ClientCommandResponse| {
                                Box::pin(async move {
                                    // let writer = writer.clone();
                                    trace!("Callback will send response: {:?} to client", response);
                                    // let _ = client_clone.reply_to_client(response, writer.clone()).await;
                                    // let _ = client_clone.reply_to_client(response, client_id).await;
                                    info!("Callback SENT response to client {client_id}");
                                    // let _ = tx_client_done_clone.send(()).await;
                                    let _ = tx_client_done_clone.send((response, client_id)).await;
                                    trace!("Callback sent information that we're done with this client onto the queue");
                                })
                            });
                            register.client_command(cmd.clone(), success_callback).await;
                            trace!("[AR worker {}, {}]: fully delivered CLIENT command to AR struct", client.self_rank, sector_idx);
                            
                            // let response = ClientCommandResponse {
                            //     status: StatusCode::Ok,
                            //     request_identifier: cmd.header.request_identifier,
                            //     op_return: crate::OperationReturn::Write
                            // };
                            // let _ = client_clone.reply_to_client(response, writer.clone()).await;
                        },
                        RegisterCommand::System(cmd) => {
                            register.system_command(cmd).await;
                            trace!("[AR worker {}, {}]: sent SYSTEM command", client.self_rank, sector_idx);
                        },
                    }
                }
                opt = rx_client_done.recv() => {
                    info!("[AR worker {}, {}]: finished processing whole client request", client.self_rank, sector_idx);
                    processing_client = false;
                    // sleep(Duration::from_secs(25)).await;
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