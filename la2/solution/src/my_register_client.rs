use std::collections::HashMap;
use std::{path::PathBuf, sync::Arc};

use log::{error, info};
use tokio::net::tcp::{OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::select;

use crate::{ClientCommandResponse, serialize_client_response};
use crate::{Broadcast, Configuration, RegisterClient, RegisterCommand, SystemRegisterCommand, my_atomic_register::MyAtomicRegister, my_sectors_manager::MySectorsManager, register_client_public, serialize_register_command};
use crate::atomic_register_public::AtomicRegister;

pub struct MyRegisterClient {
    hmac_system_key: [u8; 64],
    hmac_client_key: [u8; 32],
    // storage_dir: PathBuf,
    tcp_locations: Vec<(String, u16)>,
    self_rank: u8,
    _n_sectors: u64,
    // atomic_registers: Vec<MyAtomicRegister>,
}

impl MyRegisterClient {
    pub fn new(conf: Configuration) -> Self {
        Self {
            hmac_system_key: conf.hmac_system_key,
            hmac_client_key: conf.hmac_client_key,
            tcp_locations: conf.public.tcp_locations,
            self_rank: conf.public.self_rank,
            _n_sectors: conf.public.n_sectors,
        }
    }

    pub async fn reply_to_client(&self, cmd: Arc<ClientCommandResponse>, mut tcp_writer: OwnedWriteHalf) {
        // TODO should it be stubborn as well ???
        let hmac_key = self.hmac_client_key;

        let _ = tokio::spawn( async move {
            // let mut tcp_stream = TcpStream::connect(
            //     format!("{host}:{port}")
            // ).await.unwrap();

            // let _ = serialize_client_response(&cmd, &mut tcp_stream, &hmac_key).await;
            let _ = serialize_client_response(&cmd, &mut tcp_writer, &hmac_key).await;
        });
    }

    async fn send_msg(&self, cmd: Arc<SystemRegisterCommand>, target: u8) {
        // TODO make it stubborn
        let (host, port) = self.tcp_locations[(target - 1) as usize].clone();
        let hmac_key = self.hmac_system_key;

        let _ = tokio::spawn( async move {
            let mut tcp_stream = TcpStream::connect(
                format!("{host}:{port}")
            ).await.unwrap();

            let cmd = RegisterCommand::System((*cmd).clone());
            let _ = serialize_register_command(&cmd, &mut tcp_stream, &hmac_key).await;
        });
    }

    // TODO listen for incoming messages (both client and system) (on TCP)
    // now we can just send (system) messages, but not receive them
}

#[async_trait::async_trait]
impl RegisterClient for MyRegisterClient {
    async fn send(&self, msg: register_client_public::Send) {
        let _ = self.send_msg(msg.cmd, msg.target); 
        // don't await, just spawn the task (which will be a stubborn sender)
    }

    async fn broadcast(&self, msg: Broadcast) {
        // TODO skip TCP when sending to self
        // and maybe there just is a bettter way to send one/many messages
        for target in 1..=self.tcp_locations.len() as u8 {
            info!("[{}]: broadcast to target {}", self.self_rank, target);
            // await the creation of the task so that it actually happens,
            // (the finishing of the task is not awaited)
            let _ = self.send_msg(msg.cmd.clone(), target).await;
        }
    }
}

pub async fn ar_task(client: Arc<MyRegisterClient>, sectors_manager: Arc<MySectorsManager>, sector_idx: u64, 
                mut rx: tokio::sync::mpsc::Receiver<(RegisterCommand, Option<OwnedWriteHalf>)>,
                tx: tokio::sync::mpsc::Sender<(RegisterCommand, Option<OwnedWriteHalf>)>,
                mut client_rx: tokio::sync::mpsc::Receiver<(RegisterCommand, Option<OwnedWriteHalf>)>
                ) {
    info!("[AR worker {}]: Starting for sector {}", client.self_rank, sector_idx);
    // let client = client.clone();
    // let sectors_manager = sectors_manager.clone();
    
    let mut processing_client = false;
    let (tx_client_done, mut rx_client_done) = tokio::sync::mpsc::channel(1000);

    // if there is sth on client queue, start by moving it to main queue
    let res = client_rx.try_recv();
    if let Ok((recv_cmd, writer_opt)) = res {
        info!("[{}]: Received command on client queue, piping to main",
          client.self_rank);
        let _ = tx.send((recv_cmd, writer_opt)).await;
    }

    let n = client.tcp_locations.len() as u8;
    let mut register = MyAtomicRegister::new(
        client.self_rank,
        sector_idx,
        client.clone(),
        // client,
        // sectors_manager.clone(),
        sectors_manager,
        n
    ).await;

    info!("[AR worker {}]: Starting LOOP for sector {}", client.self_rank, sector_idx);

    // while let Some((recv_cmd, writer_opt)) = rx.recv().await {
    loop {
        select! {
            Some((recv_cmd, writer_opt)) = rx.recv() => {
                info!("[AR worker {}]: got {} for sector {}", client.self_rank, recv_cmd.clone(), sector_idx);
                match recv_cmd {
                    RegisterCommand::Client(cmd) => {
                        processing_client = true;
                        // let tcp_writer_clone = tcp_writer.clone();
                        let tcp_writer = match writer_opt {
                            Some(w) => w,
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
                        > = Box::new(|response: ClientCommandResponse| {
                            Box::pin(async move {
                                info!("Callback will send response: {:?}", response.status);
                                client_clone.reply_to_client(Arc::new(response), tcp_writer).await;
                                let _ = tx_client_done_clone.send(());
                            })
                        });
                        register.client_command(cmd, success_callback).await;
                        info!("[[AR worker {}]: sent CLIENT command to AR struct for sector {}", client.self_rank, sector_idx);
                    },
                    RegisterCommand::System(cmd) => {
                        register.system_command(cmd).await;
                        info!("[{}]: sent SYSTEM command for sector {}", client.self_rank, sector_idx);
                    },
                }
            }
            _ = rx_client_done.recv() => {
                info!("[{}]: finished processing whole CLIENT request for sector {}", client.self_rank, sector_idx);
                processing_client = false;
            }
        }
        
        // check if we're processing client request and if there is sth on client queue
        // if NO and YES, move the request from client to main queue
        if !processing_client {
            let res = client_rx.try_recv();
            if let Ok((recv_cmd, writer_opt)) = res {
                info!("[{}]: adding new CLIENT request for sector {} to main queue", client.self_rank, sector_idx);
                let _ = tx.send((recv_cmd, writer_opt)).await;
            }
        }

    }
    // TODO: remove the tx from hashmap when the task ends
    // and maybe should join the task handle
    // BUT FOR NOW DON'T END THE TASK (IT HAS INFINITE LOOP 
    // AND NO BREAKABLE (TCP) CONNECTIONS, IT WON'T END)
}

pub async fn init_registers(client: Arc<MyRegisterClient>, self_addr: (String, u16), storage_dir: PathBuf) {
    let sectors_manager = Arc::new(
        MySectorsManager::new(
            storage_dir
        ).await
    );

    let hmac_client_key = client.hmac_client_key;
    let hmac_system_key = client.hmac_system_key;
    let mut main_cmd_senders = HashMap::new();
    let mut client_cmd_senders = HashMap::new();

    info!("Starting TCP server at {}:{}", self_addr.0, self_addr.1);

    tokio::spawn( async move { // TCP server task (receive messages)
        let socket = TcpListener::bind(
            format!("{}:{}", self_addr.0, self_addr.1)
        ).await.unwrap();

        info!("Bound to socket {}:{}", self_addr.0, self_addr.1);

        loop {
            let (socket, _client_addr) = socket.accept().await.unwrap();
            let (mut rd, wr) = socket.into_split();

            info!("[{}:{}]: Accepted connection from {}", self_addr.0, self_addr.1, _client_addr);

            // let writer = Arc::new(tokio::sync::Mutex::new(wr));

            // let main_cmd_senders = main_cmd_senders.clone();

            let deserialize_res = crate::deserialize_register_command(&mut rd, &hmac_system_key, &hmac_client_key).await;
            // info!("[{}:{}]: Deserialized message, result {:?}", self_addr.0, self_addr.1, deserialize_res);
            info!("[{}:{}]: Deserialized message", self_addr.0, self_addr.1);
            match deserialize_res {
                Ok((recv_cmd, hmac_valid)) => {
                    match recv_cmd {
                        RegisterCommand::System(cmd) => {
                            if !hmac_valid {
                                error!("[{}:{}]: Received SYSTEM command with invalid HMAC signature, ignoring. The command: {:?}",
                                 self_addr.0, self_addr.1, cmd);
                                drop(rd);
                                drop(wr);
                                continue;
                            }

                            info!("[{}:{}]: Received valid SYSTEM command {:?}",
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
                            let (tx, rx_opt) = match main_cmd_senders.get(&sector_idx) {
                                None => {
                                    // TODO: revisit channel capacity
                                    let (tx, rx) = 
                                        tokio::sync::mpsc::channel::<(RegisterCommand, Option<OwnedWriteHalf>)>(1000);
                                    main_cmd_senders.insert(sector_idx, tx.clone());
                                    (tx.clone(), Some(rx))
                                },
                                Some(tx) => (tx.clone(), None)
                            };
                            
                            // get or create the client queue
                            let (_client_tx, client_rx_opt) = match client_cmd_senders.get(&sector_idx) {
                                None => {
                                    // TODO: revisit channel capacity
                                    let (client_tx, client_rx) = 
                                        tokio::sync::mpsc::channel::<(RegisterCommand, Option<OwnedWriteHalf>)>(1000);
                                        client_cmd_senders.insert(sector_idx, client_tx.clone());
                                    (&client_tx.clone(), Some(client_rx))
                                },
                                Some(client_tx) => (client_tx, None)
                            };

                            tx.send((RegisterCommand::System(cmd), None)).await.unwrap();
                            // tx.send(RegisterCommand::Client(cmd)).await.unwrap();
                            if let Some(rx) = rx_opt {
                                if let Some(client_rx) = client_rx_opt {
                                    // for moving into tokio task
                                    let client = client.clone();
                                    let sectors_manager = sectors_manager.clone();

                                    let _ = tokio::spawn( async move {
                                        ar_task(
                                            client,
                                            sectors_manager,
                                            sector_idx,
                                            rx,
                                            tx.clone(),
                                            client_rx
                                        )
                                    });
                                }
                            }

                            // let (tx, rx_opt) = match main_cmd_senders.get(&sector_idx) {
                            //     None => {
                            //         // TODO: revisit channel capacity
                            //         let (tx, rx) = 
                            //             tokio::sync::mpsc::channel::<(RegisterCommand, Option<OwnedWriteHalf>)>(1000);
                            //         main_cmd_senders.insert(sector_idx, tx.clone());
                            //         (tx.clone(), Some(rx))
                            //     },
                            //     Some(tx) => (tx.clone(), None)
                            // };
                            // tx.send((RegisterCommand::System(cmd), None)).await.unwrap();
                            // if let Some(rx) = rx_opt {
                            //     ar_task(
                            //         client.clone(),
                            //         sectors_manager.clone(),
                            //         sector_idx,
                            //         rx,
                            //         tx.clone()
                            //     );
                            // }
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
                                drop(rd);
                                drop(wr);
                                continue;
                            }

                            info!("[{}:{}]: Received valid CLIENT command from {}\nCommand:{:?}",
                                 self_addr.0, self_addr.1, _client_addr, cmd);
                            let sector_idx = cmd.header.sector_idx;

                            // regardless of whether the map has a client queue, 
                            // we should check if the map has the main queue, 
                            // as this determines the task
                            // TODO: as deserialization and piping the command to AR worker
                            // should happen in a new task, we will need mutex for the queues
                            let (tx, rx_opt) = match main_cmd_senders.get(&sector_idx) {
                                None => {
                                    // TODO: revisit channel capacity
                                    let (tx, rx) = 
                                        tokio::sync::mpsc::channel::<(RegisterCommand, Option<OwnedWriteHalf>)>(1000);
                                    main_cmd_senders.insert(sector_idx, tx.clone());
                                    (tx.clone(), Some(rx))
                                },
                                Some(tx) => (tx.clone(), None)
                            };
                            
                            // get or create the client queue
                            let (client_tx, client_rx_opt) = match client_cmd_senders.get(&sector_idx) {
                                None => {
                                    // TODO: revisit channel capacity
                                    let (client_tx, client_rx) = 
                                        tokio::sync::mpsc::channel::<(RegisterCommand, Option<OwnedWriteHalf>)>(1000);
                                        client_cmd_senders.insert(sector_idx, client_tx.clone());
                                    (&client_tx.clone(), Some(client_rx))
                                },
                                Some(client_tx) => (client_tx, None)
                            };

                            client_tx.send((RegisterCommand::Client(cmd), Some(wr))).await.unwrap();
                            info!("[{}:{}]: Sent command onto the client queue",
                              self_addr.0, self_addr.1);

                            // tx.send(RegisterCommand::Client(cmd)).await.unwrap();
                            if let Some(rx) = rx_opt {
                                if let Some(client_rx) = client_rx_opt{
                                    // for moving into tokio task
                                    let client = client.clone();
                                    let sectors_manager = sectors_manager.clone();

                                    info!("[{}:{}]: Spawning a new client task",
                                      self_addr.0, self_addr.1);

                                    let _ = tokio::spawn( async move {
                                        ar_task(
                                            client,
                                            sectors_manager,
                                            sector_idx,
                                            rx,
                                            tx.clone(),
                                            client_rx
                                        ).await;
                                    });
                                }
                            }
                        }
                    }
                },
                Err(e) => {
                    error!("Could not deserialize SYSTEM command: {:?}", e);
                    return;
                },
                
            }
        }
    });
}