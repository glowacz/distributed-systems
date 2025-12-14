use std::collections::HashMap;
use std::pin::Pin;
use std::{path::PathBuf, sync::Arc};

use log::{error, info};
use tokio::net::tcp::{OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};

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
            let _ = self.send_msg(msg.cmd.clone(), target);
            // don't await, just spawn the task (which will be a stubborn sender)
        }
    }
}

pub fn ar_task(client: Arc<MyRegisterClient>, sectors_manager: Arc<MySectorsManager>, sector_idx: u64, mut rx: tokio::sync::mpsc::Receiver<RegisterCommand>) {
    // let client = client.clone();
    // let sectors_manager = sectors_manager.clone();

    tokio::spawn( async move {
        info!("[{}]: Starting AR task for sector {}", client.self_rank, sector_idx);

        let n = client.tcp_locations.len() as u8;
        let mut register = MyAtomicRegister::new(
            client.self_rank,
            sector_idx,
            // client.clone(),
            client,
            // sectors_manager.clone(),
            sectors_manager,
            n
        ).await;

        while let Some(recv_cmd) = rx.recv().await {
            match recv_cmd {
                RegisterCommand::Client(cmd) => {
                    let success_callback: Box<
                        dyn FnOnce(ClientCommandResponse) -> Pin<Box<dyn Future<Output = ()> + Send>>
                            + Send
                            + Sync,
                    > = Box::new(|response: ClientCommandResponse| {
                        Box::pin(async move {
                            println!("Received response: {:?}", response.status);
                            // this AR is ready to handle next command
                            // TODO: send the response back to the client (via TCP)
                            // the queue should be a tuple (client_addr, RegisterCommand)
                            // and we should use RegisterClient to send the response
                            // idk if it is feasible to use it inside a callback
                            // so maybe we could do it after the callback 
                        })
                    });
                    register.client_command(cmd, success_callback).await;
                },
                RegisterCommand::System(cmd) => {
                    register.system_command(cmd).await;
                },
            }
        }
    });

    // TODO: remove the tx from hashmap when the task ends
    // and maybe should join the task handle
}

pub async fn init_registers(client: Arc<MyRegisterClient>, self_addr: (String, u16), storage_dir: PathBuf) {
    let sectors_manager = Arc::new(
        MySectorsManager::new(
            storage_dir
        ).await
    );

    let hmac_client_key = client.hmac_client_key;
    let hmac_system_key = client.hmac_system_key;
    let mut cmd_senders = HashMap::new();

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

            // let cmd_senders = cmd_senders.clone();

            // let (recv_cmd, hmac_valid) = crate::deserialize_register_command(&mut tcp_stream, &hmac_system_key).await.unwrap();
            let deserialize_res = crate::deserialize_register_command(&mut rd, &hmac_system_key, &client_hmac_key).await;
            // info!("[{}:{}]: Deserialized message with SYSTEM hmac, result {:?}", self_addr.0, self_addr.1, deserialize_res);
            info!("[{}:{}]: Deserialized message with SYSTEM hmac", self_addr.0, self_addr.1);
            match deserialize_res {
                Ok((recv_cmd, hmac_valid)) => {
                    match recv_cmd {
                        RegisterCommand::System(cmd) => {
                            if !hmac_valid {
                                error!("[{}:{}]: Received SYSTEM command with invalid HMAC signature, ignoring. The command: {:?}",
                                 self_addr.0, self_addr.1, cmd);
                                return;
                            }
                            info!("[{}:{}]: Received valid SYSTEM command {:?}",
                                 self_addr.0, self_addr.1, cmd);
                            let sector_idx = cmd.header.sector_idx;
                            let (tx, rx_opt) = match cmd_senders.get(&sector_idx) {
                                None => { // though with system messages we should already have a running task
                                    // (with corresponding tx and rx)
                                    // TODO: revisit channel capacity
                                    let (tx, rx) = tokio::sync::mpsc::channel::<RegisterCommand>(1000);
                                    cmd_senders.insert(sector_idx, tx.clone());
                                    (&tx.clone(), Some(rx))
                                },
                                Some(s) => (s, None)
                            };
                            tx.send(RegisterCommand::System(cmd)).await.unwrap();
                            if let Some(rx) = rx_opt {
                                ar_task(
                                    client.clone(),
                                    sectors_manager.clone(),
                                    sector_idx,
                                    rx
                                );
                            }
                        },
                        RegisterCommand::Client(_cmd) => {
                            info!("[{}:{}]: Received CLIENT command but checking with SYSTEM hmac, ignoring...\nThe command: {:?}",
                                 self_addr.0, self_addr.1, _cmd);
                            // ignore, when getting a good client command we will be able to deserialize it,
                            // just the hmac should be invalid with SYSTEM key
                            // this command will be handled when checking with client key below
                        }
                    }
                },
                Err(e) => {
                    error!("Could not deserialize SYSTEM command: {:?}", e);
                    return;
                },
                
            }

            info!("[{}:{}]: Before deserializing message with CLIENT hmac", self_addr.0, self_addr.1);
            let deserialize_res = crate::deserialize_register_command_from_client(&mut rd, &hmac_client_key).await;
            // info!("[{}:{}]: Deserialized message with CLIENT hmac, result {:?}", self_addr.0, self_addr.1, deserialize_res);
            info!("[{}:{}]: Deserialized message with CLIENT hmac", self_addr.0, self_addr.1);

            match deserialize_res {
                Ok((recv_cmd, hmac_valid)) => {
                    match recv_cmd {
                        RegisterCommand::System(_cmd) => {
                            info!("[{}:{}]: Received SYSTEM command but checking with CLIENT hmac, ignoring...\nThe command: {:?}",
                                 self_addr.0, self_addr.1, _cmd);
                            // ignore, when getting a good system command we will be able to deserialize it,
                            // just the hmac should be invalid with CLIENT key
                            // this command was handled when checking with system key above
                        },
                        RegisterCommand::Client(cmd) => {
                            if !hmac_valid {
                                error!("[{}:{}]: Received CLIENT command with invalid HMAC signature, ignoring. The command: {:?}",
                                 self_addr.0, self_addr.1, cmd);
                                let reply_cmd = Arc::new(
                                    ClientCommandResponse { 
                                        status: crate::StatusCode::AuthFailure, 
                                        request_identifier: cmd.header.request_identifier,
                                        op_return: crate::OperationReturn::Write
                                    }
                                );
                                client.reply_to_client(reply_cmd, wr).await;
                                return;
                            }
                            info!("[{}:{}]: Received valid CLIENT command from {}\nCommand:{:?}",
                                 self_addr.0, self_addr.1, _client_addr, cmd);
                            let sector_idx = cmd.header.sector_idx;
                            let (tx, rx_opt) = match cmd_senders.get(&sector_idx) {
                                None => { // though with system messages we should already have a running task
                                    // (with corresponding tx and rx)
                                    // TODO: revisit channel capacity
                                    let (tx, rx) = tokio::sync::mpsc::channel::<RegisterCommand>(1000);
                                    cmd_senders.insert(sector_idx, tx.clone());
                                    (&tx.clone(), Some(rx))
                                },
                                Some(s) => (s, None)
                            };
                            tx.send(RegisterCommand::Client(cmd)).await.unwrap();
                            if let Some(rx) = rx_opt {
                                ar_task(
                                    client.clone(),
                                    sectors_manager.clone(),
                                    sector_idx,
                                    rx
                                );
                            }
                        }
                    }
                },
                Err(e) => {
                    error!("Could not deserialize CLIENT command: {:?}", e);
                    return;
                },
                
            }
        }
    });
}