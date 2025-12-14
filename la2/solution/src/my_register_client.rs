use std::pin::Pin;
use std::{path::PathBuf, sync::Arc};

use tokio::net::TcpStream;

use crate::ClientCommandResponse;
use crate::{Broadcast, Configuration, RegisterClient, RegisterCommand, SystemRegisterCommand, my_atomic_register::MyAtomicRegister, my_sectors_manager::MySectorsManager, register_client_public, serialize_register_command};
use crate::atomic_register_public::AtomicRegister;

pub struct MyRegisterClient {
    hmac_system_key: [u8; 64],
    hmac_client_key: [u8; 32],
    storage_dir: PathBuf,
    tcp_locations: Vec<(String, u16)>,
    self_rank: u8,
    n_sectors: u64,
    // atomic_registers: Vec<MyAtomicRegister>,
}

impl MyRegisterClient {
    pub fn new(conf: Configuration) -> Self {
        Self { 
            hmac_system_key: conf.hmac_system_key,
            hmac_client_key: conf.hmac_client_key,
            storage_dir: conf.public.storage_dir,
            tcp_locations: conf.public.tcp_locations,
            self_rank: conf.public.self_rank,
            n_sectors: conf.public.n_sectors,
            // atomic_registers: Vec::new(),
        }
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
        self.send_msg(msg.cmd, msg.target); 
        // don't await, just spawn the task (which will be a stubborn sender)
    }

    async fn broadcast(&self, msg: Broadcast) {
        // TODO skip TCP when sending to self
        // and maybe there just is a bettter way to send one/many messages
        for target in 1..=self.tcp_locations.len() as u8 {
            self.send_msg(msg.cmd.clone(), target);
            // don't await, just spawn the task (which will be a stubborn sender)
        }
    }
}

// pub async fn init_registers(client: Arc<MyRegisterClient>) -> Vec<MyAtomicRegister> {
pub async fn init_registers(client: Arc<MyRegisterClient>) {
    let sectors_manager = Arc::new(
        MySectorsManager::new(
            client.storage_dir.clone()
        ).await
    );

    // let mut registers = Vec::with_capacity(client.n_sectors as usize);
    let mut cmd_senders = Vec::with_capacity(client.n_sectors as usize);
    // let mut client_cmd_senders = Vec::with_capacity(client.n_sectors as usize);
    // let mut system_cmd_senders = Vec::with_capacity(client.n_sectors as usize);

    for sector_idx in 0..client.n_sectors {
        // TODO: revisit channel capacity
        let (tx, mut rx) = tokio::sync::mpsc::channel::<RegisterCommand>(1000);
        cmd_senders.push(tx);

        // let (client_tx, mut client_rx) = tokio::sync::mpsc::channel::<RegisterCommand>(1000);
        // client_cmd_senders.push(client_tx);

        // let (system_tx, mut system_rx) = tokio::sync::mpsc::channel::<RegisterCommand>(1000);
        // system_cmd_senders.push(system_tx);

        let client = client.clone();
        let sectors_manager = sectors_manager.clone();

        tokio::spawn( async move {
            let mut register = MyAtomicRegister::new(
                client.self_rank,
                sector_idx,
                client.clone(),
                sectors_manager.clone(),
                client.tcp_locations.len() as u8
            ).await;

            // loop {
            //     tokio::select! {
            //         biased;
            //         Some(cmd) = client_rx.recv() => {
            //             register.client_command(match cmd {
            //                 RegisterCommand::Client(c) => c,
            //                 _ => panic!("Expected client command"),
            //             }).await;
            //         },
            //         Some(cmd) = system_rx.recv() => {
            //             register.system_command(match cmd {
            //                 RegisterCommand::System(s) => s,
            //                 _ => panic!("Expected system command"),
            //             }).await;
            //         }
            //     }
            // }

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
        // registers.push(register);
    }
    // registers
}