use std::collections::{HashMap};
use std::{sync::Arc};

use log::{info, trace,};
use tokio::net::tcp::{OwnedWriteHalf};
use tokio::net::{TcpStream};
use tokio::sync::{Mutex, RwLock};

use crate::server::{SharedState, start_tcp_reader_task};
use crate::{ClientCommandResponse, build_sectors_manager, serialize_client_response};
use crate::{Broadcast, Configuration, RegisterClient, RegisterCommand, SystemRegisterCommand, register_client_public, serialize_register_command};

#[derive(Clone)]
pub struct MyRegisterClient {
    pub self_rank: u8,
    pub _n_sectors: u64,
    pub self_addr: (String, u16),

    pub state: Arc<SharedState>
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

    pub async fn reply_to_client(&self, cmd: ClientCommandResponse, writer: Arc<Mutex<OwnedWriteHalf>>) -> std::io::Result<()> {
        // TODO should it be stubborn as well ???
        let hmac_key = self.state.hmac_client_key;
        
        // let single_mut = {
        //     let read_guard = self.state.tcp_writers.read().await;
        //     match read_guard.get(&(ip, port)) {
        //         Some(arc_single_mut) => arc_single_mut.clone(),
        //         None => {
        //             return Err(std::io::Error::new(
        //                 std::io::ErrorKind::NotFound, 
        //                 "Client not found"
        //             ));
        //         }
        //     }
        //     // guard for the whole map dropped here, won't block further
        // };

        let self_rank = self.self_rank;
        let _ = tokio::spawn( async move {
            info!("[MyRegisterClient {}] Before replying to client", self_rank);
            let mut tcp_writer = writer.lock().await;
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

                        // TODO: here we should only spawn a reader for internal ACKs
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