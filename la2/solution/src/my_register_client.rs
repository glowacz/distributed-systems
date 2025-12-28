use std::collections::HashMap;
use std::time::Duration;
use std::{sync::Arc};

use log::{info, trace, warn,};
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpStream};
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::time::{sleep};
use tokio::{select};
use tokio::sync::{Mutex, RwLock};

use crate::server::{SharedState};
use crate::{ClientCommandResponse, build_sectors_manager, deserialize_internal_ack, serialize_client_response};
use crate::{Broadcast, Configuration, RegisterClient, RegisterCommand, SystemRegisterCommand, register_client_public, serialize_register_command};

#[derive(Clone)]
pub struct MyRegisterClient {
    pub self_rank: u8,
    pub _n_sectors: u64,
    pub self_addr: (String, u16),
    to_send: Arc<Vec<Mutex<Option<Sender<RegisterCommand>>>>>,
    pub to_send_client: Arc<RwLock<HashMap<u64, Sender<ClientCommandResponse>>>>,

    pub state: Arc<SharedState>
}

// TODO: change the level of every log to trace! (maybe except new connection)

impl MyRegisterClient {
    pub async fn new(conf: Configuration) -> Self {
        let n_nodes = conf.public.tcp_locations.len();

        let self_addr = conf.public.tcp_locations[(conf.public.self_rank - 1) as usize].clone();
        let sectors_manager = build_sectors_manager(conf.public.storage_dir).await;
        let have_connection: Vec<Mutex<bool>> = (0..n_nodes)
            .map(|_| Mutex::new(false))
            .collect();

        // let to_send: Arc<Vec<RwLock<VecDeque<RegisterCommand>>>> = Arc::new(
        //     (0..n_nodes)
        //     .map(|_| RwLock::new(VecDeque::new()))
        //     .collect()
        // );

        let to_send: Arc<Vec<Mutex<Option<Sender<RegisterCommand>>>>> = Arc::new(
            (0..n_nodes)
            .map(|_| Mutex::new(None))
            .collect()
        );

        // let to_send_client: Arc<Vec<Mutex<Option<Sender<ClientCommandResponse>>>>> = Arc::new(
        //     (0..n_nodes)
        //     .map(|_| Mutex::new(None))
        //     .collect()
        // );

        let to_send_client = Arc::new(RwLock::new(HashMap::new()));

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
            to_send,
            to_send_client,
            state
        }
    }

    pub async fn reply_to_client(&self, cmd: ClientCommandResponse, client_id: u64) {
        let to_send_tx_opt = self.to_send_client.read().await.get(&client_id).cloned();
        
        if let Some(to_send_tx) = to_send_tx_opt {
            let _ = to_send_tx.send(cmd).await;
        }
        else {
            warn!("\n\nTHERE WAS NO CHANNEL FOR SENDING RESPONSES TO THIS CLIENT\n\n");
        }
    }

    pub async fn reply_to_client_task(&self, 
        client_id: u64,
        // wr: Arc<Mutex<OwnedWriteHalf>>, 
        mut to_send_rx: Receiver<ClientCommandResponse>
    ) {    
        let hmac_client_key = self.state.hmac_client_key;
        let wr = self.state.tcp_writers.read().await.get(&client_id).unwrap().clone();

        let self_rank = self.self_rank;

        trace!("[{self_rank}]: started task for replying to client {client_id}\n==========================================");

        tokio::spawn(async move {    
            while let Some(response) = to_send_rx.recv().await {
                info!("\n[MyRegisterClient {}] Before replying to client {client_id}\n", self_rank);

                let mut tcp_writer = wr.lock().await;
                let res = serialize_client_response(&response, &mut *tcp_writer, &hmac_client_key).await;
                
                let flush_res = tcp_writer.flush().await;
                
                info!("[MyRegisterClient {}] Replied to client {client_id} with result {:?} and flush result {:?}", self_rank, res, flush_res);
            }
            // tcp_writer is dropped here nicely when the sender is dropped
        });
    }

    async fn sender_task(&self, target: u8, mut to_send_rx: Receiver<RegisterCommand>) {
        let hmac_system_key = self.state.hmac_system_key;
        let hmac_client_key = self.state.hmac_client_key;
        let (ip, port) = self.state.tcp_locations[(target - 1) as usize].clone();
        let self_rank = self.self_rank;
        // let to_send = self.to_send[(self_rank - 1) as usize];

        let _ = tokio::spawn(async move {
            let retry_period = Duration::from_millis(1000);
            let mut tcp_stream: Option<(OwnedReadHalf, OwnedWriteHalf)> = None;

            while let Some(cmd) = to_send_rx.recv().await {
                loop {
                    // stubborn connect, without connection sending and waiting for ACKs
                    // don't make sense
                    if tcp_stream.is_none() {
                        info!("[{}]: connecting to node {} ({}:{})...", self_rank, target, ip, port);
                        loop {
                            match TcpStream::connect(format!("{}:{}", ip, port)).await {
                                Ok(s) => {
                                    info!("[{}]: connected to node {} ({}:{})", self_rank, target, ip, port);
                                    tcp_stream = Some(s.into_split());
                                    break;
                                }
                                Err(e) => {
                                    info!("[{}]: failed to connect to node {}, error: {}, retrying...", self_rank, target, e);
                                    sleep(retry_period).await;
                                }
                            }
                        }
                    }
                    else {
                        // info!("[{}]: there was TCP connection (for sending) with node {} ({}:{})...", self_rank, target, ip, port);
                    }

                    let (rd_ack, wr) = tcp_stream.as_mut().unwrap();
                    // info!("[{}]: connected to node {} ({}:{})", self_rank, target, ip.clone(), port);

                    if let Err(e) = serialize_register_command(&cmd, wr, &hmac_system_key).await {
                        info!("[{}]: failed to write to node {}: error {:?}, reconnecting...", self_rank, target, e);
                        tcp_stream = None;
                        sleep(retry_period).await;
                        continue; // Retrying in next loop turn (reconnect -> resend)
                    }
                    else {
                        trace!("[{}]: sent message to node {} ({}:{})...", self_rank, target, ip, port);
                        // break; // for turning off ACKs
                    }

                    info!("[{}]: going into SELECT (with node {} ({}:{}))", self_rank, target, ip, port);

                    select! {
                        biased;

                        res = deserialize_internal_ack(rd_ack, &hmac_system_key, &hmac_client_key) => {
                            if let Err(_e) = res {
                                warn!("[{}]: error getting ACK from node {} ({}{})", self_rank, target, ip.clone(), port);
                                tcp_stream = None;
                            }
                            else {
                                trace!("[{}]: got ACK from node {} ({}:{}))", self_rank, target, ip, port);
                                break;
                            }
                        }
                        _ = sleep(retry_period) => {
                            info!("[{}]: timeout waiting for ACK from node {}({}{}), resending...", self_rank, target, ip.clone(), port);
                        }
                    }
                    info!("[{}]: after SELECT (with node {} ({}:{}))", self_rank, target, ip, port);
                }
            }
        });
    }

    async fn send_msg(&self, cmd: Arc<SystemRegisterCommand>, target: u8) {
        let cmd = RegisterCommand::System((*cmd).clone());

        let mut to_send_mut = self.to_send[(target - 1) as usize].lock().await;
        let to_send_tx = match &*to_send_mut {
            Some(tx) => tx.clone(),
            None => {
                let (tx, rx) = channel::<RegisterCommand>(1000);
                *to_send_mut = Some(tx.clone());
                self.sender_task(target, rx).await;

                tx
            }
        };
        let _ = to_send_tx.send(cmd).await;
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