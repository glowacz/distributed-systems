use std::collections::HashMap;
use std::time::Duration;
use std::{sync::Arc};

use log::{debug, info, trace, warn, error};
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpStream};
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::time::{Instant, interval, sleep};
use tokio::{select};
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;

use crate::server::{SharedState, get_or_create_channels};
use crate::{Ack, ClientCommandResponse, PendingMessage, build_sectors_manager, deserialize_internal_ack, serialize_client_response};
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

        let to_send: Arc<Vec<Mutex<Option<Sender<RegisterCommand>>>>> = Arc::new(
            (0..n_nodes)
            .map(|_| Mutex::new(None))
            .collect()
        );

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
        });
    }

    async fn sender_task(&self, target: u8, mut to_send_rx: Receiver<RegisterCommand>) {
        let hmac_system_key = self.state.hmac_system_key;
        let hmac_client_key = self.state.hmac_client_key;
        let (ip, port) = self.state.tcp_locations[(target - 1) as usize].clone();
        let self_rank = self.self_rank;
    
        let _ = tokio::spawn(async move {
            let retry_period = Duration::from_millis(1000);
            let mut pending_messages: HashMap<Uuid, PendingMessage> = HashMap::new();
            let mut retry_ticker = interval(Duration::from_millis(1000));
            
            loop {
                let tcp_stream: Option<(OwnedReadHalf, OwnedWriteHalf)>;
    
                trace!("[{}]: connecting to node {} ({}:{})...", self_rank, target, ip, port);
                loop {
                    match TcpStream::connect(format!("{}:{}", ip, port)).await {
                        Ok(s) => {
                            trace!("[{}]: connected to node {} ({}:{})", self_rank, target, ip, port);
                            tcp_stream = Some(s.into_split());
                            break;
                        }
                        Err(e) => {
                            debug!("[{}]: failed to connect to {}, error: {}, retrying...", self_rank, target, e);
                            sleep(retry_period).await;
                        }
                    }
                }
    
                let (mut rd, mut wr) = tcp_stream.unwrap();
                let mut connection_active = true;

                let (ack_tx, mut ack_rx) = channel::<(Ack, bool)>(1000);

                // task for reading ACKs (couldn't do deserialize_internal_ack in select! 
                // as receiving other events would cancel this future in the middle of reading from client)
                let _ = tokio::spawn(async move {
                    loop {
                        match deserialize_internal_ack(&mut rd, &hmac_system_key, &hmac_client_key).await {
                            Ok(ack) => {
                                if ack_tx.send(ack).await.is_err() {
                                    // when the main loop goes to the next iteration
                                    // due to connection_active == false
                                    // and we lose ack_tx, we need to break out of this task
                                    // this may happen if peer dies after sending ACK and
                                    // to_send_rx.recv() is scheduled before this ack_tx.send
                                    break;
                                }
                            }
                            Err(_) => {
                                break;
                            }
                        }
                    }
                });
    
                for (id, pending) in pending_messages.iter_mut() {
                    trace!("[{}]: resending pending message {} to node {} after reconnect...", self_rank, id, target);
                    if let Err(e) = serialize_register_command(&pending.cmd, &mut wr, &hmac_system_key).await {
                        warn!("[{}]: failed to resend pending message: {:?}", self_rank, e);
                        connection_active = false;
                        break;
                    }
                    pending.last_sent = Instant::now();
                }
    
                while connection_active {
                    select! {
                        biased;

                        res = ack_rx.recv() => {
                            match res {
                                Some(res) => {
                                    let (ack, _valid_hmac) = res;
                                    pending_messages.remove(&ack.msg_ident);
                                }
                                None => {
                                    info!("[{} with {}]: Reader task died, reconnecting...", self_rank, target);
                                    connection_active = false;
                                }
                            }
                        }

                        Some(cmd) = to_send_rx.recv() => {
                            let msg_id = match cmd.clone() {
                                RegisterCommand::System(sys_cmd) => {
                                    sys_cmd.header.msg_ident
                                }
                                RegisterCommand::Client(_) => {
                                    error!("[{}]: RECEIVED CLIENT COMMAND TO SEND IN A SYSTEM TASK\n\n\n", self_rank);
                                    Uuid::new_v4()
                                }
                            };
                            
                            if let Err(e) = serialize_register_command(&cmd, &mut wr, &hmac_system_key).await {
                                info!("[{}]: failed to send new message to {}: {:?}, reconnecting...", self_rank, target, e);
                                connection_active = false;
                            } else {
                                trace!("[{}]: sent message {} to node {}...", self_rank, msg_id, target);
                            }
                            pending_messages.insert(msg_id, PendingMessage { 
                                cmd, 
                                last_sent: Instant::now(), 
                            });
                        }
    
                        _ = retry_ticker.tick() => {
                            debug!("[{} with {}]: received tick for retrying sending", self_rank, target);
                            let now = Instant::now();
                            for (id, pending) in pending_messages.iter_mut() {
                                if now.duration_since(pending.last_sent) > retry_period {
                                    debug!("[{}]: timeout waiting for ACK {} from node {}, resending...", self_rank, id, target);
                                    if let Err(e) = serialize_register_command(&pending.cmd, &mut wr, &hmac_system_key).await {
                                        info!("[{}]: failed to retry message {}: {:?}, reconnecting...", self_rank, id, e);
                                        connection_active = false;
                                        break;
                                    }
                                    pending.last_sent = now;
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    async fn send_msg(&self, cmd: Arc<SystemRegisterCommand>, target: u8) {
        let sector_idx = cmd.header.sector_idx;
        let cmd = RegisterCommand::System((*cmd).clone());

        if target == self.self_rank {
            let state = self.state.clone();
            let (tx, _client_tx, _, _) = get_or_create_channels(&state, sector_idx).await;
            tx.send((cmd, 0)).await.unwrap();
        }
        else {
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
}

#[async_trait::async_trait]
impl RegisterClient for MyRegisterClient {
    async fn send(&self, msg: register_client_public::Send) {
        let _ = self.send_msg(msg.cmd, msg.target).await;
        trace!("[{}]: sending to target {}", self.self_rank, msg.target);
    }

    async fn broadcast(&self, msg: Broadcast) {
        for target in 1..=self.state.tcp_locations.len() as u8 {
            // await the creation of the task so that it actually happens,
            // (the finishing of the task is not awaited)
            let _ = self.send_msg(msg.cmd.clone(), target).await;
            trace!("[{}]: broadcasting to target {}", self.self_rank, target);
        }
    }
}