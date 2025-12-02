use bincode::config::standard;
use log::debug;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::mem;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::time::Duration;
use uuid::Uuid;

use module_system::{Handler, ModuleRef, System, TimerHandle};

/// A message, which disables a process. Used for testing.
pub struct Disable;

/// A message, which enables a process. Used for testing.
pub struct Enable;

struct Init;

#[derive(Clone)]
struct Timeout;

pub struct FailureDetectorModule {
    /// This is to simulate a disabled process. Keep those checks in place.
    enabled: bool,
    timeout_handle: Option<TimerHandle>,
    delta: Duration,
    delay: Duration,
    self_ref: ModuleRef<Self>,
    self_uuid: Uuid,
    other_addresses: HashMap<Uuid, SocketAddr>,
    socket: Arc<UdpSocket>,
    prev_alive_set: HashSet<Uuid>,
    curr_alive_set: HashSet<Uuid>,
    // TODO add whatever fields necessary.
}

impl FailureDetectorModule {
    pub async fn new(
        system: &mut System,
        delta: Duration,
        addresses: &HashMap<Uuid, SocketAddr>,
        ident: Uuid,
    ) -> ModuleRef<Self> {
        let addr = addresses.get(&ident).unwrap();
        let mut other_addresses = addresses.clone();
        other_addresses.remove(&ident);
        let socket = Arc::new(UdpSocket::bind(addr).await.unwrap());

        let module_ref = system
            .register_module(|self_ref| Self {
                enabled: true,
                timeout_handle: None,
                delta,
                delay: delta,
                self_ref,
                self_uuid: ident,
                other_addresses,
                socket: socket.clone(),
                prev_alive_set: HashSet::new(),
                curr_alive_set: HashSet::new()
                // TODO initialize the fields you added
            })
            .await;

        // spawn UDP listener -> ModuleRef bridge
        tokio::spawn(deserialize_and_forward(socket, module_ref.clone()));

        module_ref.send(Init).await;

        module_ref
    }
}

#[async_trait::async_trait]
impl Handler<Init> for FailureDetectorModule {
    async fn handle(&mut self, _msg: Init) {
        self.timeout_handle = Some(self.self_ref.request_tick(Timeout, self.delay).await);
    }
}

/// New operation arrived at a socket.
#[async_trait::async_trait]
impl Handler<DetectorOperationUdp> for FailureDetectorModule {
    async fn handle(&mut self, msg: DetectorOperationUdp) {
        if self.enabled {
            let DetectorOperationUdp(operation, reply_addr) = msg;
            // let su = self.self_uuid;
            match operation {
                DetectorOperation::HeartbeatRequest => {
                    let msg = DetectorOperation::HeartbeatResponse(self.self_uuid);
                    let buf = bincode::serde::encode_to_vec(&msg, standard()).unwrap();
                    // self.socket.connect(reply_addr).await.unwrap();
                    self.socket.send_to(&buf, reply_addr).await.unwrap();
                },
                DetectorOperation::HeartbeatResponse(uuid) => {
                    // println!("[{su}]: received heartbeat response from uuid {uuid}");
                    self.curr_alive_set.insert(uuid);
                },
                DetectorOperation::AliveRequest => {
                    // println!("[{su}]: received AliveRequest");
                    let mut alive_set = self.prev_alive_set.clone();
                    alive_set.insert(self.self_uuid);
                    let msg = DetectorOperation::AliveInfo(alive_set);
                    let buf = bincode::serde::encode_to_vec(&msg, standard()).unwrap();
                    self.socket.send_to(&buf, reply_addr).await.unwrap();
                }
                _ => { }
            }
        }
    }
}

/// Called periodically to check send broadcast and update alive processes.
#[async_trait::async_trait]
impl Handler<Timeout> for FailureDetectorModule {
    async fn handle(&mut self, _msg: Timeout) {
        if self.enabled {
            // let su = self.self_uuid;
            // unimplemented!("Implement the timeout logic.");
            // println!("[{su}]: period tick");
            
            for uuid in &self.curr_alive_set {
                if !&self.prev_alive_set.contains(uuid) {
                    // println!("[{su}]: process {uuid} no longer suspected as dead");
                    self.timeout_handle.as_mut().unwrap().stop().await;
                    self.delay += self.delta;
                    self.timeout_handle = Some(self.self_ref.request_tick(Timeout, self.delay).await);
                    break; // ????????????????????????????
                }
            }
            
            self.prev_alive_set = mem::take(&mut self.curr_alive_set);

            // sending heartbeat messages to all other processes
            for (_uuid, socket_addr) in &self.other_addresses {
                let msg = DetectorOperation::HeartbeatRequest;
                let buf = bincode::serde::encode_to_vec(&msg, standard()).unwrap();
                self.socket.send_to(&buf, socket_addr).await.unwrap();
            }
        }
    }
}

#[async_trait::async_trait]
impl Handler<Disable> for FailureDetectorModule {
    async fn handle(&mut self, _msg: Disable) {
        self.enabled = false;
    }
}

#[async_trait::async_trait]
impl Handler<Enable> for FailureDetectorModule {
    async fn handle(&mut self, _msg: Enable) {
        self.enabled = true;
    }
}

/// Receives messages over UDP and converts them into our module system's messages
async fn deserialize_and_forward(
    socket: Arc<UdpSocket>,
    module_ref: ModuleRef<FailureDetectorModule>,
) {
    let mut buffer = vec![0];
    while let Ok((len, sender)) = socket.peek_from(&mut buffer).await {
        if len == buffer.len() {
            buffer.resize(2 * buffer.len(), 0);
        } else {
            socket.recv_from(&mut buffer).await.unwrap();
            match bincode::serde::decode_from_slice(&buffer, standard()) {
                Ok((msg, _took)) => module_ref.send(DetectorOperationUdp(msg, sender)).await,
                Err(err) => {
                    debug!("Invalid format of detector operation ({})!", err);
                }
            }
        }
    }
}

/// Received UDP message
struct DetectorOperationUdp(DetectorOperation, SocketAddr);

/// Messages that are sent over UDP
#[derive(Serialize, Deserialize)]
pub enum DetectorOperation {
    /// Request to receive a heartbeat.
    HeartbeatRequest,
    /// Response to heartbeat, contains uuid of the receiver of `HeartbeatRequest`.
    HeartbeatResponse(Uuid),
    /// Request to receive information about working processes.
    AliveRequest,
    /// Vector of processes which are alive according to `AliveRequest` receiver.
    AliveInfo(HashSet<Uuid>),
}
