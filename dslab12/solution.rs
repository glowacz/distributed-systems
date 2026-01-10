use std::{collections::HashSet, sync::{Arc, atomic::AtomicUsize}};

use crate::domain::{Action, ClientRef, Edit, EditRequest, Operation, ReliableBroadcastRef};
use module_system::Handler;
use tokio::sync::{Mutex, RwLock, watch::{self, Receiver, Sender}};

impl Operation {
    // Add any methods you need.
}

/// Process of the system.
pub(crate) struct Process<const N: usize> {
    /// Rank of the process.
    rank: usize,
    /// Reference to the broadcast module.
    broadcast: Box<dyn ReliableBroadcastRef<N>>,
    /// Reference to the process's client.
    client: Box<dyn ClientRef>,
    // log of all operations as if 
    // they were issued at the beginning of their round
    // so, the ops from other processes are original
    // and from our client are transformed to match 
    // the beginning of the round
    log: Mutex<Vec<Operation>>,
    recvd_from: RwLock<HashSet<usize>>,

    wake_up_client_tx: Sender<()>,
    wake_up_client_rx: Receiver<()>,
    wake_up_process_tx: Sender<()>,
    wake_up_process_rx: Receiver<()>,

    pending_client_cnt: Arc<AtomicUsize>
}

impl<const N: usize> Process<N> {
    pub(crate) fn new(
        rank: usize,
        broadcast: Box<dyn ReliableBroadcastRef<N>>,
        client: Box<dyn ClientRef>,
    ) -> Self {
        let (wake_up_client_tx, mut wake_up_client_rx) = watch::channel(());
        let (wake_up_process_tx, mut wake_up_process_rx) = watch::channel(());
        wake_up_client_rx.mark_unchanged();
        wake_up_process_rx.mark_unchanged();

        Self {
            rank,
            broadcast,
            client,
            log: Mutex::new(Vec::new()),
            recvd_from: RwLock::new(HashSet::new()),
            wake_up_client_tx,
            wake_up_client_rx,
            wake_up_process_tx,
            wake_up_process_rx,
            pending_client_cnt: Arc::new(AtomicUsize::new(0))
        }
    }

    fn transform(action: Action, wrt: Action) -> Action {
        todo!("Implement transformation logic");
    }

    // Add any methods you need.
}

#[async_trait::async_trait]
impl<const N: usize> Handler<Operation> for Process<N> {
    async fn handle(&mut self, msg: Operation) {
        todo!("Handle operation issued by other process.");
    }
}

#[async_trait::async_trait]
impl<const N: usize> Handler<EditRequest> for Process<N> {
    async fn handle(&mut self, request: EditRequest) {
        todo!("Handle edit request from the client.");
    }
}
