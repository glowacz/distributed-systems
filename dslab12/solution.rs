use std::{collections::HashSet, sync::{atomic::{AtomicI32, AtomicUsize, Ordering}}};

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

    pending_client_cnt: AtomicI32,
    cur_round_first_log_ind: AtomicUsize
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
            pending_client_cnt: AtomicI32::new(0),
            cur_round_first_log_ind: AtomicUsize::new(0)
        }
    }

    // pub fn transform(action: Action, wrt: Action) -> Action {
    //     todo!("Implement transformation logic");
    // }

    fn transform(
        action: Action,
        rank: usize,
        against_action: Action,
        against_rank: usize,
    ) -> Action {
        match (action, against_action) {
            (Action::Insert { idx: p1, ch: c1 }, Action::Insert { idx: p2, .. }) => {
                // [cite: 76]
                if p1 < p2 {
                    Action::Insert { idx: p1, ch: c1 }
                } else if p1 == p2 && rank < against_rank {
                    Action::Insert { idx: p1, ch: c1 }
                } else {
                    Action::Insert { idx: p1 + 1, ch: c1 }
                }
            }
            (Action::Delete { idx: p1 }, Action::Delete { idx: p2 }) => {
                // [cite: 77]
                if p1 < p2 {
                    Action::Delete { idx: p1 }
                } else if p1 == p2 {
                    Action::Nop
                } else {
                    Action::Delete { idx: p1 - 1 }
                }
            }
            (Action::Insert { idx: p1, ch: c1 }, Action::Delete { idx: p2 }) => {
                // [cite: 78]
                if p1 <= p2 {
                    Action::Insert { idx: p1, ch: c1 }
                } else {
                    Action::Insert { idx: p1 - 1, ch: c1 }
                }
            }
            (Action::Delete { idx: p1 }, Action::Insert { idx: p2, .. }) => {
                // [cite: 79]
                if p1 < p2 {
                    Action::Delete { idx: p1 }
                } else {
                    Action::Delete { idx: p1 + 1 }
                }
            }
            // NOP Transformations
            (Action::Nop, _) => Action::Nop,
            (act, Action::Nop) => act,
        }
    }

    // Add any methods you need.
}

#[async_trait::async_trait]
impl<const N: usize> Handler<Operation> for Process<N> {
    async fn handle(&mut self, msg: Operation) {
        while !self.recvd_from.write().await.insert(msg.process_rank.clone()) {
            // this op is from next round (in this round we're processing some 
            // other op from this process), we need to be woken up
            // when the next round begins (it will either be the end of 
            // previous round (if there won't be some message(s) from our client)
            // or in the next round, after handling message from our client)
            let mut wake_up = self.wake_up_process_rx.clone();
            let _ = wake_up.changed().await;
        }

        let mut transformed_action = msg.action.clone();
        let mut log_lock = self.log.lock().await;

        let start_ind = self.cur_round_first_log_ind.load(Ordering::Relaxed);
        for op in &log_lock[start_ind..] {
            transformed_action = Self::transform(transformed_action, msg.process_rank, op.action.clone(), op.process_rank.clone());
        }

        let mut send_nop = false;
        if self.recvd_from.write().await.insert(self.rank) {
            send_nop = true;
            let no_op = Operation {
                process_rank: self.rank,
                action: Action::Nop
            };
            log_lock.push(no_op);
        }

        log_lock.push(msg);

        if self.recvd_from.read().await.len() == N {
            self.recvd_from.write().await.clear();
            self.cur_round_first_log_ind.fetch_add(N, Ordering::Relaxed);

            if self.pending_client_cnt.load(Ordering::Relaxed) > 0 {
                let _ = self.wake_up_client_tx.send(());
            }
            else {
                let _ = self.wake_up_process_tx.send(());
            }
        }

        if send_nop {
            let no_op = Operation {
                process_rank: self.rank,
                action: Action::Nop
            };
            self.broadcast.send(no_op).await;
        }
        self.client.send(Edit { action: transformed_action }).await;
        
    }
}

#[async_trait::async_trait]
impl<const N: usize> Handler<EditRequest> for Process<N> {
    async fn handle(&mut self, request: EditRequest) {
        let _ = self.wake_up_process_tx.send(());

        while !self.recvd_from.write().await.insert(self.rank) {
            // this op is from next round (in this round we're processing some 
            // other op from our client), we need to be woken up
            // when the next round begins
            let mut wake_up = self.wake_up_client_rx.clone();
            let _ = wake_up.changed().await;
        }

        self.pending_client_cnt.fetch_sub(1, Ordering::Relaxed);

        let mut transformed_action = request.action.clone();
        let mut log_lock = self.log.lock().await;

        let start_ind = request.num_applied;
        for op in &log_lock[start_ind..] {
            transformed_action = Self::transform(transformed_action, N+1, op.action.clone(), op.process_rank.clone());
        }

        let op = Operation { 
            process_rank: self.rank, 
            action: request.action.clone()
        };
        log_lock.push(op);

        if self.recvd_from.read().await.len() == N {
            self.recvd_from.write().await.clear();
            self.cur_round_first_log_ind.fetch_add(N, Ordering::Relaxed);

            if self.pending_client_cnt.load(Ordering::Relaxed) > 0 {
                let _ = self.wake_up_client_tx.send(());
            }
            else {
                let _ = self.wake_up_process_tx.send(());
            }
        }

        self.client.send(Edit { action: transformed_action }).await;
    }
}
