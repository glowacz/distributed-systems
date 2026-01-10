use std::{collections::{HashMap, HashSet, VecDeque}, sync::atomic::Ordering};

use crate::domain::{Action, ClientRef, Edit, EditRequest, Operation, ReliableBroadcastRef};
use module_system::Handler;

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
    log: Vec<Operation>,
    recvd_from: HashSet<usize>,
    current_round_start_index: usize,
    pending_from_client: VecDeque<EditRequest>,
    pending_from_others: HashMap<usize, VecDeque<Operation>>
}

impl<const N: usize> Process<N> {
    pub(crate) fn new(
        rank: usize,
        broadcast: Box<dyn ReliableBroadcastRef<N>>,
        client: Box<dyn ClientRef>,
    ) -> Self {
        Self {
            rank,
            broadcast,
            client,
            log: Vec::new(),
            recvd_from: HashSet::new(),
            current_round_start_index: 0,
            pending_from_client: VecDeque::new(),
            pending_from_others: HashMap::new()
        }
    }

    fn transform(
        action: Action,
        r1: usize,
        wrt: &Action,
        r2: usize,
    ) -> Action {
        match (action, wrt) {
            (Action::Insert { idx: p1, ch: c1 }, Action::Insert { idx: p2, .. }) => {
                // [cite: 76]
                if p1 < *p2 {
                    Action::Insert { idx: p1, ch: c1 }
                } else if p1 == *p2 && r1 < r2 {
                    Action::Insert { idx: p1, ch: c1 }
                } else {
                    Action::Insert { idx: p1 + 1, ch: c1 }
                }
            }
            (Action::Delete { idx: p1 }, Action::Delete { idx: p2 }) => {
                // [cite: 77]
                if p1 < *p2 {
                    Action::Delete { idx: p1 }
                } else if p1 == *p2 {
                    Action::Nop
                } else {
                    Action::Delete { idx: p1 - 1 }
                }
            }
            (Action::Insert { idx: p1, ch: c1 }, Action::Delete { idx: p2 }) => {
                // [cite: 78]
                if p1 <= *p2 {
                    Action::Insert { idx: p1, ch: c1 }
                } else {
                    Action::Insert { idx: p1 - 1, ch: c1 }
                }
            }
            (Action::Delete { idx: p1 }, Action::Insert { idx: p2, .. }) => {
                // [cite: 79]
                if p1 < *p2 {
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

    async fn process_remote_op(&mut self, mut op: Operation) {
        for other_op in &self.log[self.current_round_start_index..] {
            op.action = Self::transform(op.action, op.process_rank, &other_op.action, other_op.process_rank);
        }

        self.log.push(op.clone());
        self.client.send(Edit { action: op.action }).await;
        
        self.recvd_from.insert(op.process_rank);
    }

    async fn process_client_request(&mut self, request: EditRequest) {
        let mut transformed_action = request.action;

        for op in &self.log[request.num_applied..] {
            transformed_action = Self::transform(transformed_action, N + 1, &op.action, op.process_rank)
        }

        let op = Operation {
            process_rank: self.rank,
            action: transformed_action.clone()
        };

        self.recvd_from.insert(self.rank);
        self.log.push(op.clone());
        self.broadcast.send(op).await;
        self.client.send(Edit { action: transformed_action }).await;
    }

    async fn process_nop(&mut self) {
        let op = Operation {
            process_rank: self.rank,
            action: Action::Nop
        };

        self.recvd_from.insert(self.rank);
        self.log.push(op.clone());
        self.broadcast.send(op).await;
        self.client.send(Edit { action: Action::Nop }).await;
    }

    async fn do_work(&mut self) {
        
    }
}

#[async_trait::async_trait]
impl<const N: usize> Handler<Operation> for Process<N> {
    async fn handle(&mut self, msg: Operation) {
        let queue = self.pending_from_others.get_mut(&msg.process_rank).unwrap();
        queue.push_back(msg);
    }
}

#[async_trait::async_trait]
impl<const N: usize> Handler<EditRequest> for Process<N> {
    async fn handle(&mut self, request: EditRequest) {
        self.pending_from_client.push_back(request);
    }
}
