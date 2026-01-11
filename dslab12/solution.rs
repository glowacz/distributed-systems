use std::collections::{HashSet, VecDeque};

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
    log: Vec<Operation>,
    recvd_from: HashSet<usize>,
    current_round_start_index: usize,
    pending_from_client: VecDeque<EditRequest>,
    pending_from_others: VecDeque<Operation>,
    future_from_others: VecDeque<Operation>,
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
            pending_from_others: VecDeque::new(),
            future_from_others: VecDeque::new(),
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
                if p1 < *p2 {
                    Action::Insert { idx: p1, ch: c1 }
                } else if p1 == *p2 && r1 < r2 {
                    Action::Insert { idx: p1, ch: c1 }
                } else {
                    Action::Insert { idx: p1 + 1, ch: c1 }
                }
            }
            (Action::Delete { idx: p1 }, Action::Delete { idx: p2 }) => {
                if p1 < *p2 {
                    Action::Delete { idx: p1 }
                } else if p1 == *p2 {
                    Action::Nop
                } else {
                    Action::Delete { idx: p1 - 1 }
                }
            }
            (Action::Insert { idx: p1, ch: c1 }, Action::Delete { idx: p2 }) => {
                if p1 <= *p2 {
                    Action::Insert { idx: p1, ch: c1 }
                } else {
                    Action::Insert { idx: p1 - 1, ch: c1 }
                }
            }
            (Action::Delete { idx: p1 }, Action::Insert { idx: p2, .. }) => {
                if p1 < *p2 {
                    Action::Delete { idx: p1 }
                } else {
                    Action::Delete { idx: p1 + 1 }
                }
            }
            (Action::Nop, _) => Action::Nop,
            (act, Action::Nop) => act,
        }
    }

    async fn process_remote_op(&mut self, mut op: Operation) {
        // println!("Processing remote op {op:?}");

        for other_op in &self.log[self.current_round_start_index..] {
            op.action = Self::transform(op.action, op.process_rank, &other_op.action, other_op.process_rank);
        }

        self.log.push(op.clone());
        // println!("Sending edit to client {:?}", op.action);
        self.client.send(Edit { action: op.action }).await;
        
        self.recvd_from.insert(op.process_rank);
    }

    async fn process_client_request(&mut self, request: EditRequest) {
        // println!("Processing client request {request:?}");

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

        // println!("Sending edit to client {:?}", transformed_action);
        self.client.send(Edit { action: transformed_action }).await;
    }

    async fn process_nop(&mut self) {
        let op = Operation {
            process_rank: self.rank,
            action: Action::Nop
        };
        // println!("Processing NOP");

        self.recvd_from.insert(self.rank);
        self.log.push(op.clone());
        self.broadcast.send(op).await;
        
        // println!("Sending NOP to client");
        self.client.send(Edit { action: Action::Nop }).await;
    }

    async fn do_work(&mut self) {
        // println!("{}: do_work start", self.rank);
        let mut progress;
        // for _i in 0.. {
        loop {
            progress = false;
            // finishing current round and moving to the next one
            if self.recvd_from.len() == N {
                self.recvd_from.clear();
                self.current_round_start_index = self.log.len();
                self.future_from_others.drain(..)
                    .for_each(|op| 
                        self.pending_from_others.push_back(op)
                    );
            }

            // first op in new round
            if self.recvd_from.is_empty() {
                progress = true;
                if let Some(req) = self.pending_from_client.pop_front() {
                    self.process_client_request(req).await;
                }
                else if let Some(op) = self.pending_from_others.pop_front() {
                    self.process_nop().await;
                    self.process_remote_op(op).await;
                }
                else {
                    break;
                }
            }

            while let Some(op) = self.pending_from_others.pop_front() {
                progress = true;
                if self.recvd_from.contains(&op.process_rank) {
                    self.future_from_others.push_back(op);
                }
                else {
                    self.process_remote_op(op).await;
                }
            }

            if !progress {
                break;
            }
        }

        // println!("{}: do_work end", self.rank);
    }
}

#[async_trait::async_trait]
impl<const N: usize> Handler<Operation> for Process<N> {
    async fn handle(&mut self, msg: Operation) {
        // println!("{}: Handling op {:?}", self.rank, msg);
        self.pending_from_others.push_back(msg);
        self.do_work().await;
    }
}

#[async_trait::async_trait]
impl<const N: usize> Handler<EditRequest> for Process<N> {
    async fn handle(&mut self, request: EditRequest) {
        // println!("{}: Handling client request {:?}", self.rank, request);
        self.pending_from_client.push_back(request);
        self.do_work().await;
    }
}
