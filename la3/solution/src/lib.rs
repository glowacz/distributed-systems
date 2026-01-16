use std::{cmp, collections::{HashMap, HashSet}, sync::Arc};

use log::{debug, error};
use module_system::{Handler, ModuleRef, System};

pub use domain::*;
use std::ops::RangeInclusive;
use rand::Rng;
use tokio::{sync::mpsc::{Receiver, Sender, UnboundedSender, channel}, task::JoinSet, time::sleep};
use tokio::time::{Duration};
use uuid::Uuid;

mod domain;

#[derive(Debug)]
enum Role {
    Leader,
    Candidate,
    Follower
}

struct ElectionTimeout;
struct HeartbeatTimeout;

#[non_exhaustive]
pub struct Raft {
    role: Role,
    current_term: u64,
    voted_for: Option<Uuid>,
    log: Vec<LogEntry>,
    commit_index: usize,
    last_applied: u64,

    next_index: HashMap<Uuid, usize>,
    match_index: HashMap<Uuid, usize>,
    waiting_channels: HashMap<(Uuid, u64), UnboundedSender<ClientRequestResponse>>,

    known_leader: Option<Uuid>,
    self_ref: ModuleRef<Self>,
    election_reset_tx: Sender<()>,
    received_votes: HashSet<Uuid>,
    send_heartbeats_on_off_tx: Sender<()>,

    config: ServerConfig,
    state_machine: Box<dyn StateMachine>,
    stable_storage: Box<dyn StableStorage>,
    message_sender: Arc<dyn RaftSender>,
    // TODO you can add fields to this struct.
}

impl Raft {
    /// Registers a new `Raft` module in the `system`, initializes it and
    /// returns a `ModuleRef` to it.
    pub async fn new(
        system: &mut System,
        config: ServerConfig,
        state_machine: Box<dyn StateMachine>,
        stable_storage: Box<dyn StableStorage>,
        message_sender: Box<dyn RaftSender>,
    ) -> ModuleRef<Self> {
        let (election_reset_tx, election_reset_rx) = channel::<()>(1000);
        let (send_heartbeats_on_off_tx, send_heartbeats_on_off_rx) = channel::<()>(1000);
        let timeout_range = config.election_timeout_range.clone();
        let heartbeat_timeout = config.heartbeat_timeout;
        
        let self_ref = system.register_module(|self_ref| 
            Raft {
                role: Role::Follower,
                current_term: 0,
                voted_for: None,
                log: Vec::new(),
                commit_index: 0,
                last_applied: 0,
                next_index: HashMap::new(),
                match_index: HashMap::new(),
                waiting_channels: HashMap::new(),
                known_leader: None,
                self_ref,
                election_reset_tx,
                received_votes: HashSet::new(),
                send_heartbeats_on_off_tx,
                config,
                state_machine,
                stable_storage,
                message_sender: message_sender.into(),
            }
        ).await;

        Self::start_and_reset_election_timeout(self_ref.clone(), timeout_range, election_reset_rx).await;
        Self::trigger_heartbeat_timeout(self_ref.clone(), heartbeat_timeout, send_heartbeats_on_off_rx).await;

        self_ref
    }

    async fn broadcast(&self, msg: RaftMessage) {
        let servers = self.config.servers.clone();
        // cloning just the Arc (to the RaftSender which is on the heap (it has unknown concrete type))
        let message_sender = self.message_sender.clone();
        let self_id = self.config.self_id;

        // error!("[{}]: starting broadcast", self_id);

        tokio::spawn( async move {
            // error!("[{}]: spawned broadcast task", self_id);
            let mut set = JoinSet::new();
            for uuid in servers {
                // error!("[{}, broadcast]: in loop for sending to {}", self_id, uuid);
                if uuid != self_id {
                    let sender = message_sender.clone();
                    let msg = msg.clone();
                    set.spawn(async move {
                        // error!("[{}, broadcast]: before sending to {}", self_id, uuid);
                        sender.send(&uuid, msg).await;
                        // error!("[{}, broadcast]: after sending to {}", self_id, uuid);
                    });
                }
            }
            set.join_all().await;
        });
    }

    async fn start_and_reset_election_timeout(self_ref: ModuleRef<Self>, timeout_range: RangeInclusive<Duration>, mut reset_rx: Receiver<()>) {
        // let self_ref = self.self_ref.clone();
        // let timeout_range = self.config.election_timeout_range.clone();

        tokio::spawn(async move {
            loop {
                let timeout = rand::rng().random_range(timeout_range.clone());
                tokio::select! {
                    _ = sleep(timeout) => {
                        let _ = self_ref.send(ElectionTimeout).await;
                    }
                    _ = reset_rx.recv() => {
                        // start new timeout in next loop iteration
                    }
                }
            }
        });
    }

    async fn trigger_heartbeat_timeout(self_ref: ModuleRef<Self>, heartbeat_timeout: Duration, mut on_off_rx: Receiver<()>) {
        tokio::spawn(async move {
            loop {
                on_off_rx.recv().await;

                loop {
                    tokio::select! {
                        biased;
                        
                        _ = on_off_rx.recv() => {
                            break;
                        }
    
                        _ = sleep(heartbeat_timeout) => {
                            self_ref.send(HeartbeatTimeout).await;
                        }
                    }
                }
            }
        });
    }

    async fn send_append_entries_response(&mut self, target: Uuid, success: bool, last_verified_log_index: usize) {
        let response = RaftMessageContent::AppendEntriesResponse(AppendEntriesResponseArgs {
            success,
            last_verified_log_index,
        });
    
        let msg = RaftMessage {
            header: RaftMessageHeader {
                source: self.config.self_id,
                term: self.current_term,
            },
            content: response,
        };
    
        self.message_sender.send(&target, msg).await;
    }

    async fn handle_append_entries(&mut self, header: RaftMessageHeader, args: AppendEntriesArgs) {
        self.known_leader = Some(header.source);

        if header.term < self.current_term {
            self.send_append_entries_response(header.source, false, 0).await;
            return;
        }

        if args.entries.is_empty() {
            let _ = self.election_reset_tx.send(()).await;
            self.apply_committed_entries().await;

            return;
        }

        let log_ok = if args.prev_log_index == 0 {
            true
        }
        else {
            let ind = args.prev_log_index - 1;
            if ind >= self.log.len() {
                false
            } 
            else {
                self.log[ind].term == args.prev_log_term
            }
        };

        if !log_ok {
            self.send_append_entries_response(header.source, false, 0).await;
            return;
        }
        
        for (i, new_entry) in args.entries.iter().enumerate() {
            // prev_log_index is 1-based (log indexes start at 1, bc 0 is the special, initial value)
            // ind is 0-based (position in vector)
            let ind = args.prev_log_index + i;

            if ind < self.log.len() {
                if self.log[ind].term != new_entry.term {
                    self.log.truncate(ind);
                    self.log.push(new_entry.clone());
                }
            } 
            else {
                self.log.push(new_entry.clone());
            }
        }
        
        // 1-based, bc 0 is the special, initial value
        let last_new_entry_index = args.prev_log_index + args.entries.len();

        if args.leader_commit > self.commit_index as usize {
            self.commit_index = cmp::min(args.leader_commit, last_new_entry_index);
        }

        self.send_append_entries_response(header.source, true, last_new_entry_index).await;
    }

    async fn send_append_entries(&mut self, target: Uuid) {
        let next_idx = *self.next_index.get(&target).unwrap_or(&(self.log.len() + 1));
        let prev_log_index = if next_idx > 0 { next_idx - 1 } else { 0 };
        
        let prev_log_term = if prev_log_index == 0 {
            0
        } else {
            // Log is 0-indexed, so entry N is at N-1
            if prev_log_index - 1 < self.log.len() {
                self.log[prev_log_index - 1].term
            } else {
                0 // Should not happen if next_idx is managed correctly relative to log
                // would happen only if we kept sth too big in send_append_entries
            }
        };

        let entries = if prev_log_index < self.log.len() {
             self.log[prev_log_index..].to_vec()
        } else {
            Vec::new()
        };

        let msg = RaftMessage {
            header: RaftMessageHeader {
                source: self.config.self_id,
                term: self.current_term,
            },
            content: RaftMessageContent::AppendEntries(AppendEntriesArgs {
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.commit_index,
            }),
        };

        self.message_sender.send(&target, msg).await;
    }

    async fn handle_append_entries_response(&mut self, header: RaftMessageHeader, args: AppendEntriesResponseArgs) {
        if let Role::Leader = self.role {
            // // If response is from a newer term, step down (handled by generic Handler, but checked here for logic flow)
            // if header.term > self.current_term {
            //     return; 
            // }

            let follower = header.source;

            if args.success {
                // Update match_index and next_index
                self.match_index.insert(follower, args.last_verified_log_index);
                self.next_index.insert(follower, args.last_verified_log_index + 1);

                // Check if we can advance commit index
                self.update_commit_index().await;
            } else {
                // Backoff: decrement next_index and retry
                let current_next = *self.next_index.get(&follower).unwrap_or(&(self.log.len() + 1));
                let new_next = if current_next > 1 { current_next - 1 } else { 1 };
                self.next_index.insert(follower, new_next);
                
                // Retry immediately with the older log entry
                self.send_append_entries(follower).await;
            }
        }
    }

    /// Checks if the commit index can be advanced based on match_index of all servers
    async fn update_commit_index(&mut self) {
        // Iterate from current commit_index + 1 up to last log entry
        // We look for the largest N such that a majority of servers have match_index >= N
        // and log[N].term == current_term
        
        let start = self.commit_index + 1;
        let end = self.log.len();

        for n in (start..=end).rev() {
            let entry_term = self.log[n - 1].term;
            if entry_term != self.current_term {
                continue;
            }

            let mut count = 1; // Count self
            for server in &self.config.servers {
                if *server == self.config.self_id { continue; }
                
                if let Some(&matched) = self.match_index.get(server) {
                    if matched >= n {
                        count += 1;
                    }
                }
            }

            if count > self.config.servers.len() / 2 {
                self.commit_index = n;
                self.apply_committed_entries().await;
                break;
            }
        }
    }

    /// Applies committed entries to the state machine and replies to clients
    async fn apply_committed_entries(&mut self) {
        while self.last_applied < self.commit_index as u64 {
            self.last_applied += 1;
            let idx = (self.last_applied - 1) as usize;
            
            if idx < self.log.len() {
                let entry = &self.log[idx];
                
                // Only commands produce output for clients
                if let LogEntryContent::Command { data, client_id, sequence_num, .. } = &entry.content {
                    let output = self.state_machine.apply(data).await;
                    
                    // Send response if we are holding a channel for this request
                    if let Some(sender) = self.waiting_channels.remove(&(*client_id, *sequence_num)) {
                         let response = ClientRequestResponse::CommandResponse(CommandResponseArgs {
                            client_id: *client_id,
                            sequence_num: *sequence_num,
                            content: CommandResponseContent::CommandApplied { output }
                        });
                        let _ = sender.send(response);
                    }
                }
            }
        }
    }

    async fn send_request_vote_response(&mut self, target: Uuid, vote_granted: bool) {
        let msg = RaftMessage {
            header: RaftMessageHeader {
                source: self.config.self_id,
                term: self.current_term,
            },
            content: RaftMessageContent::RequestVoteResponse(RequestVoteResponseArgs {
                vote_granted,
            }),
        };
        self.message_sender.send(&target, msg).await;
    }

    async fn handle_request_vote(&mut self, header: RaftMessageHeader, args: RequestVoteArgs) {
        if header.term < self.current_term {
            self.send_request_vote_response(header.source, false).await;
            return;
        }

        if let Some(voted_for) = self.voted_for {
            if voted_for != header.source {
                self.send_request_vote_response(header.source, false).await;
                return;
            }
        }

        let my_last_log_ind = self.log.len();
        let my_last_log_term = if my_last_log_ind > 0 {
            self.log[my_last_log_ind - 1].term
        } else {
            0
        };

        let log_up_to_date = if args.last_log_term > my_last_log_term {
            true
        } else if args.last_log_term == my_last_log_term {
            args.last_log_index >= my_last_log_ind
        } else {
            false
        };

        if log_up_to_date {
            self.voted_for = Some(header.source);
            let _ = self.election_reset_tx.send(()).await;
            self.send_request_vote_response(header.source, true).await;
        } else {
            self.send_request_vote_response(header.source, false).await;
        }
    }

    async fn broadcast_heartbeats(&self) {
        let heartbeat_msg = RaftMessage {
            header: RaftMessageHeader { source: self.config.self_id, term: self.current_term },
            content: RaftMessageContent::AppendEntries(
                AppendEntriesArgs { 
                    prev_log_index: self.log.len(), 
                    prev_log_term: if self.log.len() >= 1 { self.log[self.log.len() - 1].term } else { 0 },
                    entries: vec![],
                    leader_commit: self.commit_index 
                }
            )
        };

        self.broadcast(heartbeat_msg).await;
    }

    async fn assert_leadership(&mut self) {
        debug!("[{}]: CONVERTING TO LEADER", self.config.self_id);
        self.role = Role::Leader;
        self.broadcast_heartbeats().await;
        let _ = self.send_heartbeats_on_off_tx.send(()).await;
    }

    async fn handle_vote_response(&mut self, header: RaftMessageHeader, args: RequestVoteResponseArgs) {
        if let Role::Candidate = self.role {
            if args.vote_granted {
                self.received_votes.insert(header.source);

                if self.received_votes.len() > self.config.servers.len() / 2 {
                    self.assert_leadership().await;
                }
            }
        }
    }

    async fn handle_command_leader(&mut self, 
        data: Vec<u8>,
        client_id: Uuid,
        sequence_num: u64,
        lowest_sequence_num_without_response: u64,
        reply_to: UnboundedSender<ClientRequestResponse>
    ) {
        let log_entry = LogEntry {
            content: LogEntryContent::Command { 
                data, 
                client_id, 
                sequence_num, 
                lowest_sequence_num_without_response 
            },
            term: self.current_term,
            timestamp: self.config.system_boot_time.elapsed()
        };

        self.log.push(log_entry.clone());

        self.waiting_channels.insert((client_id, sequence_num), reply_to);

        let msg = RaftMessage { 
            header: RaftMessageHeader { 
                source: self.config.self_id, 
                term: self.current_term 
            }, 
            content: RaftMessageContent::AppendEntries(
                AppendEntriesArgs { 
                    prev_log_index: self.log.len() - 1, 
                    prev_log_term: if self.log.len() >= 2 { self.log[self.log.len() - 2].term} else { 0 },
                    entries: vec![log_entry],
                    leader_commit: self.commit_index 
                }
            )
        };
        
        self.broadcast(msg).await;
    }

    async fn handle_client_request(&mut self, msg: ClientRequest) {
        let reply_to = msg.reply_to;

        match msg.content {
            ClientRequestContent::Command { 
                command, client_id, sequence_num, lowest_sequence_num_without_response 
            } => {
                match self.role {
                    Role::Leader => {
                        self.handle_command_leader(
                            command, client_id, sequence_num, lowest_sequence_num_without_response, reply_to
                        ).await;
                    }
                    _ => {
                        let resp = ClientRequestResponse::CommandResponse(
                            CommandResponseArgs { 
                                client_id, 
                                sequence_num, 
                                content: CommandResponseContent::NotLeader { 
                                    leader_hint: self.known_leader
                                }
                            }
                        );
                        let _ = reply_to.send(resp);
                    }
                }
            }
            _ => {
                // TODO: maybe reply that we don't handle other client commands
            }
        }
    }

    async fn handle_election_timeout(&mut self) {
        debug!("[{}]: CONVERTING FROM {:?} TO CANDIDATE", self.config.self_id, self.role);

        self.received_votes.clear();

        self.current_term += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(self.config.self_id);
        self.received_votes.insert(self.config.self_id);

        // error!("[{}]: before sending election_reset", self.config.self_id);
        let _ = self.election_reset_tx.send(()).await;
        // error!("[{}]: after sending election_reset", self.config.self_id);

        let msg = RaftMessage {
            header: RaftMessageHeader {
                source: self.config.self_id,
                term: self.current_term,
            },
            content: RaftMessageContent::RequestVote(
                RequestVoteArgs { 
                    last_log_index: self.log.len(), 
                    last_log_term: if self.log.len() > 0 { self.log[self.log.len() - 1].term } else { 0 }
                }
            ),
        };

        // error!("[{}]: before sending election broadcast", self.config.self_id);
        self.broadcast(msg).await;
    }
}

#[async_trait::async_trait]
impl Handler<RaftMessage> for Raft {
    async fn handle(&mut self, msg: RaftMessage) {
        if msg.header.term > self.current_term {
            self.current_term = msg.header.term;
            self.voted_for = None;
            if let Role::Leader = self.role {
                let _ = self.send_heartbeats_on_off_tx.send(()).await;
            }
            self.role = Role::Follower;

            debug!("[{}]: CONVERTING TO FOLLOWER", self.config.self_id);
        }
        match msg.content {
            RaftMessageContent::AppendEntries(args) => {
                debug!("[{}]: got AppendEntries from {}", self.config.self_id, msg.header.source);
                self.handle_append_entries(msg.header, args).await;
            },
            RaftMessageContent::AppendEntriesResponse(args) => {
                debug!("[{}]: got AppendEntriesResponse from {}, success: {}", 
                    self.config.self_id, msg.header.source, args.success);
                self.handle_append_entries_response(msg.header, args).await;
            },
            RaftMessageContent::RequestVote(args) => {
                debug!("[{}]: got RequestVote from {}", self.config.self_id, msg.header.source);
                self.handle_request_vote(msg.header, args).await;
            },
            RaftMessageContent::RequestVoteResponse(args) => {
                debug!("[{}]: got RequestVoteResponse from {}, vote_granted: {}", 
                    self.config.self_id, msg.header.source, args.vote_granted);
                self.handle_vote_response(msg.header, args).await;
            }
            _ => {
                println!("dupa\n\n\n");
            }
        }
    }
}

#[async_trait::async_trait]
impl Handler<ClientRequest> for Raft {
    async fn handle(&mut self, msg: ClientRequest) {
        if let ClientRequestContent::Command { command: _, client_id, sequence_num, lowest_sequence_num_without_response: _ } = msg.content {
            debug!("[{}]: got client request from {}, seq num {}\n\n\n", 
             self.config.self_id, client_id, sequence_num);
        } else {
            debug!("[{}]: got OTHER CLIENT REQUEST\n\n\n", self.config.self_id);
        }
        
        self.handle_client_request(msg).await;
    }
}

#[async_trait::async_trait]
impl Handler<ElectionTimeout> for Raft {
    async fn handle(&mut self, _msg: ElectionTimeout) {
        if let Role::Leader = self.role { } else {
            debug!("[{}]: starting election", self.config.self_id);
            self.handle_election_timeout().await;
        }
    }
}

// for Heartbeats it is handled nicely and we shouldn't even get this HeartbeatTimeout message as leader
// but for ElectionTimeout, a leader will just ignore it like above
// this should still work, so I won't change it for now

#[async_trait::async_trait]
impl Handler<HeartbeatTimeout> for Raft {
    async fn handle(&mut self, _msg: HeartbeatTimeout) {
        if let Role::Leader = self.role {
            self.broadcast_heartbeats().await;
        }
    }
}

// TODO you can implement handlers of messages of other types for the Raft struct.
