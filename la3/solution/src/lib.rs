use std::{cmp, sync::Arc};

use module_system::{Handler, ModuleRef, System};

pub use domain::*;
use tokio::task::JoinSet;
use uuid::Uuid;

mod domain;

enum Role {
    Leader,
    Candidate,
    Follower
}

#[non_exhaustive]
pub struct Raft {
    role: Role,
    current_term: u64,
    voted_for: Option<Uuid>,
    log: Vec<LogEntry>,
    commit_index: usize,
    last_applied: u64,

    known_leader: Option<Uuid>,
    module_ref: ModuleRef<Self>,

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
        system.register_module(|module_ref| 
            Raft {
                role: Role::Follower,
                current_term: 0,
                voted_for: None,
                log: Vec::new(),
                commit_index: 0,
                last_applied: 0,
                known_leader: None,
                module_ref,
                config,
                state_machine,
                stable_storage,
                message_sender: message_sender.into(),
            }
        ).await
    }

    async fn broadcast(&self, msg: RaftMessage) {
        let servers = self.config.servers.clone();
        // cloning just the Arc (to the RaftSender which is on the heap (it has unknown concrete type))
        let message_sender = self.message_sender.clone();
        let self_id = self.config.self_id;

        tokio::spawn( async move {
            let mut set = JoinSet::new();
            for uuid in servers {
                if uuid != self_id {
                    let sender = message_sender.clone();
                    let msg = msg.clone();
                    set.spawn(async move {
                        sender.send(&uuid, msg).await;
                    });    
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
            // TODO: handle this - restart election timeout
            // and apply the entries up to the args.leader_commit
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

        // TODO: apply the operation to the state machine to state machine

        self.send_append_entries_response(header.source, true, last_new_entry_index).await;
    }

    async fn handle_command_leader(&mut self, 
        data: Vec<u8>,
        client_id: Uuid,
        sequence_num: u64,
        lowest_sequence_num_without_response: u64
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
        let responder = msg.reply_to;

        match msg.content {
            ClientRequestContent::Command { 
                command, client_id, sequence_num, lowest_sequence_num_without_response 
            } => {
                match self.role {
                    Role::Leader => {
                        self.handle_command_leader(
                            command, client_id, sequence_num, lowest_sequence_num_without_response
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
                        let _ = responder.send(resp);
                    }
                }
            }
            _ => {
                // TODO: maybe reply that we don't handle other client commands
            }
        }
    }
}

#[async_trait::async_trait]
impl Handler<RaftMessage> for Raft {
    async fn handle(&mut self, msg: RaftMessage) {
        if msg.header.term > self.current_term {
            self.current_term = msg.header.term;
            self.voted_for = None;
            self.role = Role::Follower;
        }
        match msg.content {
            RaftMessageContent::AppendEntries(args) => {
                self.handle_append_entries(msg.header, args).await;
            },
            _ => {
                println!("dupa");
            }
        }
    }
}

#[async_trait::async_trait]
impl Handler<ClientRequest> for Raft {
    async fn handle(&mut self, msg: ClientRequest) {
        self.handle_client_request(msg).await;
    }
}

// TODO you can implement handlers of messages of other types for the Raft struct.
