use std::cmp;

use module_system::{Handler, ModuleRef, System};

pub use domain::*;
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
    commit_index: u64,
    last_applied: u64,

    config: ServerConfig,
    state_machine: Box<dyn StateMachine>,
    stable_storage: Box<dyn StableStorage>,
    message_sender: Box<dyn RaftSender>,
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
        let raft = Raft {
            role: Role::Follower,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            config,
            state_machine,
            stable_storage,
            message_sender,
        };

        system.register_module(|_raft_ref| raft).await
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
        if header.term < self.current_term {
            self.send_append_entries_response(header.source, false, 0).await;
            return;
        }

        if args.entries.is_empty() {
            // TODO: handle this - restart election timeout
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
            self.commit_index = cmp::min(args.leader_commit, last_new_entry_index) as u64;
        }

        // TODO: apply the operation to the state machine to state machine (lastApplied) usually happens asynchronously

        self.send_append_entries_response(header.source, true, last_new_entry_index).await;
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
        todo!()
    }
}

// TODO you can implement handlers of messages of other types for the Raft struct.
