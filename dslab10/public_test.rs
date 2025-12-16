#[cfg(test)]
pub(crate) mod tests {
    use log::LevelFilter;
    use ntest::timeout;
    use std::any::Any;
    use std::time::Duration;
    use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
    use uuid::Uuid;

    use crate::relay_util::{BoxedModuleSender, Sendee, SenderTo};
    use crate::solution::{
        Disable, ProcessConfig, Raft, RaftMessage, RaftMessageContent, RaftMessageHeader,
    };
    use crate::{ExecutorSender, RamStorage};
    use module_system::{Handler, ModuleRef, System};

    fn init_logs() {
        let _ = env_logger::builder()
            .is_test(true)
            .filter_level(log::LevelFilter::Debug)
            .try_init();
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[timeout(100)]
    async fn single_process_transitions_to_leader() {
        init_logs();
        // Given:
        let mut system = System::new().await;

        let (tx, mut rx) = unbounded_channel();
        let sender = ExecutorSender::default();
        let self_id = Uuid::new_v4();
        let raft = Raft::new(
            &mut system,
            ProcessConfig {
                self_id,
                election_timeout: Duration::from_millis(200),
                processes_count: 1,
            },
            Box::<RamStorage>::default(),
            Box::new(sender.clone()),
        )
        .await;
        let spy = RaftSpy { raft, tx };
        sender.insert(self_id, Box::new(spy)).await;

        // When:
        tokio::time::sleep(Duration::from_millis(700)).await;
        let msgs = extract_messages(&mut rx);

        // Then:
        assert_has_heartbeat_from_leader(self_id, &msgs);

        system.shutdown().await;
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[timeout(100)]
    async fn new_leader_is_elected_after_crash() {
        init_logs();
        // Given:
        let mut system = System::new().await;

        let (tx, mut rx) = unbounded_channel();
        let sender = ExecutorSender::default();
        let mut ids = vec![];
        let mut rafts = vec![];
        for i in 0..4 {
            let id = Uuid::new_v4();
            ids.push(id);
            let config = ProcessConfig {
                self_id: id,
                election_timeout: Duration::from_millis(200 * (i + 1)),
                processes_count: 4,
            };
            rafts.push(
                Raft::new(
                    &mut system,
                    config,
                    Box::<RamStorage>::default(),
                    Box::new(sender.clone()),
                )
                .await,
            );
        }
        for i in 0..3 {
            sender.insert(ids[i], Box::new(rafts[i].clone())).await;
        }
        let spy = RaftSpy {
            raft: rafts.last().unwrap().clone(),
            tx,
        };
        sender.insert(*ids.last().unwrap(), Box::new(spy)).await;

        // When:
        tokio::time::sleep(Duration::from_millis(400)).await;
        rafts[0].send(Disable).await;
        tokio::time::sleep(Duration::from_millis(600)).await;
        let msgs = extract_messages(&mut rx);
        let heartbeats = heartbeats_from_leader(&msgs);

        // Then:
        let first_leader_heartbeats = heartbeats
            .iter()
            .take_while(|(_, leader_id)| *leader_id == ids[0]);
        assert!(first_leader_heartbeats.clone().count() > 0);
        assert!(
            first_leader_heartbeats
                .clone()
                .all(|(header, _)| header.term == 1)
        );
        let second_leader_heartbeats = heartbeats
            .iter()
            .skip_while(|(_, leader_id)| *leader_id == ids[0]);
        assert!(second_leader_heartbeats.clone().count() > 0);
        assert!(
            second_leader_heartbeats
                .clone()
                .all(|(header, leader_id)| *leader_id == ids[1] && header.term == 2)
        );
        assert_eq!(
            first_leader_heartbeats.count() + second_leader_heartbeats.count(),
            heartbeats.len()
        );

        system.shutdown().await;
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[timeout(100)]
    async fn leader_steps_down_on_higher_term() {
        init_logs();
        // Given: A system with 1 node that becomes leader immediately
        let mut system = System::new().await;
        let (tx, mut rx) = unbounded_channel();
        let sender = ExecutorSender::default();
        let self_id = Uuid::new_v4();
        
        let raft = Raft::new(
            &mut system,
            ProcessConfig {
                self_id,
                election_timeout: Duration::from_millis(200),
                processes_count: 1, // Single node ensures it becomes leader instantly
            },
            Box::<RamStorage>::default(),
            Box::new(sender.clone()),
        )
        .await;

        let spy = RaftSpy { raft: raft.clone(), tx };
        sender.insert(self_id, Box::new(spy)).await;

        // When: Allow it to become leader (Term 1)
        tokio::time::sleep(Duration::from_millis(300)).await;
        let msgs_initial = extract_messages(&mut rx);
        assert_has_heartbeat_from_leader(self_id, &msgs_initial);

        // Inject a Heartbeat from a "fake" new leader with a Higher Term (Term 2)
        let other_leader_id = Uuid::new_v4();
        let higher_term_msg = RaftMessage {
            header: RaftMessageHeader {
                term: 2,
            },
            content: RaftMessageContent::Heartbeat { 
                leader_id: other_leader_id 
            },
        };
        raft.send_message(Box::new(higher_term_msg)).await;

        // Wait a bit. The node should step down.
        // If it steps down, it might eventually timeout and start a Term 3 election,
        // but it should absolutely NOT send any Heartbeats for Term 2.
        tokio::time::sleep(Duration::from_millis(100)).await;
        let msgs_after = extract_messages(&mut rx);

        // Then:
        // Ensure no heartbeats originating from self_id exist with Term 2
        let heartbeats_as_leader = heartbeats_from_leader(&msgs_after);
        let sent_heartbeats_term_2 = heartbeats_as_leader
            .iter()
            .filter(|(header, leader)| *leader == self_id && header.term == 2)
            .count();

        assert_eq!(
            sent_heartbeats_term_2, 0,
            "Node continued acting as leader (or claimed Term 2 leadership) after seeing higher term!"
        );

        system.shutdown().await;
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[timeout(100)]
    async fn no_election_without_quorum() {
        init_logs();
        // Given: A cluster of 3 nodes, but only 1 is running.
        // Quorum is 2. This node should NEVER become leader.
        let mut system = System::new().await;
        let (tx, mut rx) = unbounded_channel();
        let sender = ExecutorSender::default();
        let self_id = Uuid::new_v4();

        let raft = Raft::new(
            &mut system,
            ProcessConfig {
                self_id,
                election_timeout: Duration::from_millis(200),
                processes_count: 3, // Requires 2 votes to win
            },
            Box::<RamStorage>::default(),
            Box::new(sender.clone()),
        )
        .await;
        
        let spy = RaftSpy { raft, tx };
        sender.insert(self_id, Box::new(spy)).await;

        // When: We wait long enough for multiple election timeouts
        tokio::time::sleep(Duration::from_millis(1000)).await;
        let msgs = extract_messages(&mut rx);

        // Then:
        // It should have sent RequestVote messages (candidates try to win)
        let request_votes = msgs.iter().filter(|m| matches!(m.content, RaftMessageContent::RequestVote { .. })).count();
        assert!(request_votes > 0, "Node should try to become candidate and request votes");

        // It should NOT have sent any Heartbeats (meaning it never thought it won)
        let heartbeats = heartbeats_from_leader(&msgs);
        assert_eq!(heartbeats.len(), 0, "Node declared itself leader without quorum!");

        system.shutdown().await;
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[timeout(100)]
    async fn candidate_steps_down_if_leader_detected() {
        init_logs();
        // Given: Node starts, times out, and becomes Candidate (Term 1)
        let mut system = System::new().await;
        let (tx, mut rx) = unbounded_channel();
        let sender = ExecutorSender::default();
        let self_id = Uuid::new_v4();

        let raft = Raft::new(
            &mut system,
            ProcessConfig {
                self_id,
                election_timeout: Duration::from_millis(200),
                processes_count: 3,
            },
            Box::<RamStorage>::default(),
            Box::new(sender.clone()),
        )
        .await;
        
        let spy = RaftSpy { raft: raft.clone(), tx };
        sender.insert(self_id, Box::new(spy)).await;

        // Wait for it to become candidate (200ms timeout)
        tokio::time::sleep(Duration::from_millis(300)).await;
        
        // Clear previous messages (it likely sent RequestVotes)
        let _ = extract_messages(&mut rx);

        // When: Inject a Heartbeat from another valid Leader for the CURRENT term (Term 1)
        // This simulates another node winning the election "at the same time"
        let other_leader = Uuid::new_v4();
        let leader_msg = RaftMessage {
            header: RaftMessageHeader {
                term: 1, // Same term as the candidate is trying for
            },
            content: RaftMessageContent::Heartbeat { 
                leader_id: other_leader 
            },
        };
        raft.send_message(Box::new(leader_msg)).await;

        // Wait a bit
        tokio::time::sleep(Duration::from_millis(100)).await;
        let msgs = extract_messages(&mut rx);

        // Then:
        // 1. It should NOT send further RequestVotes immediately (it should be Follower)
        // 2. It should NOT send Heartbeats (it is not Leader)
        let heartbeats = heartbeats_from_leader(&msgs);
        assert_eq!(heartbeats.len(), 0, "Candidate claimed leadership after seeing valid leader!");
        
        // We can also verify it hasn't incremented term to 2 immediately
        // by checking if it sends RequestVotes for Term 2. 
        // (Assuming standard backoff, it should remain silent for a new election timeout duration)
        
        system.shutdown().await;
    }

    fn assert_has_heartbeat_from_leader(expected_leader: Uuid, msgs: &Vec<RaftMessage>) {
        heartbeats_from_leader(msgs)
            .iter()
            .map(|t| t.1)
            .find(|leader_id| leader_id == &expected_leader)
            .expect("No heartbeat from expected leader!");
    }

    pub(crate) fn heartbeats_from_leader(
        msgs: &Vec<RaftMessage>,
    ) -> Vec<(RaftMessageHeader, Uuid)> {
        let mut res = Vec::new();
        for msg in msgs {
            if let RaftMessageContent::Heartbeat { leader_id } = msg.content {
                res.push((msg.header, leader_id));
            }
        }
        res
    }

    pub(crate) fn extract_messages<T>(rx: &mut UnboundedReceiver<T>) -> Vec<T> {
        let mut msgs = Vec::new();
        while let Ok(msg) = rx.try_recv() {
            msgs.push(msg);
        }
        msgs
    }

    #[derive(Clone)]
    pub struct RaftSpy {
        pub(crate) raft: ModuleRef<Raft>,
        pub(crate) tx: UnboundedSender<RaftMessage>,
    }

    #[async_trait::async_trait]
    impl SenderTo<Raft> for RaftSpy {
        async fn send_message(&self, msg: Box<dyn Sendee<Raft>>) {
            if let Some(msg) = (&*msg as &dyn Any).downcast_ref::<RaftMessage>() {
                let _ = self.tx.send(msg.clone());
            }
            self.raft.send_message(msg).await;
        }

        fn cloned_box(&self) -> BoxedModuleSender<Raft> {
            Box::new(self.clone())
        }
    }
}
