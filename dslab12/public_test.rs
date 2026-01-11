#[cfg(test)]
pub(crate) mod tests {
    use module_system::{Handler, ModuleRef, System};
    use ntest::timeout;
    use std::convert::TryInto;
    use tokio::sync::mpsc::{
        UnboundedReceiver as Receiver, UnboundedSender as Sender, unbounded_channel as unbounded,
    };
    use tokio::time::{Duration, sleep};

    use crate::domain::{
        Action, Edit, EditRequest, EditorClient, Operation, ReliableBroadcast, ReliableBroadcastRef,
    };
    use crate::solution::Process;

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[timeout(200)]
    async fn two_processes_one_issues_one_edit() {
        const NUM_PROCESSES: usize = 2;
        const TEXT: &str = "";

        let mut system = System::new().await;
        let (mut b, mut c) = build_system::<NUM_PROCESSES>(&mut system, TEXT).await;

        // P0, edit0:
        c[0].request(Action::Insert { idx: 0, ch: 'i' }).await;
        assert_eq!(
            c[0].receive().await,
            (Action::Insert { idx: 0, ch: 'i' }, "i")
        );

        // P0, edit0 -> P1:
        b.forward(0, 1).await;
        assert_eq!(c[1].receive().await, (Action::Nop, ""));
        assert_eq!(
            c[1].receive().await,
            (Action::Insert { idx: 0, ch: 'i' }, "i")
        );

        // P1, NOP -> P0:
        b.forward(1, 0).await;
        assert_eq!(c[0].receive().await, (Action::Nop, "i"));

        system.shutdown().await;
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[timeout(200)]
    async fn example_from_diagram() {
        const NUM_PROCESSES: usize = 2;
        const TEXT: &str = "";

        let mut system = System::new().await;
        let (mut b, mut c) = build_system::<NUM_PROCESSES>(&mut system, TEXT).await;

        // P0, edit0:
        c[0].request(Action::Insert { idx: 0, ch: 'i' }).await;
        assert_eq!(
            c[0].receive().await,
            (Action::Insert { idx: 0, ch: 'i' }, "i")
        );

        // P0, edit0 -> P1:
        b.forward(0, 1).await;
        assert_eq!(c[1].receive().await, (Action::Nop, ""));
        assert_eq!(
            c[1].receive().await,
            (Action::Insert { idx: 0, ch: 'i' }, "i")
        );

        // P1, NOP -> P0:
        b.forward(1, 0).await;
        assert_eq!(c[0].receive().await, (Action::Nop, "i"));

        // P0, edit1:
        c[0].request(Action::Insert { idx: 0, ch: 'H' }).await;
        assert_eq!(
            c[0].receive().await,
            (Action::Insert { idx: 0, ch: 'H' }, "Hi")
        );

        // P1, edit1:
        c[1].request(Action::Insert { idx: 1, ch: '!' }).await;
        assert_eq!(
            c[1].receive().await,
            (Action::Insert { idx: 1, ch: '!' }, "i!")
        );

        // P0, edit1 -> P1:
        b.forward(0, 1).await;
        assert_eq!(
            c[1].receive().await,
            (Action::Insert { idx: 0, ch: 'H' }, "Hi!")
        );

        // P1, edit1 -> P0:
        b.forward(1, 0).await;
        assert_eq!(
            c[0].receive().await,
            (Action::Insert { idx: 2, ch: '!' }, "Hi!")
        );

        system.shutdown().await;
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[timeout(200)]
    async fn two_processes_each_issues_two_edits() {
        const NUM_PROCESSES: usize = 2;
        const TEXT: &str = "";

        let mut system = System::new().await;
        let (mut b, mut c) = build_system::<NUM_PROCESSES>(&mut system, TEXT).await;

        // P0, edit0:
        c[0].request(Action::Insert { idx: 0, ch: '0' }).await;
        assert_eq!(
            c[0].receive().await,
            (Action::Insert { idx: 0, ch: '0' }, "0")
        );

        // P1, edit0:
        c[1].request(Action::Insert { idx: 0, ch: '1' }).await;
        assert_eq!(
            c[1].receive().await,
            (Action::Insert { idx: 0, ch: '1' }, "1")
        );

        // P1, edit0 -> P0:
        b.forward(1, 0).await;
        assert_eq!(
            c[0].receive().await,
            (Action::Insert { idx: 1, ch: '1' }, "01")
        );

        // P0, edit0 -> P1:
        b.forward(0, 1).await;
        assert_eq!(
            c[1].receive().await,
            (Action::Insert { idx: 0, ch: '0' }, "01")
        );

        // P1, edit1:
        c[1].request(Action::Insert { idx: 1, ch: '2' }).await;
        assert_eq!(
            c[1].receive().await,
            (Action::Insert { idx: 1, ch: '2' }, "021")
        );

        // P0, edit1:
        c[0].request(Action::Insert { idx: 1, ch: '3' }).await;
        assert_eq!(
            c[0].receive().await,
            (Action::Insert { idx: 1, ch: '3' }, "031")
        );

        // P0, edit1 -> P1:
        b.forward(0, 1).await;
        assert_eq!(
            c[1].receive().await,
            (Action::Insert { idx: 1, ch: '3' }, "0321")
        );

        // P1, edit1 -> P0:
        b.forward(1, 0).await;
        assert_eq!(
            c[0].receive().await,
            (Action::Insert { idx: 2, ch: '2' }, "0321")
        );

        system.shutdown().await;
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[timeout(200)]
    async fn two_processes_each_issues_two_edits_at_once() {
        const NUM_PROCESSES: usize = 2;
        const TEXT: &str = "x";

        let mut system = System::new().await;
        let (mut b, mut c) = build_system::<NUM_PROCESSES>(&mut system, TEXT).await;

        // P0, edit0 & edit1:
        c[0].request(Action::Insert { idx: 0, ch: '0' }).await;
        c[0].request(Action::Insert { idx: 1, ch: '1' }).await;
        assert_eq!(
            c[0].receive().await,
            (Action::Insert { idx: 0, ch: '0' }, "0x")
        );
        assert!(c[0].no_receive().await);

        // P1, edit0 & edit1:
        c[1].request(Action::Insert { idx: 1, ch: '2' }).await;
        c[1].request(Action::Insert { idx: 0, ch: '3' }).await;
        assert_eq!(
            c[1].receive().await,
            (Action::Insert { idx: 1, ch: '2' }, "x2")
        );
        assert!(c[1].no_receive().await);

        // P1, edit0 -> P0:
        b.forward(1, 0).await;
        assert!(b.no_forward(1, 0).await);
        assert_eq!(
            c[0].receive().await,
            (Action::Insert { idx: 2, ch: '2' }, "0x2")
        );

        // P0, edit0 -> P1:
        b.forward(0, 1).await;
        assert_eq!(
            c[1].receive().await,
            (Action::Insert { idx: 0, ch: '0' }, "0x2")
        );

        // P0, edit1 -> client:
        assert_eq!(
            c[0].receive().await,
            (Action::Insert { idx: 3, ch: '1' }, "0x21")
        );

        // P1, edit1 -> client:
        assert_eq!(
            c[1].receive().await,
            (Action::Insert { idx: 1, ch: '3' }, "03x2")
        );

        // P1, edit1 -> P0:
        b.forward(1, 0).await;
        assert_eq!(
            c[0].receive().await,
            (Action::Insert { idx: 1, ch: '3' }, "03x21")
        );

        // P0, edit1 -> P1:
        b.forward(0, 1).await;
        assert_eq!(
            c[1].receive().await,
            (Action::Insert { idx: 4, ch: '1' }, "03x21")
        );

        system.shutdown().await;
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[timeout(200)]
    async fn three_processes_each_issues_one_edit() {
        const NUM_PROCESSES: usize = 3;
        const TEXT: &str = "";

        let mut system = System::new().await;
        let (mut b, mut c) = build_system::<NUM_PROCESSES>(&mut system, TEXT).await;

        // P0, edit0:
        c[0].request(Action::Insert { idx: 0, ch: '0' }).await;
        assert_eq!(
            c[0].receive().await,
            (Action::Insert { idx: 0, ch: '0' }, "0")
        );

        // P1, edit0:
        c[1].request(Action::Insert { idx: 0, ch: '1' }).await;
        assert_eq!(
            c[1].receive().await,
            (Action::Insert { idx: 0, ch: '1' }, "1")
        );

        // P2, edit0:
        c[2].request(Action::Insert { idx: 0, ch: '2' }).await;
        assert_eq!(
            c[2].receive().await,
            (Action::Insert { idx: 0, ch: '2' }, "2")
        );

        // P1, edit0 -> P0:
        b.forward(1, 0).await;
        assert_eq!(
            c[0].receive().await,
            (Action::Insert { idx: 1, ch: '1' }, "01")
        );

        // P2, edit0 -> P0:
        b.forward(2, 0).await;
        assert_eq!(
            c[0].receive().await,
            (Action::Insert { idx: 2, ch: '2' }, "012")
        );

        // P2, edit0 -> P1:
        b.forward(2, 1).await;
        assert_eq!(
            c[1].receive().await,
            (Action::Insert { idx: 1, ch: '2' }, "12")
        );

        // P0, edit0 -> P1:
        b.forward(0, 1).await;
        assert_eq!(
            c[1].receive().await,
            (Action::Insert { idx: 0, ch: '0' }, "012")
        );

        // P0, edit0 -> P2:
        b.forward(0, 2).await;
        assert_eq!(
            c[2].receive().await,
            (Action::Insert { idx: 0, ch: '0' }, "02")
        );

        // P1, edit0 -> P2:
        b.forward(1, 2).await;
        assert_eq!(
            c[2].receive().await,
            (Action::Insert { idx: 1, ch: '1' }, "012")
        );

        system.shutdown().await;
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[timeout(200)]
    async fn transformations_insert() {
        const NUM_PROCESSES: usize = 2;
        const TEXT: &str = "0";

        let mut system = System::new().await;
        let (mut b, mut c) = build_system::<NUM_PROCESSES>(&mut system, TEXT).await;

        c[0].request(Action::Insert { idx: 0, ch: 'a' }).await;
        assert_eq!(
            c[0].receive().await,
            (Action::Insert { idx: 0, ch: 'a' }, "a0")
        );

        c[1].request(Action::Insert { idx: 1, ch: 'b' }).await;
        assert_eq!(
            c[1].receive().await,
            (Action::Insert { idx: 1, ch: 'b' }, "0b")
        );

        // else: insert(p1 + 1, c1, r1)
        b.forward(1, 0).await;
        assert_eq!(
            c[0].receive().await,
            (Action::Insert { idx: 2, ch: 'b' }, "a0b")
        );

        // if p1 < p2: insert(p1, c1, r1)
        b.forward(0, 1).await;
        assert_eq!(
            c[1].receive().await,
            (Action::Insert { idx: 0, ch: 'a' }, "a0b")
        );

        c[0].request(Action::Insert { idx: 1, ch: 'c' }).await;
        assert_eq!(
            c[0].receive().await,
            (Action::Insert { idx: 1, ch: 'c' }, "ac0b")
        );

        c[1].request(Action::Insert { idx: 1, ch: 'd' }).await;
        assert_eq!(
            c[1].receive().await,
            (Action::Insert { idx: 1, ch: 'd' }, "ad0b")
        );

        // else: insert(p1 + 1, c1, r1)
        b.forward(1, 0).await;
        assert_eq!(
            c[0].receive().await,
            (Action::Insert { idx: 2, ch: 'd' }, "acd0b")
        );

        // if p1 = p2 and r1 < r2: insert(p1, c1, r1)
        b.forward(0, 1).await;
        assert_eq!(
            c[1].receive().await,
            (Action::Insert { idx: 1, ch: 'c' }, "acd0b")
        );

        system.shutdown().await;
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[timeout(500)]
    async fn buffer_future_round_ops() {
        // Scenario: 2 Processes.
        // Round 1: P0 issues 'A', P1 issues 'B'.
        // P0 finishes Round 1 quickly and starts Round 2 (issues 'C').
        // P1 is slow to receive P0's Round 1 msg, but receives P0's Round 2 msg first.
        // P1 must buffer 'C', wait for 'A', finish Round 1, then process 'C'.

        const NUM_PROCESSES: usize = 2;
        const TEXT: &str = "";

        let mut system = System::new().await;
        let (mut b, mut c) = build_system::<NUM_PROCESSES>(&mut system, TEXT).await;

        // --- Round 1 Start ---
        // P0 requests 'A'
        c[0].request(Action::Insert { idx: 0, ch: 'A' }).await;
        assert_eq!(c[0].receive().await, (Action::Insert { idx: 0, ch: 'A' }, "A"));

        // P1 requests 'B'
        c[1].request(Action::Insert { idx: 0, ch: 'B' }).await;
        assert_eq!(c[1].receive().await, (Action::Insert { idx: 0, ch: 'B' }, "B"));

        // P1 -> P0 (Round 1 delivery)
        // P0 receives 'B', transforms wrt 'A' (concurrent). 
        // P1 inserts at 0, P0 inserted at 0. P0 rank < P1 rank? 0 < 1. 
        // P0 keeps 'A' at 0. P1's 'B' -> Insert(1, 'B').
        b.forward(1, 0).await;
        assert_eq!(c[0].receive().await, (Action::Insert { idx: 1, ch: 'B' }, "AB"));
        
        // P0 is now done with Round 1.

        // --- Round 2 Start (P0 only) ---
        // P0 requests 'C'.
        c[0].request(Action::Insert { idx: 2, ch: 'C' }).await;
        // P0 applies 'C' immediately (start of Round 2 for P0).
        assert_eq!(c[0].receive().await, (Action::Insert { idx: 2, ch: 'C' }, "ABC"));

        // --- Out of Order Delivery to P1 ---
        // P1 has NOT received 'A' from Round 1 yet.
        // We deliver P0's 'C' (Round 2) to P1 first.
        // Note: forward(0, 1) usually delivers oldest first, but we want to simulate
        // the *processing* logic. Since the broadcast module guarantees FIFO per sender,
        // we can't physically deliver Round 2 before Round 1 from the *same* sender 
        // via the channel. However, we can simulate the "wait" by having P1 receive 
        // P0's Round 1 op, but NOT finish the round (if there were a 3rd process).
        
        // Actually, with 2 processes, receiving P0's R1 op finishes the round. 
        // To test buffering effectively with FIFO channels, we need to verify P1 
        // doesn't process R2 messages if it's waiting for *another* process in R1.
        // Let's switch to 3 Processes.
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[timeout(500)]
    async fn buffer_future_round_ops_3_nodes() {
        const NUM_PROCESSES: usize = 3;
        const TEXT: &str = "";

        let mut system = System::new().await;
        let (mut b, mut c) = build_system::<NUM_PROCESSES>(&mut system, TEXT).await;

        // --- Round 1 ---
        // P0 issues '0', P1 issues '1', P2 issues '2'
        c[0].request(Action::Insert { idx: 0, ch: '0' }).await;
        c[1].request(Action::Insert { idx: 0, ch: '1' }).await;
        c[2].request(Action::Insert { idx: 0, ch: '2' }).await;
        
        // Local applies
        c[0].receive().await; // "0"
        c[1].receive().await; // "1"
        c[2].receive().await; // "2"

        // P0 finishes Round 1 completely
        b.forward(1, 0).await; // P0 gets '1'
        b.forward(2, 0).await; // P0 gets '2'
        // P0 state: "012" (assuming standard order logic)
        // P0 consumes the two external edits
        c[0].receive().await; 
        c[0].receive().await; 

        // P1 receives P0's Round 1 msg
        b.forward(0, 1).await;
        c[1].receive().await; 
        // P1 is still waiting for P2's Round 1 msg to finish Round 1.

        // --- Round 2 Start (P0) ---
        // P0 issues 'X' (Round 2)
        c[0].request(Action::Insert { idx: 0, ch: 'X' }).await;
        c[0].receive().await; // P0 has applied X

        // Deliver P0's Round 2 msg ('X') to P1.
        // P1 is still in Round 1 (waiting for P2).
        b.forward(0, 1).await; 
        
        // ASSERT: P1 should NOT output 'X' yet.
        assert!(c[1].no_receive().await);

        // Now deliver P2's Round 1 msg to P1.
        b.forward(2, 1).await;
        
        // P1 should now finish Round 1 (output P2's op) AND automatically process 
        // the buffered Round 2 msg from P0.
        
        // 1. P1 receives P2's '2' (Round 1)
        let (act1, _) = c[1].receive().await;
        // 2. P1 receives P0's 'X' (Round 2). 
        // Note: Since P1 didn't issue an op for R2, it generates internal NOP, 
        // then processes 'X'. Client receives NOP then X.
        let (act2, _) = c[1].receive().await; // NOP
        let (act3, _) = c[1].receive().await; // X

        assert_eq!(act2, Action::Nop);
        match act3 {
            Action::Insert { ch: 'X', .. } => {},
            _ => panic!("Expected Insert X from buffered round 2, got {:?}", act3),
        }

        system.shutdown().await;
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[timeout(500)]
    async fn client_lag_transformation() {
        // Scenario:
        // P0 and P1.
        // P1 issues 'B' at index 0. Broadcasts.
        // P0 receives 'B'. P0 applies 'B'. Log size becomes 1 (NOP from P0 + B? Or just B?)
        // Wait, if P0 hasn't started round, receiving B triggers R1 with P0=NOP.
        // 
        // Then P0's client (who still thinks state is empty) sends 'A' at index 0.
        // P0 process must transform 'A' (wr.t. NOP and B) before applying.
        
        const NUM_PROCESSES: usize = 2;
        const TEXT: &str = "";

        let mut system = System::new().await;
        let (mut b, mut c) = build_system::<NUM_PROCESSES>(&mut system, TEXT).await;

        // 1. P1 issues 'B' at 0.
        c[1].request(Action::Insert { idx: 0, ch: 'B' }).await;
        c[1].receive().await; // P1 applied B

        // 2. Deliver P1 -> P0.
        // P0 has no current op. It should generate NOP, apply it, then apply P1's 'B'.
        b.forward(1, 0).await;
        
        // Verify P0 state update
        assert_eq!(c[0].receive().await, (Action::Nop, ""));
        assert_eq!(c[0].receive().await, (Action::Insert { idx: 0, ch: 'B' }, "B"));

        // 3. P0's client requests 'A' at 0.
        // Crucially, the test helper `request` uses `num_applied` from the client struct.
        // The client struct has processed 2 edits (Nop, B), so `num_applied` is 2.
        // WE WANT TO SIMULATE LAG. We need to cheat or manually send a stale request.
        // Since we can't easily hack ControllableClient, we will use the fact that
        // the client updates `num_applied` only when we call `receive`.
        
        // Let's reset the scenario to properly simulate lag.
        system.shutdown().await;
        
        let mut system = System::new().await;
        let (mut b, mut c) = build_system::<NUM_PROCESSES>(&mut system, TEXT).await;

        // Step 1: P1 issues 'B'.
        c[1].request(Action::Insert { idx: 0, ch: 'B' }).await;
        c[1].receive().await;

        // Step 2: Deliver to P0. P0 processes it.
        b.forward(1, 0).await;
        // IMPORTANT: We do NOT call c[0].receive().
        // P0 process has applied NOP and Insert(B). Log size = 2.
        // P0 client `num_applied` is still 0.

        // Step 3: P0 client requests 'A' at 0.
        // This request sends num_applied = 0.
        c[0].request(Action::Insert { idx: 0, ch: 'A' }).await;

        // The process receives request (seq 0). It sees its log has 2 entries.
        // It must transform 'A' against those 2 entries.
        // 1. wrt NOP: no change.
        // 2. wrt Insert(0, 'B', rank 1).
        //    Client temp rank = N+1 = 3.
        //    Rule: insert(p1, ..) wrt insert(p2, ..). p1=0, p2=0.
        //    p1 == p2, r1(3) > r2(1).
        //    Branch: else insert(p1+1, ...)
        //    Result: Insert(1, 'A').
        
        // Now we drain P0's client queue.
        // 1. NOP (internal R1)
        assert_eq!(c[0].receive().await, (Action::Nop, ""));
        // 2. Insert B (from P1 R1)
        assert_eq!(c[0].receive().await, (Action::Insert { idx: 0, ch: 'B' }, "B"));
        // 3. The transformed request 'A' (R2)
        assert_eq!(c[0].receive().await, (Action::Insert { idx: 1, ch: 'A' }, "BA"));

        system.shutdown().await;
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[timeout(500)]
    async fn complex_insert_delete_convergence() {
        const NUM_PROCESSES: usize = 2;
        // Start with some text
        const TEXT: &str = "abc";

        let mut system = System::new().await;
        let (mut b, mut c) = build_system::<NUM_PROCESSES>(&mut system, TEXT).await;

        // P0 wants to Delete 'b' (index 1).
        c[0].request(Action::Delete { idx: 1 }).await;
        
        // P1 wants to Insert 'x' at index 1.
        c[1].request(Action::Insert { idx: 1, ch: 'x' }).await;

        // Both apply locally.
        assert_eq!(c[0].receive().await, (Action::Delete { idx: 1 }, "ac"));
        assert_eq!(c[1].receive().await, (Action::Insert { idx: 1, ch: 'x' }, "axbc"));

        // Exchange
        b.forward(0, 1).await;
        b.forward(1, 0).await;

        // P0 receives Insert(1, 'x') from P1.
        // Transform Insert(1, 'x') wrt Delete(1) (local op).
        // Rule: Insert(p1) wrt Delete(p2). 
        // p1=1, p2=1. p1 <= p2 is True.
        // Result: Insert(1, 'x').
        assert_eq!(c[0].receive().await, (Action::Insert { idx: 1, ch: 'x' }, "axc"));

        // P1 receives Delete(1) from P0.
        // Transform Delete(1) wrt Insert(1, 'x') (local op).
        // Rule: Delete(p1) wrt Insert(p2).
        // p1=1, p2=1. p1 < p2 is False.
        // Result: Delete(p1 + 1) -> Delete(2).
        // Deleting index 2 in "axbc" ('b').
        assert_eq!(c[1].receive().await, (Action::Delete { idx: 2 }, "axc"));

        // Final convergence check
        // P0: "axc", P1: "axc".

        // --- ROUND 2: Conflict causing index shift ---
        // P0: Insert 'Y' at 0.
        // P1: Delete at 0.
        c[0].request(Action::Insert { idx: 0, ch: 'Y' }).await;
        c[1].request(Action::Delete { idx: 0 }).await;

        c[0].receive().await; // "Yaxc"
        c[1].receive().await; // "xc" (deleted 'a')

        b.forward(0, 1).await;
        b.forward(1, 0).await;

        // P0 processes Delete(0) from P1.
        // Transform Delete(0) wrt Insert(0, 'Y').
        // Rule: Delete(0) wrt Insert(0). 0 < 0 False.
        // Result: Delete(1).
        // "Yaxc" delete 1 ('a') -> "Yxc".
        assert_eq!(c[0].receive().await, (Action::Delete { idx: 1 }, "Yxc"));

        // P1 processes Insert(0, 'Y') from P0.
        // Transform Insert(0, 'Y') wrt Delete(0).
        // Rule: Insert(0) wrt Delete(0). 0 <= 0 True.
        // Result: Insert(0, 'Y').
        // "xc" insert Y at 0 -> "Yxc".
        assert_eq!(c[1].receive().await, (Action::Insert { idx: 0, ch: 'Y' }, "Yxc"));

        system.shutdown().await;
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[timeout(500)]
    async fn nop_generation_and_indexing() {
        const NUM_PROCESSES: usize = 2;
        const TEXT: &str = "123";

        let mut system = System::new().await;
        let (mut b, mut c) = build_system::<NUM_PROCESSES>(&mut system, TEXT).await;

        // P0 Issues Delete(1) -> "13"
        c[0].request(Action::Delete { idx: 1 }).await;
        // P1 does NOTHING (no request).

        // P0 applies local.
        c[0].receive().await; // "13"

        // P1 receives P0's op.
        // P1 has no op. Must gen NOP.
        b.forward(0, 1).await;

        // P1 output: 
        // 1. NOP
        assert_eq!(c[1].receive().await, (Action::Nop, "123"));
        // 2. P0's Delete(1). Transform Delete(1) wrt NOP -> Delete(1).
        assert_eq!(c[1].receive().await, (Action::Delete { idx: 1 }, "13"));

        // P0 needs P1's NOP to finish round?
        // P1 generated NOP internally. Does it broadcast it?
        // Instruction: "issues itself a NOP operation, handles it, and then continues..."
        // "issues" implies: append to log, apply, AND BROADCAST.
        
        // So P1 should have sent a NOP to P0.
        assert!(!b.no_forward(1, 0).await); 
        
        b.forward(1, 0).await;
        
        // P0 receives NOP. Transform NOP wrt Delete(1) -> NOP.
        assert_eq!(c[0].receive().await, (Action::Nop, "13"));

        system.shutdown().await;
    }

    pub(crate) async fn build_system<const N: usize>(
        system: &mut System,
        initial_text: &str,
    ) -> (ControllableBroadcast<N>, [ControllableClient<N>; N]) {
        // Channels for broadcast:
        let (channels_in, channels_out): (Vec<_>, Vec<_>) =
            std::iter::repeat_with(unbounded).take(N * N).unzip();
        let mut senders_iter = channels_in.into_iter();
        let senders: [[Sender<Operation>; N]; N] =
            [[(); N]; N].map(|arr| arr.map(|()| senders_iter.next().unwrap()));
        let mut receivers_iter = channels_out.into_iter();
        let receivers: [[Receiver<Operation>; N]; N] =
            [[(); N]; N].map(|arr| arr.map(|()| receivers_iter.next().unwrap()));

        // Broadcast – module system part:
        let broadcast_module = ControllableBroadcastModule::new(system, senders).await;

        // Clients & processes:
        let mut clients_vec = Vec::new();
        let mut processes_vec = Vec::new();
        for rank in 0..N {
            let (c, p) = build_client_and_process(
                system,
                rank,
                initial_text,
                Box::new(broadcast_module.clone()),
            )
            .await;
            clients_vec.push(c);
            processes_vec.push(p);
        }
        // "unwrap()" without implementing `Debug`:
        let Ok(processes) = processes_vec.try_into() else {
            panic!("Cannot convert processes vec into array!")
        };
        // "unwrap()" without implementing `Debug`:
        let Ok(clients) = clients_vec.try_into() else {
            panic!("Cannot convert clients vec into array!")
        };

        // Broadcast – synchronous part:
        let broadcast = ControllableBroadcast::new(processes, receivers);

        (broadcast, clients)
    }

    async fn build_client_and_process<const N: usize>(
        system: &mut System,
        rank: usize,
        initial_text: &str,
        broadcast: Box<dyn ReliableBroadcastRef<N>>,
    ) -> (ControllableClient<N>, ModuleRef<Process<N>>) {
        let (sender, receiver) = unbounded();
        let client_module = ControllableClientModule::<N>::new(system, sender).await;
        let process = system
            .register_module(|_| Process::new(rank, broadcast, Box::new(client_module)))
            .await;
        let client = ControllableClient::new(initial_text, process.clone(), receiver);
        (client, process)
    }

    /// Client – module system part (module).
    /// Receives messages from the process and forwards to the synchronous part.
    ///
    /// This client consists of two parts: the module system part (module) and
    /// the synchronous part. The parts are connected via async-channel.
    pub(crate) struct ControllableClientModule<const N: usize> {
        to_client: Sender<Edit>,
    }

    impl<const N: usize> EditorClient for ControllableClientModule<N> {}

    impl<const N: usize> ControllableClientModule<N> {
        async fn new(system: &mut System, to_client: Sender<Edit>) -> ModuleRef<Self> {
            let self_ref = system.register_module(|_| Self { to_client }).await;
            self_ref
        }
    }

    #[async_trait::async_trait]
    impl<const N: usize> Handler<Edit> for ControllableClientModule<N> {
        async fn handle(&mut self, msg: Edit) {
            self.to_client.send(msg).unwrap();
        }
    }

    /// Client – synchronous part.
    /// Allows tests to fully control client operations.
    ///
    /// This client consists of two parts: the module system part (module) and
    /// the synchronous part. The parts are connected via async-channel.
    pub(crate) struct ControllableClient<const N: usize> {
        text: String,
        num_applied: usize,
        to_process: ModuleRef<Process<N>>,
        from_module: Receiver<Edit>,
    }

    impl<const N: usize> ControllableClient<N> {
        pub(crate) fn new(
            initial_text: &str,
            to_process: ModuleRef<Process<N>>,
            from_module: Receiver<Edit>,
        ) -> Self {
            Self {
                text: initial_text.to_string(),
                num_applied: 0,
                to_process,
                from_module,
            }
        }

        /// Receive and apply all edits.
        pub(crate) async fn receive_all(&mut self) -> &str {
            while !self.from_module.is_empty() {
                let edit = self.from_module.recv().await.unwrap();
                let action = edit.action;
                action.apply_to(&mut self.text);
                self.num_applied += 1;
            }

            self.text.as_str()
        }

        /// Receive single edit.
        pub(crate) async fn receive(&mut self) -> (Action, &str) {
            let edit = self.from_module.recv().await.unwrap();
            let action = edit.action;
            action.apply_to(&mut self.text);
            self.num_applied += 1;

            (action, self.text.as_str())
        }

        /// Are there no edits to be received?
        pub(crate) async fn no_receive(&mut self) -> bool {
            sleep(Duration::from_millis(200)).await;
            self.from_module.is_empty()
        }

        /// Send edit request to the process.
        pub(crate) async fn request(&self, action: Action) {
            self.to_process
                .send(EditRequest {
                    num_applied: self.num_applied,
                    action,
                })
                .await;
        }
    }

    /// Broadcast – module system part (module).
    /// Receives messages from processes and forwards to the synchronous part.
    ///
    /// This broadcast consists of two parts: the module system part (module)
    /// and the synchronous part. The parts are connected via async-channels.
    struct ControllableBroadcastModule<const N: usize> {
        channels: [[Sender<Operation>; N]; N],
    }

    impl<const N: usize> ReliableBroadcast<N> for ControllableBroadcastModule<N> {}

    impl<const N: usize> ControllableBroadcastModule<N> {
        pub(crate) async fn new(
            system: &mut System,
            channels: [[Sender<Operation>; N]; N],
        ) -> ModuleRef<Self> {
            let self_ref = system.register_module(|_| Self { channels }).await;
            self_ref
        }
    }

    #[async_trait::async_trait]
    impl<const N: usize> Handler<Operation> for ControllableBroadcastModule<N> {
        async fn handle(&mut self, msg: Operation) {
            for i in 0..N {
                if i != msg.process_rank {
                    self.channels[msg.process_rank][i]
                        .send(msg.clone())
                        .unwrap();
                }
            }
        }
    }

    /// Broadcast – synchronous part.
    /// Allows tests to fully control messages broadcasting.
    ///
    /// This broadcast consists of two parts: the module system part (module)
    /// and the synchronous part. The parts are connected via tokio channels.
    pub(crate) struct ControllableBroadcast<const N: usize> {
        processes: [ModuleRef<Process<N>>; N],
        channels: [[Receiver<Operation>; N]; N],
    }

    impl<const N: usize> ControllableBroadcast<N> {
        pub(crate) fn new(
            processes: [ModuleRef<Process<N>>; N],
            channels: [[Receiver<Operation>; N]; N],
        ) -> Self {
            Self {
                processes,
                channels,
            }
        }

        /// Forward single message.
        pub(crate) async fn forward(&mut self, from: usize, to: usize) {
            let msg = self.channels[from][to].recv().await.unwrap();
            self.processes[to].send(msg).await;
        }

        /// Try to forward all possible messages.
        pub(crate) async fn forward_all(&mut self) {
            let mut sth_forwarded = false;

            loop {
                tokio::task::yield_now().await;
                // sleep(Duration::from_millis(200)).await;
                for from in 0..N {
                    for to in 0..N {
                        while !self.channels[from][to].is_empty() {
                            let msg = self.channels[from][to].recv().await.unwrap();
                            self.processes[to].send(msg).await;
                            sth_forwarded = true;
                        }
                    }
                }

                if !sth_forwarded {
                    break;
                }
                sth_forwarded = false;
            }
        }

        /// Are there no messages to be forwarded?
        pub(crate) async fn no_forward(&self, from: usize, to: usize) -> bool {
            sleep(Duration::from_millis(200)).await;
            self.channels[from][to].is_empty()
        }
    }
}
