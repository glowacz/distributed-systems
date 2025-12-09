#[cfg(test)]
mod tests {
    use crate::relay_util::SenderTo;
    use crate::solution::{
        DistributedStore, Node, Product, ProductPrice, ProductPriceQuery, ProductType, Transaction,
        TransactionMessage, TwoPhaseResult,
    };
    use module_system::{ModuleRef, System};
    use std::time::Duration;
    use tokio::sync::oneshot::channel;
    use uuid::Uuid;

    // Helper to fetch the current price of a product
    async fn send_query(node: &ModuleRef<Node>, product_ident: Uuid) -> ProductPrice {
        let (result_sender, result_receiver) = channel::<ProductPrice>();
        node.send(ProductPriceQuery {
            product_ident,
            result_sender,
        })
        .await;
        result_receiver.await.unwrap()
    }

    #[tokio::test]
    async fn test_three_nodes_commit_and_abort_atomicity() {
        let mut system = System::new().await;

        // --- Setup Data for 3 Nodes ---
        
        // Node 1: Contains a Laptop (Electronics) and a Book
        let laptop_id = Uuid::new_v4();
        let book_id = Uuid::new_v4();
        let products_node_1 = vec![
            Product {
                identifier: laptop_id,
                pr_type: ProductType::Electronics,
                price: 1000,
            },
            Product {
                identifier: book_id,
                pr_type: ProductType::Books,
                price: 50,
            },
        ];

        // Node 2: Contains a Phone (Electronics) and a Cheap Toy
        let phone_id = Uuid::new_v4();
        let cheap_toy_id = Uuid::new_v4();
        let products_node_2 = vec![
            Product {
                identifier: phone_id,
                pr_type: ProductType::Electronics,
                price: 800,
            },
            Product {
                identifier: cheap_toy_id,
                pr_type: ProductType::Toys,
                price: 20, // Low price, susceptible to negative result
            },
        ];

        // Node 3: Contains an Expensive Toy
        let expensive_toy_id = Uuid::new_v4();
        let products_node_3 = vec![
            Product {
                identifier: expensive_toy_id,
                pr_type: ProductType::Toys,
                price: 100,
            },
        ];

        // --- Register Modules ---

        let node_1 = system.register_module(|_| Node::new(products_node_1)).await;
        let node_2 = system.register_module(|_| Node::new(products_node_2)).await;
        let node_3 = system.register_module(|_| Node::new(products_node_3)).await;

        // The DistributedStore communicates with all three nodes
        let shards: Vec<Box<dyn SenderTo<Node>>> = vec![
            Box::new(node_1.clone()),
            Box::new(node_2.clone()),
            Box::new(node_3.clone()),
        ];

        let distributed_store = system
            .register_module(|sr: ModuleRef<DistributedStore>| {
                DistributedStore::new(shards, Box::new(sr))
            })
            .await;

        // --- SCENARIO 1: Successful Distributed Commit ---
        // We increase the price of all Electronics by 100.
        // This should affect Node 1 (Laptop) and Node 2 (Phone), but not Node 3.

        let (tx_success_sender, tx_success_receiver) = channel();
        distributed_store
            .send(TransactionMessage {
                transaction: Transaction {
                    pr_type: ProductType::Electronics,
                    shift: 100,
                },
                completed_callback: Box::new(|result| {
                    Box::pin(async move {
                        tx_success_sender.send(result).unwrap();
                    })
                }),
            })
            .await;

        let result = tx_success_receiver.await.expect("Transaction timed out");
        assert_eq!(result, TwoPhaseResult::Ok, "Expected successful commit for valid price increase");

        // Allow some time for messages to propagate if the implementation is eventually consistent (though 2PC usually locks)
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Verify Updates
        // Laptop (Node 1): 1000 + 100 = 1100
        assert_eq!(send_query(&node_1, laptop_id).await.0.unwrap(), 1100);
        // Phone (Node 2): 800 + 100 = 900
        assert_eq!(send_query(&node_2, phone_id).await.0.unwrap(), 900);
        // Book (Node 1): Unchanged
        assert_eq!(send_query(&node_1, book_id).await.0.unwrap(), 50);

        println!("Scenario 1 (Commit) passed.");

        // --- SCENARIO 2: Distributed Abort (Atomicity Check) ---
        // We attempt to decrease the price of Toys by 30.
        // Node 3 (Expensive Toy): 100 - 30 = 70 (Valid, would vote Commit)
        // Node 2 (Cheap Toy): 20 - 30 = -10 (Invalid <= 0, must vote Abort)
        // Expected Result: The Store should aggregate votes and abort globally.

        let (tx_fail_sender, tx_fail_receiver) = channel();
        distributed_store
            .send(TransactionMessage {
                transaction: Transaction {
                    pr_type: ProductType::Toys,
                    shift: -30,
                },
                completed_callback: Box::new(|result| {
                    Box::pin(async move {
                        tx_fail_sender.send(result).unwrap();
                    })
                }),
            })
            .await;

        let result = tx_fail_receiver.await.expect("Transaction timed out");
        assert_eq!(result, TwoPhaseResult::Abort, "Expected abort because one product price would become non-positive");

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Verify Atomicity (Rollback)
        // The Cheap Toy (Node 2) must remain 20.
        assert_eq!(send_query(&node_2, cheap_toy_id).await.0.unwrap(), 20);
        
        // The Expensive Toy (Node 3) must ALSO remain 100.
        // If Node 3 updated to 70 while Node 2 aborted, Atomicity is broken.
        assert_eq!(send_query(&node_3, expensive_toy_id).await.0.unwrap(), 100);

        println!("Scenario 2 (Abort/Atomicity) passed.");

        // --- SCENARIO 3: Recovery and Successful Commit ---
        // We now attempt a VALID transaction on the same category (Toys) that failed previously.
        // Decrease Toys by 10.
        // Node 2 (Cheap Toy): 20 - 10 = 10 (Valid)
        // Node 3 (Expensive Toy): 100 - 10 = 90 (Valid)
        // This proves the system is not "stuck" after the previous abort.

        let (tx_recovery_sender, tx_recovery_receiver) = channel();
        distributed_store
            .send(TransactionMessage {
                transaction: Transaction {
                    pr_type: ProductType::Toys,
                    shift: -10,
                },
                completed_callback: Box::new(|result| {
                    Box::pin(async move {
                        tx_recovery_sender.send(result).unwrap();
                    })
                }),
            })
            .await;

        let result = tx_recovery_receiver.await.expect("Transaction timed out");
        assert_eq!(result, TwoPhaseResult::Ok, "Expected successful commit after previous abort");

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Verify Updates
        // Cheap Toy (Node 2): 20 - 10 = 10
        assert_eq!(send_query(&node_2, cheap_toy_id).await.0.unwrap(), 10);
        
        // Expensive Toy (Node 3): 1100 - 10 = 90
        assert_eq!(send_query(&node_3, expensive_toy_id).await.0.unwrap(), 90);
        
        // Unrelated Product (Laptop Node 1): Should remain at 1100 (from Scenario 1)
        assert_eq!(send_query(&node_1, laptop_id).await.0.unwrap(), 1100);

        println!("Scenario 3 (Recovery/Commit) passed.");

        system.shutdown().await;
    }
}