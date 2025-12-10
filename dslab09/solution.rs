use crate::relay_util::{BoxedModuleSender, ModuleProxy};
use module_system::Handler;
use std::future::Future;
use std::pin::Pin;
use tokio::sync::oneshot::Sender;
use uuid::Uuid;

// As always, you should not modify the public types unless explicitly asked!

#[derive(Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd, Debug)]
pub(crate) enum ProductType {
    Electronics,
    Toys,
    Books,
}

#[derive(Clone)]
pub(crate) struct StoreMsg {
    sender: BoxedModuleSender<DistributedStore>,
    content: StoreMsgContent,
}

#[derive(Clone, Debug)]
pub(crate) enum StoreMsgContent {
    /// Transaction Manager initiates voting for the transaction.
    RequestVote(Transaction),
    /// If every process is ok with transaction, TM issues commit.
    Commit,
    /// System-wide abort.
    Abort,
}

#[derive(Clone)]
pub(crate) struct NodeMsg {
    content: NodeMsgContent,
}

#[derive(Clone, Debug)]
pub(crate) enum NodeMsgContent {
    /// Process replies to TM whether it can/cannot commit the transaction.
    RequestVoteResponse(TwoPhaseResult),
    /// Process acknowledges to TM committing/aborting the transaction.
    FinalizationAck,
}

pub(crate) struct TransactionMessage {
    /// Request to change price.
    pub(crate) transaction: Transaction,

    /// Called after 2PC completes (i.e., the transaction was decided to be
    /// committed/aborted by `DistributedStore`). This must be called after responses
    /// from all processes acknowledging commit or abort are collected.
    #[allow(clippy::type_complexity, reason = "Single use")]
    pub(crate) completed_callback:
        Box<dyn FnOnce(TwoPhaseResult) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum TwoPhaseResult {
    Ok,
    Abort,
}

#[derive(Copy, Clone, Debug)]
pub(crate) struct Product {
    pub(crate) identifier: Uuid,
    pub(crate) pr_type: ProductType,
    pub(crate) price: u64,
}

#[derive(Copy, Clone, Debug)]
pub(crate) struct Transaction {
    pub(crate) pr_type: ProductType,
    pub(crate) shift: i32,
}

#[derive(Debug)]
pub(crate) struct ProductPriceQuery {
    pub(crate) product_ident: Uuid,
    pub(crate) result_sender: Sender<ProductPrice>,
}

#[derive(Copy, Clone, Debug)]
pub(crate) struct ProductPrice(pub(crate) Option<u64>);

/// Message which disables a node. Used for testing.
pub(crate) struct Disable;

/// `DistributedStore`.
/// This structure serves as TM.
pub(crate) struct DistributedStore {
    // Add any fields you need.
    nodes: Vec<BoxedModuleSender<Node>>,
    self_ref: BoxedModuleSender<Self>,
    completed_callback: Option<
        Box<dyn FnOnce(TwoPhaseResult) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>>,
    can_commit_cnt: usize,
    must_abort_cnt: usize,
    committed_cnt: usize,
}

impl DistributedStore {
    pub(crate) fn new(
        nodes: Vec<BoxedModuleSender<Node>>,
        self_ref: BoxedModuleSender<Self>,
    ) -> Self {
        Self { 
            nodes,
            self_ref,
            completed_callback: None,
            can_commit_cnt: 0,
            must_abort_cnt: 0,
            committed_cnt: 0,
        }
    }

    async fn broadcast(&mut self, content: StoreMsgContent) {
        let msg = Box::new(StoreMsg { sender: self.self_ref.clone(), content });
        for nd in self.nodes.clone() {
            nd.send_message(msg.clone()).await;
        }
    }
}

/// Node of `DistributedStore`.
/// This structure serves as a process of the distributed system.
// Add any fields you need.
pub(crate) struct Node {
    products: Vec<Product>,
    pending_transaction: Option<Transaction>,
    enabled: bool,
}

impl Node {
    pub(crate) fn new(products: Vec<Product>) -> Self {
        Self {
            products,
            pending_transaction: None,
            enabled: true,
        }
    }
}

#[async_trait::async_trait]
impl Handler<TransactionMessage> for DistributedStore {
    async fn handle(&mut self, msg: TransactionMessage) {
        self.can_commit_cnt = 0;
        self.must_abort_cnt = 0;
        self.committed_cnt = 0;

        if self.nodes.len() == 0 {
            (msg.completed_callback)(TwoPhaseResult::Ok).await;
            return;
        }

        self.completed_callback = Some(msg.completed_callback);
        self.broadcast(StoreMsgContent::RequestVote(msg.transaction)).await;
    }
}

#[async_trait::async_trait]
impl Handler<NodeMsg> for DistributedStore {
    async fn handle(&mut self, msg: NodeMsg) {
        match msg.content {
            NodeMsgContent::RequestVoteResponse(res) => {
                match res {
                    TwoPhaseResult::Abort => { self.must_abort_cnt += 1 },
                    TwoPhaseResult::Ok => { self.can_commit_cnt += 1 }
                }
                if self.must_abort_cnt + self.can_commit_cnt == self.nodes.len() {
                    // let trans_result = if self.must_abort_cnt == 0 { TwoPhaseResult::Ok } else { TwoPhaseResult::Abort };
                    if self.must_abort_cnt > 0 {
                        if let Some(callback) = self.completed_callback.take() {
                            self.broadcast(StoreMsgContent::Abort).await;
                            callback(TwoPhaseResult::Abort).await;
                        }
                    }
                    else {
                        self.broadcast(StoreMsgContent::Commit).await;
                    }
                }
            },
            NodeMsgContent::FinalizationAck => {
                self.committed_cnt += 1;
                if self.committed_cnt == self.nodes.len() {
                    if let Some(callback) = self.completed_callback.take() {
                        callback(TwoPhaseResult::Ok).await;
                    }    
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl Handler<StoreMsg> for Node {
    async fn handle(&mut self, msg: StoreMsg) {
        if self.enabled {
            let sender = msg.sender;
            match msg.content {
                // 1st phase of 2PC
                StoreMsgContent::RequestVote(transaction) => {
                    self.pending_transaction = Some(transaction);
                    let mut can_commit = true;
                    for product in &self.products {
                        if product.pr_type == transaction.pr_type {
                            match product.price.checked_add_signed(transaction.shift as i64) {
                                Some(res) => {
                                    if res == 0 {
                                        can_commit = false;
                                    }
                                },
                                None => { can_commit = false; },
                            }
                        }
                    }

                    if can_commit {
                        let reply_msg = Box::new(NodeMsg { content: NodeMsgContent::RequestVoteResponse(TwoPhaseResult::Ok)} );
                        sender.send_message(reply_msg).await;
                    }
                    else {
                        let reply_msg = Box::new(NodeMsg { content: NodeMsgContent::RequestVoteResponse(TwoPhaseResult::Abort)} );
                        sender.send_message(reply_msg).await;
                    }
                },
                // TM decided to commit (all processes were available to do so)
                StoreMsgContent::Commit => {
                    let transaction = self.pending_transaction.expect("There should be a transaction to commit");
                    for product in self.products.iter_mut() {
                        if product.pr_type == transaction.pr_type {
                            product.price = product.price.checked_add_signed(transaction.shift as i64).expect("The shift from the transaction results in a negative number, even though all modules declared otherwise");
                        }
                    }

                    let reply_msg = Box::new(NodeMsg { content: NodeMsgContent::FinalizationAck } );
                    sender.send_message(reply_msg).await;
                },
                // TM decided to abort (some process couldn't commit)
                StoreMsgContent::Abort => {
                    self.pending_transaction = None
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl Handler<ProductPriceQuery> for Node {
    async fn handle(&mut self, msg: ProductPriceQuery) {
        if self.enabled {
            for product in &self.products {
                if product.identifier == msg.product_ident {
                    msg.result_sender.send(ProductPrice(Some(product.price))).unwrap();
                    return;
                }
            }
            msg.result_sender.send(ProductPrice(None)).unwrap();
        }
    }
}

#[async_trait::async_trait]
impl Handler<Disable> for Node {
    async fn handle(&mut self, _msg: Disable) {
        self.enabled = false;
    }
}
