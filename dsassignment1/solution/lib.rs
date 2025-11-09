use std::{collections::VecDeque, sync::Arc};
use tokio::{sync::{Mutex, mpsc::{UnboundedReceiver, UnboundedSender, channel, unbounded_channel}, watch}, time::Duration};
use async_trait::async_trait;

pub trait Message: Send + 'static {}
impl<T: Send + 'static> Message for T {}

pub trait Module: Send + 'static {}
impl<T: Send + 'static> Module for T {}

/// A trait for modules capable of handling messages of type `M`.
#[async_trait::async_trait]
pub trait Handler<M: Message>: Module {
    /// Handles the message.
    async fn handle(&mut self, msg: M);
}

#[async_trait]
trait Handlee<T: Module>: Message {
    async fn get_handled(self: Box<Self>, module: &mut T);
}

#[async_trait]
impl<M: Message, T: Handler<M>> Handlee<T> for M {
    async fn get_handled(self: Box<Self>, module: &mut T) {
        module.handle(*self).await;
    }
}

type SenderForModule<T> = tokio::sync::mpsc::Sender<Box<dyn Handlee<T> + Send>>;
type ReceiverForModule<T> = tokio::sync::mpsc::Receiver<Box<dyn Handlee<T> + Send>>;

/// A handle returned by `ModuleRef::request_tick()` can be used to stop sending further ticks.
#[non_exhaustive]
pub struct TimerHandle {
    // You can add fields to this struct (non_exhaustive makes it SemVer-compatible).
}

impl TimerHandle {
    /// Stops the sending of ticks resulting from the corresponding call to `ModuleRef::request_tick()`.
    /// If the ticks are already stopped, does nothing.
    pub async fn stop(&self) {
        unimplemented!()
    }
}

#[non_exhaustive]
pub struct System {
    // You can add fields to this struct (non_exhaustive makes it SemVer-compatible).
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>
}

impl System {
    /// Registers the module in the system.
    ///
    /// Accepts a closure constructing the module (allowing it to hold a reference to itself).
    /// Returns a `ModuleRef`, which can be used then to send messages to the module.
    pub async fn register_module<T: Module>(
        &mut self,
        module_constructor: impl FnOnce(ModuleRef<T>) -> T,
    ) -> ModuleRef<T> {
        let shutdown_rx = self.shutdown_rx.clone();
        // creating normal (bounded) channel for messages to apply backpressure:
        // if someone tried to send too many messages at once to the module, 
        // at some point they would have to wait for the send operation to complete
        // as some message would have to be read from the channel for the send operation to complete
        // this way we avoid the problem of unbounded channels (from docs):
        // "the process to run out of memory. In this case, the process will be aborted."
        let (msg_tx, mut msg_rx): (SenderForModule<T>, ReceiverForModule<T>) = channel(64);
        let module_ref = ModuleRef {
            msg_tx,
        };
        let mut module = module_constructor(module_ref.clone());
        tokio::spawn(async move {
            loop {
                let shutdown = shutdown_rx.has_changed().unwrap();
                if shutdown {
                    break;
                    // here, the msg_rx should get dropped; so we should handle that where we use msg_tx.send() (in ModuleRef::send)
                    // also, the module should get dropped which is what the task description says
                }
                let msg = msg_rx.recv().await.unwrap();
                msg.get_handled(&mut module).await;
            }
        });
        println!("Module registered");
        module_ref
    }

    /// Creates and starts a new instance of the system.
    pub async fn new() -> Self {
        // unimplemented!()
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        System {
            shutdown_tx,
            shutdown_rx
        }
    }

    /// Gracefully shuts the system down.
    pub async fn shutdown(&mut self) {
        self.shutdown_tx.send(true).unwrap();
    }
}

/// A reference to a module used for sending messages.
// You can add fields to this struct (non_exhaustive makes it SemVer-compatible).
#[non_exhaustive]
// #[derive(Clone)]
pub struct ModuleRef<T: Module>
where
    Self: Send, // As T is Send, with this line we easily SemVer-promise ModuleRef is Send.
{
    // msg_tx: UnboundedSender<Box<dyn Handlee<T>>>,
    msg_tx: SenderForModule<T>,
}

impl<T: Module> ModuleRef<T> {
    /// Sends the message to the module.
    pub async fn send<M: Message>(&self, msg: M)
    where
        T: Handler<M>,
    {
        // unimplemented!()
        let erased_msg: Box<dyn Handlee<T>> = Box::new(msg);
        let _ = self.msg_tx.send(erased_msg).await;
    }

    /// Schedules a message to be sent to the module periodically with the given interval.
    /// The first tick is sent after the interval elapses.
    /// Every call to this function results in sending new ticks and does not cancel
    /// ticks resulting from previous calls.
    pub async fn request_tick<M>(&self, message: M, delay: Duration) -> TimerHandle
    where
        M: Message + Clone,
        T: Handler<M>,
    {
        unimplemented!()
    }
}

// You may replace this with `#[derive(Clone)]` if you want.
// It is just important to implement the Clone trait.
impl<T: Module> Clone for ModuleRef<T> {
    /// Creates a new reference to the same module.
    fn clone(&self) -> Self {
        // unimplemented!()
        ModuleRef { 
            msg_tx: self.msg_tx.clone()
        }
    }
}
