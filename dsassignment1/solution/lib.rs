use std::{collections::VecDeque, sync::Arc};
use tokio::{sync::{Mutex, mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel}}, time::Duration};
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
trait ErasedMessage<T: Module>: Send + 'static {
    /// The type-erased handle call.
    async fn handle(self: Box<Self>, module: &mut T);
}

/// Implementation of the type-erasure trait.
/// This implementation allows any `M` that `T` can handle
/// to be boxed into a `Box<dyn ErasedMessage<T>>`.
#[async_trait]
impl<M: Message, T: Module + Handler<M>> ErasedMessage<T> for M {
    async fn handle(self: Box<Self>, module: &mut T) {
        // This calls the *specific* T::handle(M) implementation.
        T::handle(module, *self).await;
    }
}

type MySender<T> = UnboundedSender<Box<dyn ErasedMessage<T> + Send>>;

// The type for your receiver
type MyReceiver<T> = UnboundedReceiver<Box<dyn ErasedMessage<T> + Send>>;

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
        let (msg_tx, mut msg_rx): (MySender<T>, MyReceiver<T>) = unbounded_channel();
        let module_ref = ModuleRef {
            msg_tx,
        };
        let mut module = module_constructor(module_ref.clone());
        // module_ref.module = Some(module);
        tokio::spawn(async move {
            loop {
                let msg = msg_rx.recv().await.unwrap();
                msg.handle(&mut module).await;
            }
        });
        println!("Module registered");
        module_ref
    }

    /// Creates and starts a new instance of the system.
    pub async fn new() -> Self {
        // unimplemented!()
        System {}
    }

    /// Gracefully shuts the system down.
    pub async fn shutdown(&mut self) {
        // unimplemented!()
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
    // msg_tx: UnboundedSender<Box<dyn ErasedMessage<T>>>,
    msg_tx: MySender<T>,
}

impl<T: Module> ModuleRef<T> {
    /// Sends the message to the module.
    pub async fn send<M: Message>(&self, msg: M)
    where
        T: Handler<M>,
    {
        // unimplemented!()
        let erased_msg: Box<dyn ErasedMessage<T>> = Box::new(msg);
        self.msg_tx.send(erased_msg).unwrap();
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
