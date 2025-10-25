// WARNING: Do not modify definitions of public types or function names in this
// file – your solution will be tested automatically! Implement all missing parts.

use crate::definitions::{
    Ident, Message, MessageHandler, Module, ModuleMessage, Num, SystemMessage,
};
use crossbeam_channel::{Receiver, Sender, unbounded};
use std::thread;
use std::thread::JoinHandle;
use std::collections::HashMap;

/// Structure representing a Divider module.
///
/// The Divider module performs the division-by-two operation.
#[derive(Debug)]
pub(crate) struct DividerModule {
    /// Identifier of the module.
    id: Ident,
    /// Identifier of the multiplier module.
    other: Option<Ident>,
    /// Queue for outgoing messages.
    queue: Sender<Message>,
}

/// Structure representing a Multiplier module.
///
/// The Multiplier module performs the multiply-by-three-add-one operation.
#[derive(Debug)]
pub(crate) struct MultiplierModule {
    /// The initial value for the computation of the Collatz problem.
    num: Num,
    /// Identifier of the module.
    id: Ident,
    /// Identifier of the other module.
    other: Option<Ident>,
    /// Queue for outgoing messages.
    queue: Sender<Message>,
}

impl DividerModule {
    /// Create the module and register it in the system.
    ///
    /// Note this function is returning an identifier rather than `Self`.
    /// That is, this module may be interacted with only through sending messages.
    pub(crate) fn create(queue: Sender<Message>) -> Ident {
        // The identifier is an opaque number for your code!
        // It represents the idea that modules refer to each other
        // by sending messages at an address.
        
        let id = Ident::new();
        let divider_mod = DividerModule { id, other: Option::None, queue: queue.clone() };
        
        let msg = Message::System(SystemMessage::RegisterModule(Module::Divider(divider_mod)));
        queue.send(msg).unwrap();

        id
    }
}

/// Method for handling messages directed at a Divider module.
impl MessageHandler for DividerModule {
    /// Get an identifier of the module.
    fn get_id(&self) -> Ident {
        self.id
    }

    /// Handle the computation-step message.
    ///
    /// Here the next number of in the sequence is calculated,
    /// or the computation is stopped.
    /// Remember to make the module send messages to itself,
    /// rather than recursively invoking methods of the module.
    fn compute_step(&mut self, idx: usize, num: Num) {
        assert!(num.is_multiple_of(2));
                
        let new_num = num / 2;
        let compute_step_msg = ModuleMessage::ComputeStep { idx: idx + 1, num: new_num };
        let other = self.other.expect("This module ({self.id}) had no `other` registered");
        let msg = match new_num.is_multiple_of(2) {
            true => Message::ToModule(self.id, compute_step_msg),
            false => Message::ToModule(other, compute_step_msg)
        };

        self.queue.send(msg).unwrap();
    }

    /// Handle the init message.
    ///
    /// The module finishes its initialization.
    fn init(&mut self, other: Ident) {
        self.other = Some(other);
        println!("Initializing Divider Module, its id is {:?}, other's id is {:?}", self.id, self.other);
    }
}

impl MultiplierModule {
    /// Create the module and register it in the system.
    pub(crate) fn create(some_num: Num, queue: Sender<Message>) -> Ident {
        // The identifier is an opaque number for your code!
        let id = Ident::new();
        let multiplier_mod = MultiplierModule { num: some_num, id, other: Option::None, queue: queue.clone() };
        let msg = Message::System(SystemMessage::RegisterModule(Module::Multiplier(multiplier_mod)));
        queue.send(msg).unwrap();
        id
    }
}

impl MessageHandler for MultiplierModule {
    fn get_id(&self) -> Ident {
        self.id
    }

    /// Handle the computation-step message.
    ///
    /// Here the next number of in the sequence is calculated,
    /// or the computation is stopped.
    /// Remember to make the module send messages to itself,
    /// rather than recursively invoking methods of the module.
    fn compute_step(&mut self, idx: usize, num: Num) {
        assert!(!num.is_multiple_of(2));

        if num == 1 {
            let msg = Message::System(SystemMessage::Exit(idx));
            self.queue.send(msg).unwrap();
        }
        
        let new_num = 3 * num + 1;
        let compute_step_msg = ModuleMessage::ComputeStep { idx: idx + 1, num: new_num };
        let other = self.other.expect("This module ({self.id}) had no `other` registered");
        let msg = match new_num.is_multiple_of(2) {
            true => Message::ToModule(other, compute_step_msg),
            false => Message::ToModule(self.id, compute_step_msg)
        };
        
        self.queue.send(msg).unwrap();
    }

    /// Handle the init message.
    ///
    /// The module finishes its initialization and starts the computation
    /// by sending a message.
    fn init(&mut self, other: Ident) {
        self.other = Some(other);
        println!("Initializing Multiplier Module, its id is {:?}, other's id is {:?}", self.id, self.other);
    }
}

/// Run the executor.
///
/// The executor handles `Message::System` messages locally
/// and dispatches `Message::ToModule` by calling methods of `<Module as MessageHandler>`.
/// The system should be generic to support any number of modules of any types.
/// For instance, adding a new module type to the `Module` enum in the `definitions.rs`
/// should not require changes to the implementation of this function.
///
/// The system returns the value found in the `Exit` message
/// or `None` if there are no more messages to process.
pub(crate) fn run_executor(rx: Receiver<Message>) -> JoinHandle<Option<usize>> {
    // unimplemented!();
    thread::spawn(move || {
        let mut modules = HashMap::<Ident, Module>::new();
        while let Ok(msg) = rx.recv() {
            // unimplemented!("Handle");
            match msg {
                Message::System(msg) => {
                    println!("Received system message {msg:?}");
                    match msg {
                        SystemMessage::RegisterModule(module) => {
                            let id = module.get_id();
                            println!("Module {module:?} registered in the executor");
                            modules.insert(id, module);
                        },
                        SystemMessage::Exit(n_steps) => {
                            // println!("Exiting the system\nTook {n_steps} steps");
                            println!("Exiting the system");
                            return Some(n_steps);
                        },
                    }
                },
                Message::ToModule(id, msg) => {
                    // println!("Received message {msg:?} to module {id:?}");
                    match msg {
                        ModuleMessage::Init{ other } => {
                            // let mut module: &mut Module = modules.get_mut(&id).unwrap();
                            let module: &mut Module = modules.get_mut(&id).unwrap();
                            module.init(other);
                        },
                        ModuleMessage::ComputeStep { idx, num } => {
                            let module = modules.get_mut(&id).unwrap();
                            module.compute_step(idx, num);
                            // println!("[executor] compute step number {idx}; current num {num}");
                        }
                    }
                }
            }
        }
        // No more messages in the system
        None
    })
}

/// Execute the Collatz problem returning the *total stopping time* of `n`.
///
/// The *total stopping time* is just the number of iterations in the computation.
///
/// PS, this function is known to stop for every possible u64 value :).
pub(crate) fn collatz(n: Num) -> usize {
    // Create the queue and two modules:
    let (tx, rx): (Sender<Message>, Receiver<Message>) = unbounded();
    let divider = DividerModule::create(tx.clone());
    println!("divider module created with id {divider:?}");
    let multiplier = MultiplierModule::create(n, tx.clone());
    println!("multiplier module created with id {multiplier:?}");

    // // Initialize the modules by sending `Init` messages:
    let init_divider_msg = Message::ToModule(divider, ModuleMessage::Init { other: multiplier });
    tx.send(init_divider_msg).unwrap();
    let init_multiplier_msg = Message::ToModule(multiplier, ModuleMessage::Init { other: divider });
    tx.send(init_multiplier_msg).unwrap();

    let compute_step_msg = ModuleMessage::ComputeStep { idx: 1, num: n };
    let first_compute_step_whole_msg = match n.is_multiple_of(2) {
        true => Message::ToModule(divider, compute_step_msg),
        false => Message::ToModule(multiplier, compute_step_msg)
    };
    tx.send(first_compute_step_whole_msg).unwrap();
    // Run the executor:
    run_executor(rx).join().unwrap().unwrap()
}
