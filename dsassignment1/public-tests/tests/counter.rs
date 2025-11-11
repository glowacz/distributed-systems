use assignment_1_solution::{Handler, ModuleRef, System, TimerHandle};
use ntest::timeout;
use std::{time::Duration, vec};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

struct CountToFive {
    self_ref: ModuleRef<Self>,
    /// Backdoor messaging for test.
    five_sender: UnboundedSender<u8>,
}

#[async_trait::async_trait]
impl Handler<u8> for CountToFive {
    async fn handle(&mut self, msg: u8) {
        println!("Handling {}", msg);
        // tokio::time::sleep(Duration::from_nanos(1)).await;
        if msg == 5 {
            self.five_sender.send(msg).unwrap();
        } else {
            self.self_ref.send(msg + 1).await;
        }
    }
}

#[tokio::test]
#[timeout(300)]
async fn self_ref_works() {
    let mut system = System::new().await;
    let (five_sender, mut five_receiver) = unbounded_channel::<u8>();
    let count_to_five = system
        .register_module(|self_ref| CountToFive {
            self_ref,
            five_sender,
        })
        .await;
    // comment sleep in CountToFive module for running this test
    count_to_five.send(1).await;
    assert_eq!(five_receiver.recv().await.unwrap(), 5);

    system.shutdown().await;
}

struct Counter {
    num: u8,
    /// Backdoor
    num_sender: UnboundedSender<u8>,
}

/// Message sent by the timer
#[derive(Clone)]
struct Tick;

#[async_trait::async_trait]
impl Handler<Tick> for Counter {
    async fn handle(&mut self, _msg: Tick) {
        self.num_sender.send(self.num).unwrap();
        println!("Sent number {}; incrementing it", self.num);
        self.num += 1;
    }
}

#[tokio::test]
#[timeout(500)]
async fn stopping_ticks_works() {
    let mut system = System::new().await;
    let (num_sender, mut num_receiver) = unbounded_channel();
    let counter_ref = system
        .register_module(|_| Counter { num: 0, num_sender })
        .await;

    let timer_handle = counter_ref
        .request_tick(Tick, Duration::from_millis(50))
        .await;
    tokio::time::sleep(Duration::from_millis(170)).await;
    timer_handle.stop().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut received_numbers = Vec::new();
    while let Ok(num) = num_receiver.try_recv() {
        received_numbers.push(num);
    }
    assert_eq!(received_numbers, vec![0, 1, 2]);

    system.shutdown().await;
}

#[tokio::test]
#[timeout(300)]
async fn unexpected_shutdown_works() {
    let mut system = System::new().await;
    let (five_sender, _five_receiver) = unbounded_channel::<u8>();
    let count_to_five = system
        .register_module(|self_ref| CountToFive {
            self_ref,
            five_sender,
        })
        .await;
    // uncomment sleep in CountToFive module for running this test
    count_to_five.send(1).await;
    tokio::time::sleep(Duration::from_micros(1000)).await;
    system.shutdown().await;
}

async fn create_multiple_timers(system: &mut System, n: u8) -> (Vec<TimerHandle>, UnboundedReceiver<u8>){
    let mut timer_handles = vec![];

    let (num_sender, num_receiver) = unbounded_channel();
    let counter_ref = system
    .register_module(|_| Counter { num: 0, num_sender })
    .await;

    for _ in 0..n {
        let timer_handle = counter_ref
            .request_tick(Tick, Duration::from_millis(50))
            .await;

        timer_handles.push(timer_handle);
    }

    (timer_handles, num_receiver)
}

#[tokio::test]
#[timeout(500)]
async fn stopping_some_ticks_works() {
    let n: usize = 5;
    let mut system = System::new().await;
    let (timer_handles, mut num_receiver) = create_multiple_timers(&mut system, n as u8).await;
    
    tokio::time::sleep(Duration::from_millis(120)).await; 
    // timer_handles[0].stop().await;
    timer_handles[1].stop().await;
    // timer_handles[2].stop().await;
    timer_handles[3].stop().await;
    timer_handles[4].stop().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut received_numbers: Vec<u8> = vec![];

    for _i in 0..n {
        while let Ok(num) = num_receiver.try_recv() {
            received_numbers.push(num);
        }
    }
    
    // println!("Received numbers {:?}", received_numbers);

    assert_eq!(received_numbers, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]);

    system.shutdown().await;
}

#[tokio::test]
#[timeout(500)]
async fn multiple_stopping_ticks_works() {
    let mut system = System::new().await;
    let (num_sender, mut num_receiver) = unbounded_channel();
    let counter_ref = system
        .register_module(|_| Counter { num: 0, num_sender })
        .await;

    let timer_handle = counter_ref
        .request_tick(Tick, Duration::from_millis(50))
        .await;
    tokio::time::sleep(Duration::from_millis(170)).await;
    timer_handle.stop().await;
    timer_handle.stop().await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    timer_handle.stop().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut received_numbers = Vec::new();
    while let Ok(num) = num_receiver.try_recv() {
        received_numbers.push(num);
    }
    assert_eq!(received_numbers, vec![0, 1, 2]);

    system.shutdown().await;
}

#[tokio::test]
#[timeout(500)]
async fn dropping_module_ref_doesnt_hog_system() {
    let mut system = System::new().await;
    let (num_sender, _num_receiver) = unbounded_channel();
    system
        .register_module(|_| Counter { num: 0, num_sender })
        .await;
    
    println!("dropping_module_ref_doesnt_hog_system successful");
    // system.shutdown().await;
}