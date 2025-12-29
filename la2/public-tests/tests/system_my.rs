use assignment_2_solution::{
    ClientCommandHeader, ClientRegisterCommand, ClientRegisterCommandContent, 
    OperationReturn, RegisterCommand, SectorVec,
};
use assignment_2_test_utils::system::*;
use ntest::timeout;
use serde_big_array::Array;
use std::io::Write;
use chrono::Local;

fn init_logs() {
    let _ = env_logger::builder()
        .is_test(true)
        .filter_level(log::LevelFilter::Info)
        // .format(|buf, record| { // NO Timestamps
        //     writeln!(buf, "{}", record.args())
        // })
        .format(|buf, record| { // precise timestamps
            writeln!(
                buf,
                "{} [{}] - {}",
                Local::now().format("%M:%S%.3f"),
                record.level(),
                record.args()
            )
        })
        .try_init();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[serial_test::serial]
#[timeout(30000)]
async fn two_nodes() {
    init_logs();
    // given
    let port_range_start = 21625;
    let commands_total = 1;
    let config = TestProcessesConfig::new(2, port_range_start).await;
    config.start().await;
    let mut stream = config.connect(0).await;

    for cmd_idx in 0..commands_total {
        config
            .send_cmd(
                &RegisterCommand::Client(ClientRegisterCommand {
                    header: ClientCommandHeader {
                        request_identifier: cmd_idx,
                        sector_idx: cmd_idx,
                    },
                    content: ClientRegisterCommandContent::Write {
                        // data: SectorVec(Box::new(Array([cmd_idx as u8; 4096]))),
                        data: SectorVec(Box::new(Array([5 as u8; 4096]))),
                    },
                }),
                &mut stream,
            )
            .await;
    }

    for _ in 0..commands_total {
        config.read_response(&mut stream).await;
    }

    // when
    for cmd_idx in 0..commands_total {
        config
            .send_cmd(
                &RegisterCommand::Client(ClientRegisterCommand {
                    header: ClientCommandHeader {
                        request_identifier: cmd_idx + 256,
                        sector_idx: cmd_idx,
                    },
                    content: ClientRegisterCommandContent::Read,
                }),
                &mut stream,
            )
            .await;
    }

    // then
    for _ in 0..commands_total {
        let response = config.read_response(&mut stream).await;
        match response.content.op_return {
            OperationReturn::Read {
                read_data: SectorVec(sector),
            } => {
                assert_eq!(
                    sector,
                    Box::new(Array(
                        // [(response.content.request_identifier - 256) as u8; 4096]
                        [5 as u8; 4096]
                    ))
                )
            }
            _ => panic!("Expected read response"),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[serial_test::serial]
#[timeout(50000)]
async fn run_on_many_sectors() {
    // let _ = env_logger::builder().is_test(true).try_init();
    init_logs();

    // given
    let port_range_start = 21518;
    let n_clients = 1;
    let config = TestProcessesConfig::new(4, port_range_start).await;
    config.start().await;
    let mut stream = config.connect(0).await;
    let sectors_to_ask = 50;
    // when
    for i in 1..sectors_to_ask+1 {
        config
            .send_cmd(
                &RegisterCommand::Client(ClientRegisterCommand {
                    header: ClientCommandHeader {
                        request_identifier: i as u64,
                        sector_idx: i as u64,
                    },
                    content: ClientRegisterCommandContent::Write {
                        data: SectorVec(Box::new(Array([i+4; 4096]))),
                    },
                }),
                &mut stream,
            )
            .await;
    }

    for _i in 1..sectors_to_ask+1 {
        config.read_response(&mut stream).await;
    }

    for i in 1..sectors_to_ask+1 {
        config
            .send_cmd(
                &RegisterCommand::Client(ClientRegisterCommand {
                    header: ClientCommandHeader {
                        request_identifier: i as u64 + n_clients,
                        sector_idx: i as u64,
                    },
                    content: ClientRegisterCommandContent::Read,
                }),
                &mut stream,
            )
            .await;
    }

    for i in 1..sectors_to_ask+1 {
        let response = config.read_response(&mut stream).await;

        println!("Sector {i}:\n{:?}", response.content.op_return);

        match response.content.op_return {
            OperationReturn::Read {
                read_data: SectorVec(sector),
            } => {
                assert!(*sector == Array([response.content.request_identifier as u8 - n_clients as u8 + 4; 4096]));
            }
            _ => panic!("Expected read response"),
        }

        println!("Sector {i} passed");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[serial_test::serial]
#[timeout(100000)]
async fn multiple_nodes_multiple_sectors() {
    init_logs();
    // given
    let port_range_start = 21625;
    let commands_total = 500;
    let config = TestProcessesConfig::new(4, port_range_start).await;
    config.start().await;
    let mut stream = config.connect(0).await;

    for cmd_idx in 0..commands_total {
        config
            .send_cmd(
                &RegisterCommand::Client(ClientRegisterCommand {
                    header: ClientCommandHeader {
                        request_identifier: cmd_idx,
                        sector_idx: cmd_idx,
                    },
                    content: ClientRegisterCommandContent::Write {
                        data: SectorVec(Box::new(Array([cmd_idx as u8; 4096]))),
                        // data: SectorVec(Box::new(Array([5 as u8; 4096]))),
                    },
                }),
                &mut stream,
            )
            .await;
    }

    for _ in 0..commands_total {
        config.read_response(&mut stream).await;
    }

    // when
    for cmd_idx in 0..commands_total {
        config
            .send_cmd(
                &RegisterCommand::Client(ClientRegisterCommand {
                    header: ClientCommandHeader {
                        request_identifier: cmd_idx + 256,
                        sector_idx: cmd_idx,
                    },
                    content: ClientRegisterCommandContent::Read,
                }),
                &mut stream,
            )
            .await;
    }

    // then
    for _i in 0..commands_total {
        let response = config.read_response(&mut stream).await;
        // println!("\n====== CLIENT GOT READ RESPONSE {i} ======\n");
        match response.content.op_return {
            OperationReturn::Read {
                read_data: SectorVec(sector),
            } => {
                assert_eq!(
                    sector,
                    Box::new(Array(
                        [(response.content.request_identifier - 256) as u8; 4096]
                        // [5 as u8; 4096]
                    ))
                )
            }
            _ => panic!("Expected read response"),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[serial_test::serial]
#[timeout(500000)]
async fn multiple_clients() {
    init_logs();
    // given
    let port_range_start = 21625;
    let n_clients = 5;
    let config = TestProcessesConfig::new(2, port_range_start).await;
    config.start().await;
    let mut streams = Vec::new();
    for _ in 0..n_clients {
        streams.push(config.connect(0).await);
    }

    for (i, stream) in streams.iter_mut().enumerate() {
        let idx = i.try_into().unwrap();
        config
            .send_cmd(
                &RegisterCommand::Client(ClientRegisterCommand {
                    header: ClientCommandHeader {
                        request_identifier: idx,
                        sector_idx: idx,
                    },
                    content: ClientRegisterCommandContent::Write {
                        data: SectorVec(Box::new(Array([idx as u8; 4096])))
                    },
                }),
                stream,
            )
            .await;
        println!("\n\n\n================================== CLIENT AFTER SENDING {i}TH WRITE ==================================\n\n\n");
    }

    let mut i = 0;
    for stream in &mut streams {
        config.read_response(stream).await;
        println!("\n====== CLIENT GOT WRITE RESPONSE {i} ======\n");
        i += 1;
    }

    // when
    for (i, stream) in streams.iter_mut().enumerate() {
        let idx = i.try_into().unwrap();
        config
            .send_cmd(
                &RegisterCommand::Client(ClientRegisterCommand {
                    header: ClientCommandHeader {
                        request_identifier: idx,
                        sector_idx: idx,
                    },
                    content: ClientRegisterCommandContent::Read
                }),
                stream,
            )
            .await;
        println!("\n\n\n================================== CLIENT AFTER SENDING {i}TH READ ==================================\n\n\n");
    }

    // then
    for (i, stream) in streams.iter_mut().enumerate() {
        let response = config.read_response(stream).await;
        match response.content.op_return {
            OperationReturn::Read {
                read_data: SectorVec(sector),
            } => {
                assert_eq!(
                    sector,
                    Box::new(Array(
                        [i as u8; 4096]
                        // [5 as u8; 4096]
                    ))
                )
            }
            _ => panic!("Expected read response"),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[serial_test::serial]
#[timeout(50000)]
async fn many_clients_same_sector() {
    init_logs();
    // given
    let port_range_start = 21625;
    let n_clients = 16;
    let config = TestProcessesConfig::new(1, port_range_start).await;
    config.start().await;
    let mut streams = Vec::new();
    for _ in 0..n_clients {
        streams.push(config.connect(0).await);
    }

    for (i, stream) in streams.iter_mut().enumerate() {
        let idx = i.try_into().unwrap();
        config
            .send_cmd(
                &RegisterCommand::Client(ClientRegisterCommand {
                    header: ClientCommandHeader {
                        request_identifier: idx,
                        // sector_idx: idx,
                        sector_idx: 0,
                    },
                    content: ClientRegisterCommandContent::Write {
                        data: SectorVec(Box::new(Array([idx as u8; 4096])))
                    },
                }),
                stream,
            )
            .await;
        // println!("\n\n\n================================== CLIENT AFTER SENDING {i}TH WRITE ==================================\n\n\n");
    }

    // let mut i = 0;
    for stream in &mut streams {
        config.read_response(stream).await;
        // println!("\n====== CLIENT GOT WRITE RESPONSE {i} ======\n");
        // i += 1;
    }

    // // when
    // for (i, stream) in streams.iter_mut().enumerate() {
    //     let idx = i.try_into().unwrap();
    //     config
    //         .send_cmd(
    //             &RegisterCommand::Client(ClientRegisterCommand {
    //                 header: ClientCommandHeader {
    //                     request_identifier: n_clients + idx,
    //                     sector_idx: idx,
    //                 },
    //                 content: ClientRegisterCommandContent::Read
    //             }),
    //             stream,
    //         )
    //         .await;
    //     println!("\n\n\n================================== CLIENT AFTER SENDING {i}TH READ ==================================\n\n\n");
    // }

    // // then
    // for (i, stream) in streams.iter_mut().enumerate() {
    //     let response = config.read_response(stream).await;
    //     println!("\n====== CLIENT GOT READ RESPONSE {i} ======\n");
    //     match response.content.op_return {
    //         OperationReturn::Read {
    //             read_data: SectorVec(_sector),
    //         } => {
    //             // assert_eq!(
    //             //     sector,
    //             //     Box::new(Array(
    //             //         [i as u8; 4096]
    //             //         // [5 as u8; 4096]
    //             //     ))
    //             // )
    //         }
    //         _ => panic!("Expected read response"),
    //     }
    // }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[serial_test::serial]
#[timeout(50000)]
async fn serial_writes_same_sector() {
    init_logs();
    // given
    let port_range_start = 21625;
    let config = TestProcessesConfig::new(2, port_range_start).await;
    config.start().await;
    let mut stream = config.connect(0).await;

    // =======================================================================================================================================
    // WRITING 1
    // =======================================================================================================================================
    config
        .send_cmd(
            &RegisterCommand::Client(ClientRegisterCommand {
                header: ClientCommandHeader {
                    request_identifier: 1001,
                    // sector_idx: idx,
                    sector_idx: 0,
                },
                content: ClientRegisterCommandContent::Write {
                    data: SectorVec(Box::new(Array([1 as u8; 4096])))
                },
            }),
            &mut stream,
        )
        .await;

    config.read_response(&mut stream).await;

    config
        .send_cmd(
            &RegisterCommand::Client(ClientRegisterCommand {
                header: ClientCommandHeader {
                    request_identifier: 1002,
                    sector_idx: 0,
                },
                content: ClientRegisterCommandContent::Read
            }),
            &mut stream,
        )
        .await;

    let response = config.read_response(&mut stream).await;
    match response.content.op_return {
        OperationReturn::Read {
            read_data: SectorVec(sector),
        } => {
            assert_eq!(
                sector,
                Box::new(Array(
                    [1 as u8; 4096]
                    // [5 as u8; 4096]
                ))
            )
        }
        _ => panic!("Expected read response"),
    }

    // =======================================================================================================================================
    // WRITING 2
    // =======================================================================================================================================
    config
        .send_cmd(
            &RegisterCommand::Client(ClientRegisterCommand {
                header: ClientCommandHeader {
                    request_identifier: 1003,
                    // sector_idx: idx,
                    sector_idx: 0,
                },
                content: ClientRegisterCommandContent::Write {
                    data: SectorVec(Box::new(Array([2 as u8; 4096])))
                },
            }),
            &mut stream,
        )
        .await;

    config.read_response(&mut stream).await;

    config
        .send_cmd(
            &RegisterCommand::Client(ClientRegisterCommand {
                header: ClientCommandHeader {
                    request_identifier: 1004,
                    sector_idx: 0,
                },
                content: ClientRegisterCommandContent::Read
            }),
            &mut stream,
        )
        .await;

    let response = config.read_response(&mut stream).await;
    match response.content.op_return {
        OperationReturn::Read {
            read_data: SectorVec(sector),
        } => {
            assert_eq!(
                sector,
                Box::new(Array(
                    [2 as u8; 4096]
                    // [5 as u8; 4096]
                ))
            )
        }
        _ => panic!("Expected read response"),
    }
}