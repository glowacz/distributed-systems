use assignment_2_solution::{
    ClientCommandHeader, ClientRegisterCommand, ClientRegisterCommandContent, 
    OperationReturn, RegisterCommand, SectorVec,
    serialize_register_command,
};
use assignment_2_test_utils::system::*;
use hmac::Mac;
use ntest::timeout;
use serde_big_array::Array;
use tokio::io::{AsyncWriteExt};
use tokio::net::TcpStream;
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

#[tokio::test]
#[serial_test::serial]
#[timeout(2000)]
async fn two_nodes() {
    init_logs();
    // given
    let port_range_start = 21625;
    let commands_total = 1;
    let config = TestProcessesConfig::new(2, port_range_start);
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

// TODO: in this test only 5 connections should form: client <--> Node 0 and Node 0 with every other node (incl self for now)
#[tokio::test]
#[serial_test::serial]
#[timeout(100000)]
async fn multiple_nodes_multiple_sectors() {
    init_logs();
    // given
    let port_range_start = 21625;
    let commands_total = 200;
    let config = TestProcessesConfig::new(4, port_range_start);
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
    for _ in 0..commands_total {
        let response = config.read_response(&mut stream).await;
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

#[tokio::test]
#[serial_test::serial]
#[timeout(5000)]
async fn multiple_clients() {
    init_logs();
    // given
    let port_range_start = 21625;
    let n_clients = 100;
    let config = TestProcessesConfig::new(4, port_range_start);
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

async fn send_cmd(register_cmd: &RegisterCommand, stream: &mut TcpStream, hmac_client_key: &[u8]) {
    let mut data = Vec::new();
    serialize_register_command(register_cmd, &mut data, hmac_client_key)
        .await
        .unwrap();

    stream.write_all(&data).await.unwrap();
}

fn hmac_tag_is_ok(key: &[u8], data: &[u8]) -> bool {
    let boundary = data.len() - HMAC_TAG_SIZE;
    let mut mac = HmacSha256::new_from_slice(key).unwrap();
    mac.update(&data[..boundary]);
    mac.verify_slice(&data[boundary..]).is_ok()
}
