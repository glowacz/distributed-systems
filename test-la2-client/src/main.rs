use std::convert::TryInto;
use std::env;

use assignment_2_solution::{
    ClientCommandHeader, ClientCommandResponse, ClientRegisterCommand,
    ClientRegisterCommandContent, OperationReturn, RegisterCommand, SectorVec, StatusCode,
    serialize_register_command,
};
use serde_big_array::Array;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::task::JoinSet;
use std::io::Write;

// Configuration constants
const HMAC_TAG_SIZE: usize = 32;
const TARGET_HOST: &str = "127.0.0.1";
// const TARGET_PORT: u16 = 8080;
const PORT_0: u16 = 8080;
const PORT_1: u16 = 8081;

// !! IMPORTANT !!
// Both the Server and this Client must use the SAME key. 
// If your server generates a random key on startup, this client will fail 
// (StatusCode::AuthFailure) because it cannot generate the correct signature.
// For this test to work, hardcode this key in your Server config as well.
const FIXED_CLIENT_KEY: [u8; 32] = [100; 32]; 

pub struct RegisterResponse {
    pub content: ClientCommandResponse,
    pub hmac_tag: [u8; HMAC_TAG_SIZE],
}

/// A wrapper struct to handle the TCP connection and Protocol logic
struct RegisterClient {
    stream: TcpStream,
    hmac_key: Vec<u8>,
}

pub fn init_logs() {
    let _ = env_logger::builder()
        .is_test(true)
        .filter_level(log::LevelFilter::Trace)
        // .format(|buf, record| { // NO Timestamps
        //     writeln!(buf, "{}", record.args())
        // })
        .format(|buf, record| { // precise timestamps
            writeln!(
                buf,
                "{} [{}] - {}",
                buf.timestamp_millis(),
                record.level(),
                record.args()
            )
        })
        .try_init();
}

impl RegisterClient {
    /// Connects to the target server
    pub async fn connect(host: &str, port: u16, hmac_key: Vec<u8>) -> Self {
        let addr = format!("{}:{}", host, port);
        let stream = TcpStream::connect(&addr)
            .await
            .expect(&format!("Could not connect to {}", addr));
        
        RegisterClient {
            stream,
            hmac_key,
        }
    }

    /// Serializes and sends a command
    pub async fn send_cmd(&mut self, register_cmd: &RegisterCommand) {
        let mut data = Vec::new();
        // Uses the library function to serialize and sign with the key
        serialize_register_command(register_cmd, &mut data, &self.hmac_key)
            .await
            .unwrap();

        self.stream.write_all(&data).await.unwrap();
    }

    /// Reads and parses the response
    pub async fn read_response(&mut self) -> RegisterResponse {
        // 1. Read message length
        let size = self.stream.read_u64().await.unwrap();
        
        // 2. Read Status Code
        let status = match self.stream.read_u32().await.unwrap() {
            0 => StatusCode::Ok,
            1 => StatusCode::AuthFailure,
            2 => StatusCode::InvalidSectorIndex,
            _ => panic!("Invalid status code received"),
        };

        // 3. Read Request Identifier
        let req_id = self.stream.read_u64().await.unwrap();
        
        // 4. Read Operation Type
        let op_type = self.stream.read_u32().await.unwrap();
        
        // 5. Read Content (if Read op) or determine Write return
        let op_return = match op_type {
            0 => {
                let mut buf = [0u8; 4096];
                self.stream.read_exact(&mut buf).await.unwrap();
                OperationReturn::Read {
                    read_data: SectorVec(Box::new(Array(buf))),
                }
            }
            1 => OperationReturn::Write,
            _ => panic!("Invalid operation type received"),
        };

        // Validate size consistency
        assert_eq!(
            size,
            match op_return {
                OperationReturn::Write => 16 + HMAC_TAG_SIZE as u64,
                OperationReturn::Read { .. } => 16 + HMAC_TAG_SIZE as u64 + 4096,
            }
        );
        
        // 6. Read HMAC Tag
        let mut tag = [0x00_u8; HMAC_TAG_SIZE];
        self.stream.read_exact(&mut tag).await.unwrap();
        
        RegisterResponse {
            content: ClientCommandResponse {
                status,
                request_identifier: req_id,
                op_return,
            },
            hmac_tag: tag,
        }
    }
}

async fn concurrent_operations_write() {
    let n_clients = 16;
    let mut clients = Vec::new();

    // 1. Establish connections
    println!("Connecting {} clients...", n_clients);
    for _ in 0..n_clients {
        clients.push(RegisterClient::connect(TARGET_HOST, PORT_1, FIXED_CLIENT_KEY.to_vec()).await);
    }

    // 2. Send WRITE commands concurrently (iterating logic)
    println!("Sending WRITE commands...");
    for (i, client) in clients.iter_mut().enumerate() {
        let request_id = i.try_into().unwrap();
        
        // Create the command
        let cmd = RegisterCommand::Client(ClientRegisterCommand {
            header: ClientCommandHeader {
                request_identifier: request_id,
                sector_idx: 0,
            },
            content: ClientRegisterCommandContent::Write {
                // Alternating pattern: Client 0 writes 1s, Client 1 writes 254s, etc.
                // data: SectorVec(Box::new(Array([if i % 2 == 0 { 1 } else { 254 }; 4096]))),
                data: SectorVec(Box::new(Array([69; 4096]))),
            },
        });

        client.send_cmd(&cmd).await;
    }

    // 3. Read responses for WRITES
    println!("Reading WRITE responses...");
    for client in &mut clients {
        let resp = client.read_response().await;
        if let StatusCode::Ok = resp.content.status {
            // OK
        } else {
             eprintln!("Warning: Received non-OK status: {:?}", resp.content.status);
        }
    }
}

async fn concurrent_operations_read() {
    let n_clients = 16;
    let mut client = RegisterClient::connect(TARGET_HOST, PORT_0, FIXED_CLIENT_KEY.to_vec()).await;

    // 4. Send READ command (using the first client connection)
    println!("Sending READ command verification...");
    let read_cmd = RegisterCommand::Client(ClientRegisterCommand {
        header: ClientCommandHeader {
            request_identifier: n_clients as u64, // New ID
            sector_idx: 0,
        },
        content: ClientRegisterCommandContent::Read,
    });
    
    client.send_cmd(&read_cmd).await;

    println!("Sent READ");

    // 5. Verify data
    let response = client.read_response().await;

    println!("Received READ response");
    
    match response.content.op_return {
        OperationReturn::Read { read_data: SectorVec(sector) } => {
            let is_pattern_a = *sector == Array([1; 4096]);
            let is_pattern_b = *sector == Array([254; 4096]);
            
            if is_pattern_a || is_pattern_b {
                println!("SUCCESS: Read data matches one of the written patterns.");
            } else {
                eprintln!("FAILURE: Data read back did not match any written pattern.");
                panic!("Data verification failed");
            }
        }
        _ => panic!("Expected Read operation return, got Write or other."),
    }
}

async fn multiple_clients_write() {
    let n_clients = 250;

    // 1. Establish connections, send WRITE commands and receive responses concurrently
    println!("Connecting {} clients...", n_clients);

    let mut handles = Vec::with_capacity(n_clients);

    for idx in 0..n_clients {
        let handle = tokio::spawn(async move {
            let mut client = RegisterClient::connect(TARGET_HOST, PORT_1, FIXED_CLIENT_KEY.to_vec()).await;
            
            // Create the command
            let cmd = RegisterCommand::Client(ClientRegisterCommand {
                header: ClientCommandHeader {
                    request_identifier: idx as u64,
                    sector_idx: idx as u64,
                },
                content: ClientRegisterCommandContent::Write {
                    data: SectorVec(Box::new(Array([(idx % 255) as u8; 4096])))
                },
            });
    
            client.send_cmd(&cmd).await;

            let resp = client.read_response().await;

            if let StatusCode::Ok = resp.content.status {
                // OK
            } else {
                    eprintln!("Warning: Received non-OK status: {:?}", resp.content.status);
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        if let Err(e) = handle.await {
            eprintln!("Task failed to join: {:?}", e);
        }
    }
}

async fn multiple_clients_read() {
    let n_clients = 250;

    // 1. Establish connections, send WRITE commands and receive responses concurrently
    println!("Connecting {} clients...", n_clients);

    let mut set = JoinSet::new();

    for idx in 0..n_clients {
        set.spawn(async move {
            let mut client = RegisterClient::connect(TARGET_HOST, PORT_0, FIXED_CLIENT_KEY.to_vec()).await;
            
            // Create the command
            let cmd = RegisterCommand::Client(ClientRegisterCommand {
                header: ClientCommandHeader {
                    request_identifier: idx + n_clients,
                    sector_idx: idx,
                },
                content: ClientRegisterCommandContent::Read
            });
    
            client.send_cmd(&cmd).await;

            let response = client.read_response().await;

            match response.content.op_return {
                OperationReturn::Read { read_data: SectorVec(sector) } => {           
                    if *sector == Array([(idx % 255) as u8; 4096]) {
                        println!("SUCCESS: Read data for sector {idx} is good.");
                    } else {
                        eprintln!("FAILURE: Data read back did not match any written pattern.");
                        panic!("Data verification failed");
                    }
                }
                _ => panic!("Expected Read operation return, got Write or other."),
            }
        });
    }

    set.join_all().await;
}

async fn write_67() {
    println!("Test: write_67");

    // 1. Establish connections
    println!("Connecting 1 client...");
    let mut client = RegisterClient::connect(TARGET_HOST, PORT_1, FIXED_CLIENT_KEY.to_vec()).await;

    // 2. Send WRITE commands concurrently (iterating logic)
    println!("Sending WRITE command...");
    let request_id = 67;
    
    // Create the command
    let cmd = RegisterCommand::Client(ClientRegisterCommand {
        header: ClientCommandHeader {
            request_identifier: request_id,
            sector_idx: 0,
        },
        content: ClientRegisterCommandContent::Write {
            data: SectorVec(Box::new(Array([67; 4096]))),
        },
    });

    client.send_cmd(&cmd).await;

    // 3. Read responses for WRITES
    println!("Reading WRITE responses...");
    let resp = client.read_response().await;
    if let StatusCode::Ok = resp.content.status {
        // OK
    } else {
            eprintln!("Warning: Received non-OK status: {:?}", resp.content.status);
    }
}

async fn write_69() {
    println!("Test: write_69");

    // 1. Establish connections
    println!("Connecting 1 client...");
    let mut client = RegisterClient::connect(TARGET_HOST, PORT_1, FIXED_CLIENT_KEY.to_vec()).await;

    // 2. Send WRITE commands concurrently (iterating logic)
    println!("Sending WRITE command...");
    let request_id = 69;
    
    // Create the command
    let cmd = RegisterCommand::Client(ClientRegisterCommand {
        header: ClientCommandHeader {
            request_identifier: request_id,
            sector_idx: 0,
        },
        content: ClientRegisterCommandContent::Write {
            data: SectorVec(Box::new(Array([69; 4096]))),
        },
    });

    client.send_cmd(&cmd).await;

    // 3. Read responses for WRITES
    println!("Reading WRITE responses...");
    let resp = client.read_response().await;
    if let StatusCode::Ok = resp.content.status {
        // OK
    } else {
            eprintln!("Warning: Received non-OK status: {:?}", resp.content.status);
    }
}

async fn read_67() {
    println!("Test: read_67");

    // 1. Establish connections
    println!("Connecting 1 client...");
    let mut client = RegisterClient::connect(TARGET_HOST, PORT_0, FIXED_CLIENT_KEY.to_vec()).await;

    // 2. Send WRITE commands concurrently (iterating logic)
    println!("Sending WRITE command...");
    let request_id = 67 + 100;
    
    // Create the command
    let cmd = RegisterCommand::Client(ClientRegisterCommand {
        header: ClientCommandHeader {
            request_identifier: request_id,
            sector_idx: 0,
        },
        content: ClientRegisterCommandContent::Read
    });

    client.send_cmd(&cmd).await;

    // 3. Read responses for WRITES
    println!("Reading READ responses...");
    let response = client.read_response().await;
    println!("Received READ response");
    
    match response.content.op_return {
        OperationReturn::Read { read_data: SectorVec(sector) } => {
            if *sector == Array([67; 4096]) {
                println!("SUCCESS: Read 67");
            } else {
                eprintln!("FAILURE: Data read back did not match any written pattern.");
                panic!("Data verification failed");
            }
        }
        _ => panic!("Expected Read operation return, got Write or other."),
    }
}

async fn read_69() {
    println!("Test: read_69");

    // 1. Establish connections
    println!("Connecting 1 client...");
    let mut client = RegisterClient::connect(TARGET_HOST, PORT_0, FIXED_CLIENT_KEY.to_vec()).await;

    // 2. Send WRITE commands concurrently (iterating logic)
    println!("Sending WRITE command...");
    let request_id = 69 + 100;
    
    // Create the command
    let cmd = RegisterCommand::Client(ClientRegisterCommand {
        header: ClientCommandHeader {
            request_identifier: request_id,
            sector_idx: 0,
        },
        content: ClientRegisterCommandContent::Read
    });

    client.send_cmd(&cmd).await;

    // 3. Read responses for WRITES
    println!("Reading READ responses...");
    let response = client.read_response().await;
    println!("Received READ response");
    
    match response.content.op_return {
        OperationReturn::Read { read_data: SectorVec(sector) } => {
            if *sector == Array([69; 4096]) {
                println!("SUCCESS: Read Read 69.");
            } else {
                eprintln!("FAILURE: Data read back did not match any written pattern.");
                panic!("Data verification failed");
            }
        }
        _ => panic!("Expected Read operation return, got Write or other."),
    }
}

#[tokio::main]
async fn main() {
    init_logs();

    // 2. Parse arguments
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: {} <port_range_start>", args[0]);
        std::process::exit(1);
    }

    let op_code: u16 = match args[1].parse() {
        Ok(p) => p,
        Err(_) => {
            eprintln!("Error: Operation code must be a valid 16-bit integer.");
            std::process::exit(1);
        }
    };
    
    // Wait a brief moment to ensure connection isn't instant if executed via script
    // tokio::time::sleep(Duration::from_millis(100)).await;

    println!("--- Starting Client Scenario ---");
    // println!("Target: {}:{}", TARGET_HOST, TARGET_PORT);

    match op_code {
        0 => concurrent_operations_write().await,
        100 => concurrent_operations_read().await,

        1 => multiple_clients_write().await,
        101 => multiple_clients_read().await,

        67 => write_67().await,
        167 => read_67().await,

        69 => write_69().await,
        169 => read_69().await,

        code => { println!("Unsupported operation code {code}"); }
    }
}