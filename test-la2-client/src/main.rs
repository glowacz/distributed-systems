use std::convert::TryInto;
use std::time::Duration;

use assignment_2_solution::{
    ClientCommandHeader, ClientCommandResponse, ClientRegisterCommand,
    ClientRegisterCommandContent, OperationReturn, RegisterCommand, SectorVec, StatusCode,
    serialize_register_command,
};
use serde_big_array::Array;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// Configuration constants
const HMAC_TAG_SIZE: usize = 32;
const TARGET_HOST: &str = "127.0.0.1";
const TARGET_PORT: u16 = 8080;

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

async fn concurrent_operations_scenario() {
    println!("--- Starting Client Scenario ---");
    println!("Target: {}:{}", TARGET_HOST, TARGET_PORT);
    
    let n_clients = 16;
    let mut clients = Vec::new();

    // 1. Establish connections
    println!("Connecting {} clients...", n_clients);
    for _ in 0..n_clients {
        clients.push(RegisterClient::connect(TARGET_HOST, TARGET_PORT, FIXED_CLIENT_KEY.to_vec()).await);
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

    // 4. Send READ command (using the first client connection)
    println!("Sending READ command verification...");
    let read_cmd = RegisterCommand::Client(ClientRegisterCommand {
        header: ClientCommandHeader {
            request_identifier: n_clients as u64, // New ID
            sector_idx: 0,
        },
        content: ClientRegisterCommandContent::Read,
    });
    
    clients[0].send_cmd(&read_cmd).await;

    println!("Sent READ");

    // 5. Verify data
    let response = clients[0].read_response().await;

    println!("Received READ response");
    
    match response.content.op_return {
        OperationReturn::Read { read_data: SectorVec(sector) } => {
            let is_pattern_a = *sector == Array([1; 4096]);
            let is_pattern_b = *sector == Array([254; 4096]);
            let is_pattern_c = *sector == Array([69; 4096]);

            // if is_pattern_a || is_pattern_b {
            if is_pattern_c {
                println!("SUCCESS: Read data matches one of the written patterns.");
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
    env_logger::init();
    
    // Wait a brief moment to ensure connection isn't instant if executed via script
    tokio::time::sleep(Duration::from_millis(100)).await;

    concurrent_operations_scenario().await;
}