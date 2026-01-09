use std::{convert::TryInto, path::Path};
use std::env;
use std::time::Duration;
use std::io::Write;

// Assuming assignment_2_solution is available in your workspace/dependencies
use assignment_2_solution::{
    ClientCommandResponse, Configuration, PublicConfiguration, 
    run_register_process,
};
use tokio::time::sleep;

pub const HMAC_TAG_SIZE: usize = 32;
pub const FIXED_SYSTEM_KEY: [u8; 64] = [67; 64];
pub const FIXED_CLIENT_KEY: [u8; 32] = [100; 32];

pub struct RegisterResponse {
    pub content: ClientCommandResponse,
    pub hmac_tag: [u8; HMAC_TAG_SIZE],
}

pub struct TestProcessesConfig {
    hmac_client_key: Vec<u8>,
    hmac_system_key: Vec<u8>,
    tcp_locations: Vec<(String, u16)>,
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

impl TestProcessesConfig {
    pub const N_SECTORS: u64 = 65536;

    #[allow(clippy::missing_panics_doc, clippy::must_use_candidate)]
    pub async fn new(processes_count: usize, _port_range_start: u16) -> Self {
        let tcp_locations = (0..processes_count)
            .map(|idx| {
                (
                    "localhost".to_string(),
                    _port_range_start + u16::try_from(idx).unwrap(),
                )
            })
            .collect();

        // Note: usage of rand::rng() implies rand 0.9 or a custom wrapper. 
        // If using rand 0.8, replace with rand::thread_rng().
        // let mut rng = rand::thread_rng();

        TestProcessesConfig {
            // hmac_client_key: (0..32).map(|_| rng.gen_range(0..255)).collect(),
            hmac_client_key: Vec::from(FIXED_CLIENT_KEY),
            // hmac_system_key: (0..64).map(|_| rng.gen_range(0..255)).collect(),
            hmac_system_key: Vec::from(FIXED_SYSTEM_KEY),
            tcp_locations
        }
    }

    fn config(&self, proc_idx: usize) -> Configuration {
        Configuration {
            public: PublicConfiguration {
                storage_dir: Path::new(&format!("/tmp/ADD/{proc_idx}")).to_path_buf(),
                tcp_locations: self.tcp_locations.clone(),
                self_rank: u8::try_from(proc_idx + 1).unwrap(),
                n_sectors: TestProcessesConfig::N_SECTORS,
            },
            hmac_system_key: self.hmac_system_key.clone().try_into().unwrap(),
            hmac_client_key: self.hmac_client_key.clone().try_into().unwrap(),
        }
    }

    pub async fn start(&self, idx: usize) {
        let processes_count = self.tcp_locations.len();
        println!("Starting {} processes...", processes_count);
        
        // for idx in 0..processes_count {
        let config = self.config(idx);
        let rank = config.public.self_rank;
        let port = config.public.tcp_locations[idx].1;
        
        println!("Spawning process rank={} on port={}", rank, port);
        
        // Spawn the process in the background
        tokio::spawn(run_register_process(config));
        // }
        
        // Wait briefly for sockets to bind
        wait_for_tcp_listen().await;
    }
}

// Implementation of the helper function missing in the snippet
async fn wait_for_tcp_listen() {
    // Simple implementation: yield to the runtime to allow spawns to initialize
    sleep(Duration::from_secs(1)).await;
}

#[tokio::main]
async fn main() {
    // 1. Initialize Logger (optional, but good for `log::trace` used in your snippet)
    init_logs();

    // 2. Parse arguments
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: {} <port_range_start>", args[0]);
        std::process::exit(1);
    }

    let self_idx: u16 = match args[1].parse() {
        Ok(p) => p,
        Err(_) => {
            eprintln!("Error: Port argument must be a valid 16-bit integer.");
            std::process::exit(1);
        }
    };
    let port_range_start: u16 = 8080;

    // 3. Configure with process_count = 1
    let processes_count = 3;
    let system = TestProcessesConfig::new(processes_count, port_range_start).await;

    // 4. Start the system
    system.start(self_idx as usize).await;

    println!("System started on port {}. Press Ctrl+C to stop.", port_range_start + self_idx);

    // 5. Keep the main thread alive so the spawned tokio tasks continue running
    match tokio::signal::ctrl_c().await {
        Ok(()) => println!("Shutting down..."),
        Err(err) => eprintln!("Unable to listen for shutdown signal: {}", err),
    }
}