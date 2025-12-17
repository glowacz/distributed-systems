mod domain;

use std::sync::Arc;

pub use crate::domain::*;
use crate::my_register_client::{MyRegisterClient, start_tcp_server};
pub use atomic_register_public::*;
pub use register_client_public::*;
pub use sectors_manager_public::*;
pub use transfer_public::*;
pub mod stable_storage;
pub mod my_sectors_manager;
pub mod my_atomic_register;
pub mod my_register_client;
pub mod alt_register_client;

pub async fn run_register_process(config: Configuration) {
    let self_addr = config.public.tcp_locations[(config.public.self_rank - 1) as usize].clone();
    let storage_dir = config.public.storage_dir.clone();

    let register_client = Arc::new(MyRegisterClient::new(config).await);
    // this is not a tokio task, but it will spawn a task for each of its operations (send/broadcast)
    let _ = start_tcp_server(register_client, self_addr, storage_dir).await;
}

pub mod atomic_register_public {
    use crate::my_atomic_register::MyAtomicRegister;
    use crate::{
        ClientCommandResponse, ClientRegisterCommand, RegisterClient, SectorIdx, SectorsManager,
        SystemRegisterCommand,
    };
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;

    #[async_trait::async_trait]
    pub trait AtomicRegister: Send + Sync {
        /// Handle a client command. After the command is completed, we expect
        /// callback to be called. Note that completion of client command happens after
        /// delivery of multiple system commands to the register, as the algorithm specifies.
        ///
        /// This function corresponds to the handlers of Read and Write events in the
        /// (N,N)-AtomicRegister algorithm.
        async fn client_command(
            &mut self,
            cmd: ClientRegisterCommand,
            success_callback: Box<
                dyn FnOnce(ClientCommandResponse) -> Pin<Box<dyn Future<Output = ()> + Send>>
                    + Send
                    + Sync,
            >,
        );

        /// Handle a system command.
        ///
        /// This function corresponds to the handlers of `SystemRegisterCommand` messages in the (N,N)-AtomicRegister algorithm.
        async fn system_command(&mut self, cmd: SystemRegisterCommand);
    }

    /// Idents are numbered starting at 1 (up to the number of processes in the system).
    /// Communication with other processes of the system is to be done by `register_client`.
    /// And sectors must be stored in the `sectors_manager` instance.
    ///
    /// This function corresponds to the handlers of Init and Recovery events in the
    /// (N,N)-AtomicRegister algorithm.
    pub async fn build_atomic_register(
        self_ident: u8,
        sector_idx: SectorIdx,
        register_client: Arc<dyn RegisterClient>,
        sectors_manager: Arc<dyn SectorsManager>,
        processes_count: u8,
    ) -> Box<dyn AtomicRegister> {
        Box::new(MyAtomicRegister::new(
            self_ident,
            sector_idx,
            register_client,
            sectors_manager,
            processes_count,
        ).await)
    }
}

pub mod sectors_manager_public {
    use crate::{SectorIdx, SectorVec};
    use std::path::PathBuf;
    use std::sync::Arc;

    #[async_trait::async_trait]
    pub trait SectorsManager: Send + Sync {
        /// Returns 4096 bytes of sector data by index.
        async fn read_data(&self, idx: SectorIdx) -> SectorVec;

        /// Returns timestamp and write rank of the process which has saved this data.
        /// Timestamps and ranks are relevant for atomic register algorithm, and are described
        /// there.
        async fn read_metadata(&self, idx: SectorIdx) -> (u64, u8);

        /// Writes a new data, along with timestamp and write rank to some sector.
        async fn write(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8));
    }

    /// Path parameter points to a directory to which this method has exclusive access.
    pub async fn build_sectors_manager(path: PathBuf) -> Arc<dyn SectorsManager> {
        Arc::new(crate::my_sectors_manager::MySectorsManager::new(path).await)
    }
}

pub mod transfer_public {
    use crate::{ClientCommandResponse, RegisterCommand};
    use bincode::{config::standard, error::{DecodeError, EncodeError}, serde::{decode_from_slice, encode_to_vec}};
    use hmac::{Hmac, Mac};
    use serde::Serialize;
    use sha2::Sha256;
    use std::io::{Error, ErrorKind};
    use tokio::io::{AsyncRead, AsyncWrite};
    use tokio::io::AsyncWriteExt;
    use tokio::io::AsyncReadExt; // Import the trait to bring `read_exact` into scope
    // use hmac::digest::KeyInit;
    type HmacSha256 = Hmac<Sha256>;
    #[derive(Debug)]
    pub enum EncodingError {
        IoError(Error),
        BincodeError(EncodeError),
    }

    #[derive(Debug, derive_more::Display)]
    pub enum DecodingError {
        IoError(Error),
        BincodeError(DecodeError),
        InvalidMessageSize,
    }

    pub async fn serialize<T: Serialize>(
        cmd: &T,
        writer: &mut (dyn AsyncWrite + Send + Unpin),
        hmac_key: &[u8],
    ) -> Result<(), EncodingError> {
        let config = standard()
            .with_big_endian()
            .with_fixed_int_encoding();
        
        let payload = encode_to_vec(cmd, config)
            .map_err(|encode_error| 
                EncodingError::BincodeError(encode_error)
            )?;
        
        let mut mac = HmacSha256::new_from_slice(hmac_key)
            .map_err(|_| 
                EncodingError::IoError(
                    Error::new(ErrorKind::InvalidInput, "HMAC key invalid")
                )
            )?;
        mac.update(&payload);
        let tag = mac.finalize();
        let hmac_bytes = tag.into_bytes();

        let message_size = (payload.len() + hmac_bytes.len()) as u64;
        let size_bytes = message_size.to_be_bytes();
        writer
            .write_all(&size_bytes)
            .await
            .map_err(|io_error| EncodingError::IoError(io_error))?;

        writer
            .write_all(&payload)
            .await
            .map_err(|io_error| EncodingError::IoError(io_error))?;

        writer
            .write_all(&hmac_bytes)
            .await
            .map_err(|io_error| EncodingError::IoError(io_error))?;
        
        Ok(())
    }

    // async fn deserialize_register_command_any_hmac(
    //     data: &mut (dyn AsyncRead + Send + Unpin),
    //     hmac_key: &[u8],
    // ) -> Result<(RegisterCommand, bool), DecodingError> 

    // pub async fn deserialize_register_command_from_client(
    //     data: &mut (dyn AsyncRead + Send + Unpin),
    //     hmac_client_key: &[u8; 32],
    // ) -> Result<(RegisterCommand, bool), DecodingError> {
    //     deserialize_register_command_any_hmac(data, hmac_client_key).await
    // }


    pub async fn deserialize_register_command(
        data: &mut (dyn AsyncRead + Send + Unpin),
        hmac_system_key: &[u8; 64],
        hmac_client_key: &[u8; 32],
    ) -> Result<(RegisterCommand, bool), DecodingError> {
        let mut size_buf = [0u8; 8];
        data.read_exact(&mut size_buf).await.map_err(|e| DecodingError::IoError(e))?;
        let message_size = u64::from_be_bytes(size_buf) as usize;

        if message_size < 32 {
            return Err(DecodingError::InvalidMessageSize);
        }

        let mut buf = vec![0u8; message_size];
        data.read_exact(&mut buf).await.map_err(|e| DecodingError::IoError(e))?;

        let (payload, received_hmac) = buf.split_at(message_size - 32);

        // first checking with system hmac key as there 
        // should be more system than client messages
        let mut mac = HmacSha256::new_from_slice(hmac_system_key)
            .map_err(|_| DecodingError::IoError(Error::new(ErrorKind::InvalidInput, "HMAC key invalid")))?;
        mac.update(payload);
        let mut hmac_valid = mac.verify_slice(received_hmac).is_ok();

        // even if the message isn't signed with a valid hmac using system key, 
        // it still a valid message (with valid hmac) from client
        if !hmac_valid {
            let mut mac = HmacSha256::new_from_slice(hmac_client_key)
            .map_err(|_| DecodingError::IoError(Error::new(ErrorKind::InvalidInput, "HMAC key invalid")))?;
            mac.update(payload);
            hmac_valid = mac.verify_slice(received_hmac).is_ok();
        }
        
        let config = standard()
            .with_big_endian()
            .with_fixed_int_encoding();
        let (message, _): (RegisterCommand, usize) = decode_from_slice(payload, config)
            .map_err(|e| DecodingError::BincodeError(e))?;

        Ok((message, hmac_valid))
    }
    pub async fn serialize_register_command(
        cmd: &RegisterCommand,
        writer: &mut (dyn AsyncWrite + Send + Unpin),
        hmac_key: &[u8],
    ) -> Result<(), EncodingError> {
        serialize(cmd, writer, hmac_key).await
    }

    pub async fn serialize_client_response(
        cmd: &ClientCommandResponse,
        writer: &mut (dyn AsyncWrite + Send + Unpin),
        hmac_key: &[u8],
    ) -> Result<(), EncodingError> {
        serialize(cmd, writer, hmac_key).await
    }
}

pub mod register_client_public {
    use crate::SystemRegisterCommand;
    use std::sync::Arc;

    #[async_trait::async_trait]
    /// We do not need any public implementation of this trait. It is there for use
    /// in `AtomicRegister`. In our opinion it is a safe bet to say some structure of
    /// this kind must appear in your solution.
    pub trait RegisterClient: core::marker::Send + Sync {
        /// Sends a system message to a single process.
        async fn send(&self, msg: Send);

        /// Broadcasts a system message to all processes in the system, including self.
        async fn broadcast(&self, msg: Broadcast);
    }

    pub struct Broadcast {
        pub cmd: Arc<SystemRegisterCommand>,
    }

    pub struct Send {
        pub cmd: Arc<SystemRegisterCommand>,
        /// Identifier of the target process. Those start at 1.
        pub target: u8,
    }
}
