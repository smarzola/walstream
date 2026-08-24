//! Tokio TCP server and Kafka frame transport.

use std::{future::Future, io, sync::Arc};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinSet,
};
use tracing::{debug, warn};

use crate::{
    log::LogEngine,
    protocol::{BrokerIdentity, handle_request},
};

/// Default maximum Kafka request frame: 16 MiB, excluding its length prefix.
pub const DEFAULT_MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
/// Smallest request containing the fixed header and a nullable client ID.
pub const MIN_FRAME_BYTES: usize = 10;

/// Validate the local request-frame bound without starting external work.
pub fn validate_max_frame_bytes(max_frame_bytes: usize) -> Result<()> {
    if !(MIN_FRAME_BYTES..=i32::MAX as usize).contains(&max_frame_bytes) {
        bail!(
            "max frame bytes must be between {MIN_FRAME_BYTES} and {}",
            i32::MAX
        );
    }
    Ok(())
}

/// Serve Kafka connections until `shutdown` resolves.
pub async fn serve<F>(
    listener: TcpListener,
    engine: LogEngine,
    identity: BrokerIdentity,
    max_frame_bytes: usize,
    shutdown: F,
) -> Result<()>
where
    F: Future<Output = ()>,
{
    identity.validate()?;
    validate_max_frame_bytes(max_frame_bytes)?;

    let engine = Arc::new(engine);
    let identity = Arc::new(identity);
    let mut connections = JoinSet::new();
    tokio::pin!(shutdown);

    loop {
        while let Some(result) = connections.try_join_next() {
            report_connection_result(result);
        }

        tokio::select! {
            _ = &mut shutdown => break,
            accepted = listener.accept() => {
                let (stream, peer) = accepted.context("accept Kafka connection")?;
                let engine = Arc::clone(&engine);
                let identity = Arc::clone(&identity);
                connections.spawn(async move {
                    debug!(%peer, "accepted Kafka connection");
                    let result = serve_connection(stream, &engine, &identity, max_frame_bytes).await;
                    if let Err(error) = &result {
                        warn!(%peer, %error, "closing Kafka connection");
                    }
                    result
                });
            }
        }
    }

    connections.abort_all();
    while let Some(result) = connections.join_next().await {
        if !matches!(result, Err(ref error) if error.is_cancelled()) {
            report_connection_result(result);
        }
    }
    Ok(())
}

fn report_connection_result(result: Result<Result<()>, tokio::task::JoinError>) {
    match result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => warn!(%error, "Kafka connection ended with an error"),
        Err(error) if error.is_cancelled() => {}
        Err(error) => warn!(%error, "Kafka connection task failed"),
    }
}

async fn serve_connection(
    mut stream: TcpStream,
    engine: &LogEngine,
    identity: &BrokerIdentity,
    max_frame_bytes: usize,
) -> Result<()> {
    loop {
        let mut length = [0_u8; 4];
        match stream.read_exact(&mut length).await {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(error) => return Err(error).context("read Kafka frame length"),
        }
        let length = i32::from_be_bytes(length);
        let length = usize::try_from(length)
            .ok()
            .filter(|length| *length <= max_frame_bytes)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Kafka frame length {length} is invalid or exceeds {max_frame_bytes} bytes"
                )
            })?;

        let mut frame = vec![0_u8; length];
        stream
            .read_exact(&mut frame)
            .await
            .context("read Kafka frame body")?;
        if let Some(response) = handle_request(Bytes::from(frame), engine, identity).await? {
            let response_length = i32::try_from(response.len())
                .context("Kafka response exceeds signed 32-bit frame size")?;
            stream
                .write_all(&response_length.to_be_bytes())
                .await
                .context("write Kafka response length")?;
            stream
                .write_all(&response)
                .await
                .context("write Kafka response body")?;
        }
    }
}
