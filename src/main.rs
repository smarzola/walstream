use std::net::SocketAddr;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use futures::StreamExt;
use tokio::{net::TcpListener, signal};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;
use walstream::config::S3Settings;
use walstream::log::LogEngine;
use walstream::protocol::BrokerIdentity;
use walstream::server::{DEFAULT_MAX_FRAME_BYTES, serve, validate_max_frame_bytes};
use walstream::storage::{build_s3_store, verify_store_contract};

#[derive(Debug, Parser)]
#[command(name = "walstream", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Serve the bounded Kafka-compatible API.
    Serve(ServeSettings),

    /// Validate object-store configuration and connectivity.
    VerifyStore(S3Settings),
}

#[derive(Debug, Args)]
struct ServeSettings {
    #[command(flatten)]
    s3: S3Settings,

    /// TCP address on which the broker accepts Kafka requests.
    #[arg(long, env = "WALSTREAM_LISTEN", default_value = "0.0.0.0:9092")]
    listen: SocketAddr,

    /// Hostname or IP returned to Kafka clients in metadata.
    #[arg(long, env = "WALSTREAM_ADVERTISED_HOST", default_value = "127.0.0.1")]
    advertised_host: String,

    /// Port returned to Kafka clients. Defaults to the bound listener port.
    #[arg(long, env = "WALSTREAM_ADVERTISED_PORT")]
    advertised_port: Option<u16>,

    /// Maximum request bytes, excluding the four-byte Kafka length prefix.
    #[arg(
        long,
        env = "WALSTREAM_MAX_FRAME_BYTES",
        default_value_t = DEFAULT_MAX_FRAME_BYTES
    )]
    max_frame_bytes: usize,
}

impl ServeSettings {
    fn preflight(&self) -> Result<BrokerIdentity> {
        self.s3.validate()?;
        validate_max_frame_bytes(self.max_frame_bytes)?;
        let identity = BrokerIdentity {
            host: self.advertised_host.clone(),
            port: self.advertised_port.unwrap_or(self.listen.port()),
            cluster_id: self.s3.cluster_id.clone(),
        };
        identity.validate()?;
        Ok(identity)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .init();

    match Cli::parse().command {
        Command::Serve(settings) => {
            // Reject all locally knowable configuration errors before any
            // credentialed object-store request or socket bind.
            let identity = settings.preflight()?;
            let store = build_s3_store(&settings.s3)?;
            verify_store_contract(store.as_ref(), &settings.s3.cluster_prefix())
                .await
                .context("object store does not satisfy Walstream's conditional-write contract")?;
            let engine = LogEngine::new(store, settings.s3.cluster_prefix())?;
            let listener = TcpListener::bind(settings.listen)
                .await
                .with_context(|| format!("bind Kafka listener {}", settings.listen))?;
            let bound = listener.local_addr()?;
            info!(listen = %bound, advertised_host = %identity.host, advertised_port = identity.port, "Walstream is ready");
            serve(
                listener,
                engine,
                identity,
                settings.max_frame_bytes,
                async {
                    if let Err(error) = signal::ctrl_c().await {
                        error!(%error, "failed to listen for shutdown signal");
                    }
                },
            )
            .await?;
        }
        Command::VerifyStore(settings) => {
            let store = build_s3_store(&settings)?;
            store.list(None).next().await.transpose()?;
            verify_store_contract(store.as_ref(), &settings.cluster_prefix()).await?;
            println!("object store supports required create and update preconditions");
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings() -> ServeSettings {
        ServeSettings {
            s3: S3Settings {
                bucket: "walstream-test".into(),
                region: "us-east-1".into(),
                endpoint: None,
                allow_http: false,
                prefix: "walstream".into(),
                cluster_id: "test".into(),
            },
            listen: "127.0.0.1:9092".parse().unwrap(),
            advertised_host: "localhost".into(),
            advertised_port: None,
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
        }
    }

    #[test]
    fn serve_preflight_rejects_identity_and_unusable_frame_bounds() {
        let mut value = settings();
        value.advertised_host.clear();
        assert!(value.preflight().is_err());

        let mut value = settings();
        value.listen.set_port(0);
        assert!(value.preflight().is_err());

        for maximum in [8, 9] {
            let mut value = settings();
            value.max_frame_bytes = maximum;
            assert!(value.preflight().is_err());
        }
    }
}
