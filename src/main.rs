use anyhow::Result;
use clap::{Parser, Subcommand};
use futures::StreamExt;
use tracing_subscriber::EnvFilter;
use walstream::config::S3Settings;
use walstream::storage::{build_s3_store, verify_store_contract};

#[derive(Debug, Parser)]
#[command(name = "walstream", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Validate object-store configuration and connectivity.
    VerifyStore(S3Settings),
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .init();

    match Cli::parse().command {
        Command::VerifyStore(settings) => {
            let store = build_s3_store(&settings)?;
            store.list(None).next().await.transpose()?;
            verify_store_contract(store.as_ref(), &settings.cluster_prefix()).await?;
            println!("object store supports required create and update preconditions");
        }
    }

    Ok(())
}
