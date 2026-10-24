use anyhow::Result;
use tracing::warn;
use tracing_subscriber::EnvFilter;

use natter_server::Command;
use natter_server::config::{self, Cli, Settings};

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let cli = Cli::parse(&args)?;
    let command = Command::parse(&cli.command)?;
    if let Command::GenerateKeys = command {
        println!("{}", config::generate_keys());
        return Ok(());
    }
    let config = Settings::load(&cli)?;
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new(&config.log_filter)),
        )
        .init();
    for note in &config.notes {
        warn!("{note}");
    }
    match command {
        Command::GenerateKeys => unreachable!("handled before the configuration"),
        Command::Ports => println!("{}", config.ports()),
        Command::ListNodes => println!("{}", natter_server::list_nodes(&config).await?),
        Command::Admin(op) => println!("{}", natter_server::admin(&config, op)?),
        Command::Serve => natter_server::serve(config).await?,
    }
    Ok(())
}
