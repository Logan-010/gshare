use clap::Parser;
use tokio::{select, signal, task};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

mod net;

mod cli;
use cli::{Cli, Mode};

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;

    let cli = Cli::parse();

    if tracing_subscriber::registry()
        .with(EnvFilter::new(cli.logging))
        .with(tracing_subscriber::fmt::layer())
        .try_init()
        .is_err()
    {
        eprintln!("failed to initialize logger");
    }

    let swarm = net::util::init_swarm(&cli.config).await?;

    let task = task::spawn(async move {
        match cli.mode {
            Mode::Share { inner } => net::share::task(cli.config.local, inner, swarm).await,
            Mode::Download { inner } => net::download::task(cli.config.local, inner, swarm).await,
        }
    });

    let ctrl_c = signal::ctrl_c();

    select! {
        res = task => res??,
        res = ctrl_c => res?
    }

    Ok(())
}
