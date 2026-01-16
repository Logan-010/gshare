mod net;

mod cli;
use cli::{Cli, Mode};

use clap::Parser;
use tokio::{select, signal, task};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;

    let cli = Cli::parse();

    if tracing_subscriber::registry()
        .with(EnvFilter::new(&cli.logging))
        .with(tracing_subscriber::fmt::layer())
        .try_init()
        .is_err()
    {
        eprintln!("failed to initialize logger");
    }

    let (swarm, blockstore) = net::util::init_swarm().await?;

    let token = CancellationToken::new();

    let t = token.child_token();
    let task = task::spawn(async move {
        match cli.mode {
            Mode::Share { inner } => {
                select! {
                    _ = t.cancelled() => Ok(()),
                    task_res = net::share::task(inner, swarm, blockstore) => task_res
                }
            }
            Mode::Download { inner } => {
                select! {
                    _ = t.cancelled() => Ok(()),
                    task_res = net::download::task(inner, swarm) => task_res
                }
            }
        }
    });

    let ctrl_c = signal::ctrl_c();

    select! {
        res = task => res??,
        res = ctrl_c => res?
    }

    println!("quitting...");

    token.cancel();

    Ok(())
}
