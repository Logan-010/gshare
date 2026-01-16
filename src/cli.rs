use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(about = env!("CARGO_PKG_DESCRIPTION"))]
#[command(author = env!("CARGO_PKG_AUTHORS"))]
pub struct Cli {
    /// Sets custom logging level
    #[arg(long, required = false, env = "RUST_LOG", default_value_t = String::from("gshare=warn"))]
    pub logging: String,

    #[clap(subcommand)]
    pub mode: Mode,
}

#[derive(Subcommand)]
pub enum Mode {
    /// Share a file
    Share {
        #[command(flatten)]
        inner: ShareOpts,
    },
    /// Download a file
    Download {
        #[command(flatten)]
        inner: DownloadOpts,
    },
}

#[derive(Args)]
pub struct ShareOpts {
    /// Path of file to share
    pub path: PathBuf,

    /// Copies command to clipboard
    #[arg(long)]
    pub copy: bool,
}

#[derive(Args)]
pub struct DownloadOpts {
    /// Download code
    pub code: String,

    /// Directory to save file to
    #[arg(long)]
    pub to: Option<PathBuf>,
}
