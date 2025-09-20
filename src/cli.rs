use base64::{Engine, prelude::BASE64_STANDARD_NO_PAD};
use clap::{Args, Parser, Subcommand};
use libp2p::PeerId;
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, str::FromStr};

#[derive(Parser)]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(about = env!("CARGO_PKG_DESCRIPTION"))]
#[command(author = env!("CARGO_PKG_AUTHORS"))]
pub struct Cli {
    /// Sets custom logging level
    #[arg(long, required = false, env = "RUST_LOG", default_value_t = String::from("gshare=warn"))]
    pub logging: String,

    #[clap(flatten)]
    pub config: SwarmConfig,

    #[clap(subcommand)]
    pub mode: Mode,
}

#[derive(Parser, Clone)]
pub struct SwarmConfig {
    /// Enables Ipv6
    #[arg(long, default_value_t = false)]
    pub ipv6: bool,

    /// Port to listen on
    #[arg(long, default_value_t = 0)]
    pub port: u16,

    /// Save or load identity to file
    #[arg(long)]
    pub identity: Option<PathBuf>,

    /// Local discovery only
    #[arg(long)]
    pub local: bool,

    /// Use TCP transport as opposed to (default) QUIC
    /// * Sometimes TCP is faster in local mode
    #[arg(long)]
    pub tcp: bool,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Ticket {
    pub relay_peer_id: Option<PeerId>,
    pub peer_id: PeerId,
    pub key: [u8; 32],
}

impl Ticket {
    pub fn encode(&self) -> color_eyre::Result<String> {
        Ok(BASE64_STANDARD_NO_PAD.encode(postcard::to_stdvec(self)?))
    }

    pub fn decode(s: &str) -> color_eyre::Result<Self> {
        Ok(postcard::from_bytes(&BASE64_STANDARD_NO_PAD.decode(s)?)?)
    }
}

impl FromStr for Ticket {
    type Err = color_eyre::Report;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::decode(s)
    }
}

#[allow(clippy::large_enum_variant, reason = "needed for clap parsing")]
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
    /// Download ticket
    pub ticket: Ticket,

    /// Directory to save file to
    #[arg(long)]
    pub to: Option<PathBuf>,
}
