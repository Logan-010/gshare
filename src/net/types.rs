use blockstore::InMemoryBlockstore;
use libp2p::{dcutr, identify, identity::Keypair, kad, mdns, relay, swarm::NetworkBehaviour};
use libp2p_stream as stream;
use std::sync::Arc;

pub const BOOTSTRAP_URL: &str = "bootstrap.libp2p.io";
pub const BOOTNODES: [&str; 4] = [
    "QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
    "QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
    "QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
    "QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
];

const PROTOCOL_NAME: &str = concat!("/", env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"));
pub const PROTOCOL_GSHARE: &str = concat!(
    "/",
    env!("CARGO_PKG_NAME"),
    "/share/",
    env!("CARGO_PKG_VERSION")
);

#[derive(NetworkBehaviour)]
pub struct Behaviour {
    identify: identify::Behaviour,
    relay: relay::client::Behaviour,
    dcutr: dcutr::Behaviour,
    mdns: mdns::tokio::Behaviour,
    pub bitswap: beetswap::Behaviour<64, InMemoryBlockstore<64>>,
    pub kad: kad::Behaviour<kad::store::MemoryStore>,
    pub stream: stream::Behaviour,
}

impl Behaviour {
    pub fn new(
        key: &Keypair,
        relay: relay::client::Behaviour,
        blockstore: Arc<InMemoryBlockstore<64>>,
    ) -> Result<Self, std::io::Error> {
        Ok(Self {
            identify: identify::Behaviour::new(identify::Config::new(
                String::from(PROTOCOL_NAME),
                key.public(),
            )),
            relay,
            dcutr: dcutr::Behaviour::new(key.public().to_peer_id()),
            mdns: mdns::tokio::Behaviour::new(mdns::Config::default(), key.public().to_peer_id())?,
            bitswap: beetswap::Behaviour::new(blockstore),
            kad: kad::Behaviour::new(
                key.public().to_peer_id(),
                kad::store::MemoryStore::new(key.public().to_peer_id()),
            ),
            stream: stream::Behaviour::new(),
        })
    }
}
