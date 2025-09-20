use libp2p::{dcutr, identify, identity::Keypair, mdns, relay, swarm::NetworkBehaviour};
use libp2p_stream as stream;

pub const BOOTSTRAP_URL: &str = "bootstrap.libp2p.io";

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
    pub stream: stream::Behaviour,
}

impl Behaviour {
    pub fn new(key: &Keypair, relay: relay::client::Behaviour) -> Result<Self, std::io::Error> {
        Ok(Self {
            identify: identify::Behaviour::new(identify::Config::new(
                String::from(PROTOCOL_NAME),
                key.public(),
            )),
            relay,
            dcutr: dcutr::Behaviour::new(key.public().to_peer_id()),
            mdns: mdns::tokio::Behaviour::new(mdns::Config::default(), key.public().to_peer_id())?,
            stream: stream::Behaviour::new(),
        })
    }
}
