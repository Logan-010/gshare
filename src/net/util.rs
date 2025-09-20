use crate::{cli::SwarmConfig, net::Behaviour};
use blake3::{Hash, Hasher};
use libp2p::{
    Multiaddr, Swarm, SwarmBuilder,
    identity::{Keypair, ed25519},
    multiaddr::Protocol,
    noise, tcp, yamux,
};
use std::{
    net::{Ipv4Addr, Ipv6Addr},
    path::{Path, PathBuf},
};
use tokio::{fs, task};

async fn create_or_load_identity<P: AsRef<Path>>(path: Option<P>) -> color_eyre::Result<Keypair> {
    match path {
        Some(p) => {
            if p.as_ref().exists() {
                let mut content = fs::read(p).await?;

                let key = ed25519::Keypair::try_from_bytes(&mut content)?;

                Ok(Keypair::from(key))
            } else {
                let key = ed25519::Keypair::generate();

                fs::write(p, &key.to_bytes()).await?;

                Ok(Keypair::from(key))
            }
        }
        None => Ok(Keypair::generate_ed25519()),
    }
}

pub async fn init_swarm(config: &SwarmConfig) -> color_eyre::Result<Swarm<Behaviour>> {
    let identity = create_or_load_identity(config.identity.as_ref()).await?;

    let mut swarm = SwarmBuilder::with_existing_identity(identity)
        .with_tokio()
        .with_tcp(
            tcp::Config::new().nodelay(true),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_quic()
        .with_dns()?
        .with_relay_client(noise::Config::new, yamux::Config::default)?
        .with_behaviour(|key, relay| Ok(Behaviour::new(key, relay)?))?
        .build();

    tracing::info!("local peer id: {}", swarm.local_peer_id());

    if config.tcp {
        swarm.listen_on(
            Multiaddr::empty()
                .with(match config.ipv6 {
                    true => Protocol::Ip6(Ipv6Addr::UNSPECIFIED),
                    false => Protocol::Ip4(Ipv4Addr::UNSPECIFIED),
                })
                .with(Protocol::Tcp(config.port)),
        )?;
    } else {
        swarm.listen_on(
            Multiaddr::empty()
                .with(match config.ipv6 {
                    true => Protocol::Ip6(Ipv6Addr::UNSPECIFIED),
                    false => Protocol::Ip4(Ipv4Addr::UNSPECIFIED),
                })
                .with(Protocol::Udp(config.port))
                .with(Protocol::QuicV1),
        )?;
    }

    Ok(swarm)
}

pub async fn hash_file(path: PathBuf) -> color_eyre::Result<Hash> {
    Ok(
        task::spawn_blocking(|| Hasher::new().update_mmap_rayon(path).map(|h| h.finalize()))
            .await??,
    )
}
