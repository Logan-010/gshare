use crate::net::{BOOTNODES, BOOTSTRAP_URL, Behaviour};
use blake3::{Hash, Hasher};
use blockstore::InMemoryBlockstore;
use libp2p::{Multiaddr, PeerId, Swarm, SwarmBuilder, multiaddr::Protocol, noise, tcp, yamux};
use std::{
    net::{Ipv4Addr, Ipv6Addr},
    path::PathBuf,
    sync::Arc,
};
use tokio::task;

pub async fn init_swarm() -> color_eyre::Result<(Swarm<Behaviour>, Arc<InMemoryBlockstore<64>>)> {
    let blockstore = Arc::new(InMemoryBlockstore::new());

    let mut swarm = SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::new().nodelay(true),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_quic()
        .with_dns()?
        .with_relay_client(noise::Config::new, yamux::Config::default)?
        .with_behaviour(|key, relay| Ok(Behaviour::new(key, relay, blockstore.clone())?))?
        .build();

    tracing::info!("local peer id: {}", swarm.local_peer_id());

    for peer in BOOTNODES {
        let id = peer.parse::<PeerId>().unwrap();

        let addr = Multiaddr::empty().with(Protocol::Dnsaddr(BOOTSTRAP_URL.into()));

        swarm.behaviour_mut().kad.add_address(&id, addr.clone());

        swarm.listen_on(addr.with(Protocol::P2p(id)).with(Protocol::P2pCircuit))?;
    }

    swarm.listen_on(
        Multiaddr::empty()
            .with(Protocol::Ip4(Ipv4Addr::UNSPECIFIED))
            .with(Protocol::Tcp(0)),
    )?;
    swarm.listen_on(
        Multiaddr::empty()
            .with(Protocol::Ip6(Ipv6Addr::UNSPECIFIED))
            .with(Protocol::Tcp(0)),
    )?;
    swarm.listen_on(
        Multiaddr::empty()
            .with(Protocol::Ip4(Ipv4Addr::UNSPECIFIED))
            .with(Protocol::Udp(0))
            .with(Protocol::QuicV1),
    )?;
    swarm.listen_on(
        Multiaddr::empty()
            .with(Protocol::Ip6(Ipv6Addr::UNSPECIFIED))
            .with(Protocol::Udp(0))
            .with(Protocol::QuicV1),
    )?;

    Ok((swarm, blockstore))
}

pub async fn hash_file(path: PathBuf) -> color_eyre::Result<Hash> {
    Ok(
        task::spawn_blocking(|| Hasher::new().update_mmap_rayon(path).map(|h| h.finalize()))
            .await??,
    )
}
