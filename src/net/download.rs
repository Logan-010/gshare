use crate::{
    cli::{DownloadOpts, SwarmConfig},
    net::{BOOTSTRAP_URL, Behaviour, BehaviourEvent, PROTOCOL_GSHARE, util},
};
use blake3::Hash;
use color_eyre::eyre::{ContextCompat, bail};
use indicatif::{ProgressBar, ProgressStyle};
use libp2p::{
    Multiaddr, Stream, StreamProtocol, Swarm,
    futures::{AsyncReadExt, AsyncWriteExt, StreamExt},
    identify, mdns,
    multiaddr::Protocol,
    swarm::SwarmEvent,
};
use std::path::PathBuf;
use tokio::{fs::File, io::AsyncWriteExt as TokioWrite};

pub async fn task(
    config: SwarmConfig,
    opts: DownloadOpts,
    mut swarm: Swarm<Behaviour>,
) -> color_eyre::Result<()> {
    println!("dialing peer");

    if !config.local {
        swarm.dial(
            Multiaddr::empty()
                .with(Protocol::Dnsaddr(BOOTSTRAP_URL.into()))
                .with(Protocol::P2p(
                    opts.ticket
                        .relay_peer_id
                        .context("local ticket for a remote connection")?,
                ))
                .with(Protocol::P2pCircuit)
                .with(Protocol::P2p(opts.ticket.peer_id)),
        )?;
    }

    loop {
        match swarm.select_next_some().await {
            SwarmEvent::NewListenAddr { address, .. } => tracing::info!("listening on {}", address),
            SwarmEvent::Behaviour(BehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                for (_, address) in list {
                    swarm.dial(address)?;
                }
            }
            SwarmEvent::Behaviour(BehaviourEvent::Identify(identify::Event::Received {
                peer_id,
                ..
            })) => {
                tracing::info!("connected to {}", peer_id);

                if opts
                    .ticket
                    .relay_peer_id
                    .map(|p| p != peer_id)
                    .unwrap_or(true)
                {
                    println!("connected to {}", peer_id);
                }

                if config.local && peer_id == opts.ticket.peer_id {
                    println!("opening connection to {}", peer_id);

                    let stream = swarm
                        .behaviour()
                        .stream
                        .new_control()
                        .open_stream(peer_id, StreamProtocol::new(PROTOCOL_GSHARE))
                        .await?;

                    tracing::info!("handling open stream");

                    if let Err(e) = handle(stream, opts.to, opts.ticket.key).await {
                        tracing::error!("failed to handle file download: {}", e);
                        return Err(e);
                    }

                    break;
                }
            }
            SwarmEvent::Behaviour(BehaviourEvent::Dcutr(dcutr)) => {
                if let Err(e) = dcutr.result {
                    tracing::warn!("dcutr error: {}", e);
                } else {
                    let id = dcutr.remote_peer_id;
                    println!("opening connection");

                    let stream = swarm
                        .behaviour()
                        .stream
                        .new_control()
                        .open_stream(id, StreamProtocol::new(PROTOCOL_GSHARE))
                        .await?;

                    if let Err(e) = handle(stream, opts.to, opts.ticket.key).await {
                        tracing::error!("failed to handle file download: {}", e);
                        return Err(e);
                    }

                    break;
                }
            }
            ev => tracing::trace!("{:?}", ev),
        }
    }

    Ok(())
}

async fn handle(mut stream: Stream, to: Option<PathBuf>, key: [u8; 32]) -> color_eyre::Result<()> {
    stream.write_all(&key).await?;
    stream.flush().await?;

    tracing::trace!("wrote key");

    let mut hash_bytes = [0u8; 32];
    stream.read_exact(&mut hash_bytes).await?;

    tracing::trace!("read hash");

    let hash = Hash::from_bytes(hash_bytes);

    let mut size_bytes = [0u8; 8];
    stream.read_exact(&mut size_bytes).await?;

    tracing::trace!("read size");

    let size = u64::from_be_bytes(size_bytes);

    let mut name_length_bytes = [0u8; 8];
    stream.read_exact(&mut name_length_bytes).await?;

    tracing::trace!("read name length");

    let name_length = u64::from_be_bytes(name_length_bytes) as usize;

    let mut name_bytes = vec![0u8; name_length];
    stream.read_exact(&mut name_bytes).await?;

    tracing::trace!("read name");

    let name = String::from_utf8(name_bytes)?;

    let out_path = to.unwrap_or_default().join(name);

    let mut file = File::create_new(&out_path).await?;

    println!("downloading file to {}", out_path.display());

    let bar = ProgressBar::new(size);
    bar.set_style(
            ProgressStyle::default_bar()
                .template("{spinner} [{elapsed_precise}] [{wide_bar}] {percent_precise} ({bytes_per_sec}, {eta})")
                .unwrap()
                .progress_chars("#>-"),
        );

    let mut buf = vec![0u8; 2 * 1024 * 1024];
    loop {
        let read = stream.read(&mut buf).await?;

        if read == 0 {
            break;
        }

        file.write_all(&buf[..read]).await?;
        file.flush().await?;

        bar.inc(read as u64);
    }

    bar.finish_and_clear();

    println!("hashing file");

    let my_hash = util::hash_file(out_path).await?;

    if hash != my_hash {
        bail!("hashes do not match");
    }

    println!("done");

    Ok(())
}
