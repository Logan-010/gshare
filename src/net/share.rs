use crate::{
    cli::{ShareOpts, Ticket},
    net::{BOOTSTRAP_URL, Behaviour, BehaviourEvent, PROTOCOL_GSHARE, util},
};
use arboard::Clipboard;
use color_eyre::eyre::bail;
use indicatif::{ProgressBar, ProgressStyle};
use libp2p::{
    Multiaddr, Stream, StreamProtocol, Swarm,
    futures::{AsyncReadExt, AsyncWriteExt, StreamExt},
    identify,
    multiaddr::Protocol,
    swarm::SwarmEvent,
};
use std::path::Path;
use tokio::{
    fs::{self, File},
    io::AsyncReadExt as TokioRead,
    task,
};

pub async fn task(
    local: bool,
    opts: ShareOpts,
    mut swarm: Swarm<Behaviour>,
) -> color_eyre::Result<()> {
    let mut incoming = swarm
        .behaviour()
        .stream
        .new_control()
        .accept(StreamProtocol::new(PROTOCOL_GSHARE))?;

    let path = opts.path;
    let key = rand::random();

    task::spawn(async move {
        let path = path.clone();
        let key = key;
        while let Some((peer, stream)) = incoming.next().await {
            println!("sending file to {}", peer);
            if let Err(e) = handle(stream, &path, &key).await {
                tracing::warn!("failed to serve file: {}", e);
            }
        }

        tracing::warn!("file handler closed");
    });

    if !local {
        println!("dialing relay");
        swarm.dial(Multiaddr::empty().with(Protocol::Dnsaddr(BOOTSTRAP_URL.into())))?;
    } else {
        let ticket = Ticket {
            relay_peer_id: None,
            peer_id: *swarm.local_peer_id(),
            key,
        };

        let command = format!("gshare --local download {}", ticket.encode()?);

        println!("run:\n\t{}", command);

        if opts.copy {
            Clipboard::new()?.set_text(command)?;
            println!("copied");
        }
    }

    let mut connected_to_relay = local;
    loop {
        match swarm.select_next_some().await {
            SwarmEvent::NewListenAddr { address, .. } => tracing::info!("listening on {}", address),
            SwarmEvent::Behaviour(BehaviourEvent::Identify(identify::Event::Received {
                peer_id,
                ..
            })) => {
                if !connected_to_relay {
                    swarm.listen_on(
                        Multiaddr::empty()
                            .with(Protocol::Dnsaddr(BOOTSTRAP_URL.into()))
                            .with(Protocol::P2p(peer_id))
                            .with(Protocol::P2pCircuit),
                    )?;

                    let ticket = Ticket {
                        relay_peer_id: Some(peer_id),
                        peer_id: *swarm.local_peer_id(),
                        key,
                    };

                    let command = format!("gshare download {}", ticket.encode()?);

                    println!("run:\n\t{}", command);

                    if opts.copy {
                        Clipboard::new()?.set_text(command)?;
                        println!("copied");
                    }

                    connected_to_relay = true;
                }

                tracing::info!("connected to {}", peer_id);
            }
            SwarmEvent::Behaviour(BehaviourEvent::Dcutr(dcutr)) => match dcutr.result {
                Ok(_) => tracing::info!("connection make to {}", dcutr.remote_peer_id),
                Err(e) => tracing::warn!("dcutr error: {}", e),
            },
            ev => tracing::trace!("{:?}", ev),
        }
    }
}

async fn handle(mut stream: Stream, path: &Path, key: &[u8; 32]) -> color_eyre::Result<()> {
    let mut peer_key = [0u8; 32];
    stream.read_exact(&mut peer_key).await?;

    if &peer_key != key {
        bail!("invalid key from peer");
    }

    let Some(name) = path
        .file_name()
        .and_then(|o| o.to_str())
        .map(|s| s.to_string())
    else {
        bail!("invalid file name")
    };

    let hash = util::hash_file(path.to_owned()).await?;

    let size = fs::metadata(path).await?.len();

    stream.write_all(hash.as_bytes()).await?;
    stream.flush().await?;

    stream.write_all(&size.to_be_bytes()).await?;
    stream.flush().await?;

    stream.write_all(&(name.len() as u64).to_be_bytes()).await?;
    stream.flush().await?;
    stream.write_all(name.as_bytes()).await?;
    stream.flush().await?;

    let mut file = File::open(path).await?;

    let bar = ProgressBar::new(size);
    bar.set_style(
            ProgressStyle::default_bar()
                .template("{spinner} [{elapsed_precise}] [{wide_bar}] {percent_precise} ({bytes_per_sec}, {eta})")
                .unwrap()
                .progress_chars("#>-"),
        );

    let mut buf = vec![0u8; 2 * 1024 * 1024];
    loop {
        let read = file.read(&mut buf).await?;

        if read == 0 {
            bar.finish();
            break;
        }

        stream.write_all(&buf[..read]).await?;
        stream.flush().await?;

        bar.inc(read as u64);
    }

    Ok(())
}
