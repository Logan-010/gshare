use crate::{
    cli::{ShareOpts, SwarmConfig, Ticket},
    net::{BOOTSTRAP_URL, Behaviour, BehaviourEvent, PROTOCOL_GSHARE, util},
};
use arboard::Clipboard;
use color_eyre::eyre::bail;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use libp2p::{
    Multiaddr, Stream, StreamProtocol, Swarm,
    futures::{AsyncReadExt, AsyncWriteExt, StreamExt},
    identify,
    multiaddr::Protocol,
    swarm::SwarmEvent,
};
use std::{path::Path, sync::Arc};
use tokio::{
    fs::{self, File},
    io::AsyncReadExt as TokioRead,
    task,
};

pub async fn task(
    config: SwarmConfig,
    opts: ShareOpts,
    mut swarm: Swarm<Behaviour>,
) -> color_eyre::Result<()> {
    let mut incoming = swarm
        .behaviour()
        .stream
        .new_control()
        .accept(StreamProtocol::new(PROTOCOL_GSHARE))?;

    let path = Arc::new(opts.path);
    let key = Arc::new(rand::random::<[u8; 32]>());

    let tp = path.clone();
    let tk = key.clone();
    task::spawn(async move {
        let progress = MultiProgress::new();

        while let Some((peer, stream)) = incoming.next().await {
            let p = tp.clone();
            let k = tk.clone();

            let pr = progress.clone();

            tracing::info!("incoming connection from {}", peer);
            task::spawn(async move {
                let pb = ProgressBar::no_length();
                pb.set_style(
                    ProgressStyle::default_bar()
                        .template("{spinner} [{elapsed_precise}] [{wide_bar}] {percent_precise} ({bytes_per_sec}, {eta})")
                        .unwrap()
                        .progress_chars("#>-"),
                );

                let progress = pr.add(pb);

                tracing::info!("sending file to {}", peer);
                if let Err(e) = handle(stream, &p, &k, &progress).await {
                    progress.finish_and_clear();
                    progress.force_draw();
                    pr.remove(&progress);

                    tracing::warn!("failed to serve file: {}", e);
                } else {
                    progress.finish_and_clear();
                    progress.force_draw();
                    pr.remove(&progress);
                }
            });
        }

        tracing::error!("file handler closed");
    });

    if !config.local {
        println!("dialing relay");
        swarm.dial(Multiaddr::empty().with(Protocol::Dnsaddr(BOOTSTRAP_URL.into())))?;
    } else {
        let ticket = Ticket {
            relay_peer_id: None,
            peer_id: *swarm.local_peer_id(),
            key: *key,
        };

        let command = format!(
            "gshare {}--local download {}",
            match config.tcp {
                true => "--tcp ",
                false => "",
            },
            ticket.encode()?
        );

        println!("run:\n\t{}", command);

        if opts.copy {
            Clipboard::new()?.set_text(command)?;
            println!("copied");
        }
    }

    let mut connected_to_relay = config.local;
    loop {
        match swarm.select_next_some().await {
            SwarmEvent::NewListenAddr { address, .. } => tracing::info!("listening on {}", address),
            SwarmEvent::Behaviour(BehaviourEvent::Identify(identify::Event::Received {
                peer_id,
                ..
            })) => {
                tracing::info!("connected to {}", peer_id);

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
                        key: *key,
                    };

                    let command = format!(
                        "gshare {}download {}",
                        match config.tcp {
                            true => "--tcp ",
                            false => "",
                        },
                        ticket.encode()?
                    );

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
                Ok(_) => tracing::info!("connection made to {}", dcutr.remote_peer_id),
                Err(e) => tracing::warn!("hole punch error: {}", e),
            },
            ev => tracing::trace!("{:?}", ev),
        }
    }
}

async fn handle(
    mut stream: Stream,
    path: &Path,
    key: &[u8; 32],
    bar: &ProgressBar,
) -> color_eyre::Result<()> {
    let mut peer_key = [0u8; 32];
    stream.read_exact(&mut peer_key).await?;

    tracing::trace!("read key");

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

    bar.set_length(size);

    stream.write_all(hash.as_bytes()).await?;
    stream.flush().await?;

    tracing::trace!("wrote hash");

    stream.write_all(&size.to_be_bytes()).await?;
    stream.flush().await?;

    tracing::trace!("wrote size");

    stream.write_all(&(name.len() as u64).to_be_bytes()).await?;
    stream.flush().await?;
    stream.write_all(name.as_bytes()).await?;
    stream.flush().await?;

    tracing::trace!("wrote name");

    let mut file = File::open(path).await?;

    let mut buf = vec![0u8; 2 * 1024 * 1024];
    loop {
        let read = file.read(&mut buf).await?;

        if read == 0 {
            break;
        }

        stream.write_all(&buf[..read]).await?;
        stream.flush().await?;

        bar.inc(read as u64);
    }

    Ok(())
}
