use crate::{
    cli::ShareOpts,
    net::{Behaviour, BehaviourEvent, PROTOCOL_GSHARE, util, words::WORDS},
};
use arboard::Clipboard;
use blockstore::{Blockstore, InMemoryBlockstore};
use cid::Cid;
use color_eyre::eyre::bail;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use libp2p::{
    Stream, StreamProtocol, Swarm,
    futures::{AsyncWriteExt, StreamExt},
    identify, kad, mdns,
    swarm::SwarmEvent,
};
use multihash_codetable::{Code, MultihashDigest};
use rand::seq::IndexedRandom;
use std::{path::Path, sync::Arc};
use tokio::{
    fs::{self, File},
    io::AsyncReadExt as TokioRead,
    task,
};

pub async fn task(
    opts: ShareOpts,
    mut swarm: Swarm<Behaviour>,
    blockstore: Arc<InMemoryBlockstore<64>>,
) -> color_eyre::Result<()> {
    swarm.behaviour_mut().kad.set_mode(Some(kad::Mode::Server));

    let mut incoming = swarm
        .behaviour()
        .stream
        .new_control()
        .accept(StreamProtocol::new(PROTOCOL_GSHARE))?;

    let path = Arc::new(opts.path);

    let tp = path.clone();
    task::spawn(async move {
        let progress = MultiProgress::new();

        while let Some((peer, stream)) = incoming.next().await {
            let p = tp.clone();

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
                if let Err(e) = handle(stream, &p, &progress).await {
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

    let keycode = WORDS
        .choose_multiple(&mut rand::rng(), 4)
        .copied()
        .collect::<Vec<&str>>()
        .join("-");

    let code = Code::Sha2_256;
    let hash = code.digest(keycode.as_bytes());
    let cid = Cid::new_v1(0x55, hash);

    blockstore.put_keyed(&cid, keycode.as_bytes()).await?;

    swarm
        .behaviour_mut()
        .kad
        .start_providing(kad::RecordKey::new(&cid.hash().to_bytes()))?;

    loop {
        match swarm.select_next_some().await {
            SwarmEvent::NewListenAddr { address, .. } => tracing::info!("listening on {}", address),
            SwarmEvent::Behaviour(BehaviourEvent::Identify(identify::Event::Received {
                peer_id,
                info: identify::Info { listen_addrs, .. },
                ..
            })) => {
                tracing::trace!("connected to {}", peer_id);

                for addr in listen_addrs {
                    swarm.behaviour_mut().kad.add_address(&peer_id, addr);
                }
            }
            SwarmEvent::Behaviour(BehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                for (peer, address) in list {
                    tracing::info!("discovered {}", peer);
                    swarm.behaviour_mut().kad.add_address(&peer, address);
                }
            }
            SwarmEvent::Behaviour(BehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
                for (peer, address) in list {
                    tracing::info!("peer {} expired", peer);
                    swarm.behaviour_mut().kad.remove_address(&peer, &address);
                }
            }
            SwarmEvent::Behaviour(BehaviourEvent::Dcutr(dcutr)) => match dcutr.result {
                Ok(_) => tracing::info!("connection made to {}", dcutr.remote_peer_id),
                Err(e) => tracing::warn!("hole punch error: {}", e),
            },
            SwarmEvent::Behaviour(BehaviourEvent::Kad(kad::Event::OutboundQueryProgressed {
                result,
                ..
            })) => match result {
                kad::QueryResult::StartProviding(Ok(_)) => {
                    let command = format!("gshare download {}", keycode);

                    println!("run:\n\t{}", command);

                    if opts.copy {
                        Clipboard::new()?.set_text(command)?;
                        println!("copied");
                    }
                }
                kad::QueryResult::StartProviding(Err(e)) => {
                    tracing::error!("failed to provide key: {}", e)
                }
                res => tracing::info!("{:?}", res),
            },
            ev => tracing::trace!("{:?}", ev),
        }
    }
}

async fn handle(mut stream: Stream, path: &Path, bar: &ProgressBar) -> color_eyre::Result<()> {
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
