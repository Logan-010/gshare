use crate::{
    cli::DownloadOpts,
    net::{Behaviour, BehaviourEvent, PROTOCOL_GSHARE, util},
};
use blake3::Hash;
use cid::Cid;
use color_eyre::eyre::bail;
use indicatif::{ProgressBar, ProgressStyle};
use libp2p::{
    PeerId, Stream, StreamProtocol, Swarm,
    futures::{AsyncReadExt, StreamExt},
    identify, kad, mdns,
    swarm::SwarmEvent,
};
use multihash_codetable::{Code, MultihashDigest};
use std::{collections::HashSet, path::PathBuf};
use tokio::{fs::File, io::AsyncWriteExt as TokioWrite};

pub async fn task(opts: DownloadOpts, mut swarm: Swarm<Behaviour>) -> color_eyre::Result<()> {
    println!("finding peer");

    let code = Code::Sha2_256;
    let hash = code.digest(opts.code.as_bytes());
    let cid = Cid::new_v1(0x55, hash);

    swarm
        .behaviour_mut()
        .kad
        .get_providers(kad::RecordKey::new(&cid.hash().to_bytes()));

    let mut target = PeerId::random();
    let mut local_peers = HashSet::new();
    loop {
        match swarm.select_next_some().await {
            SwarmEvent::NewListenAddr { address, .. } => tracing::info!("listening on {}", address),
            SwarmEvent::Behaviour(BehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                for (peer, address) in list {
                    tracing::info!("discovered peer {}", peer);
                    local_peers.insert(peer);
                    swarm.behaviour_mut().kad.add_address(&peer, address);
                }
            }
            SwarmEvent::Behaviour(BehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
                for (peer, address) in list {
                    tracing::info!("peer {} expired", peer);
                    local_peers.remove(&peer);
                    swarm.behaviour_mut().kad.remove_address(&peer, &address);
                }
            }
            SwarmEvent::Behaviour(BehaviourEvent::Identify(identify::Event::Received {
                peer_id,
                info: identify::Info { listen_addrs, .. },
                ..
            })) => {
                tracing::trace!("connected to {}", peer_id);

                for addr in listen_addrs {
                    swarm.behaviour_mut().kad.add_address(&peer_id, addr);
                }

                if target == peer_id {
                    println!("opening connection");

                    let stream = swarm
                        .behaviour()
                        .stream
                        .new_control()
                        .open_stream(peer_id, StreamProtocol::new(PROTOCOL_GSHARE))
                        .await?;

                    if let Err(e) = handle(stream, opts.to).await {
                        tracing::error!("failed to handle file download: {}", e);
                        return Err(e);
                    }

                    break;
                }
            }
            SwarmEvent::Behaviour(BehaviourEvent::Dcutr(dcutr)) => match dcutr.result {
                Ok(_) => tracing::info!("opened connection to {}", dcutr.remote_peer_id),
                Err(e) => tracing::warn!(
                    "failed to hole punch connection to {}, {}",
                    dcutr.remote_peer_id,
                    e
                ),
            },
            SwarmEvent::Behaviour(BehaviourEvent::Kad(kad::Event::OutboundQueryProgressed {
                result,
                ..
            })) => match result {
                kad::QueryResult::GetProviders(Ok(
                    kad::GetProvidersOk::FinishedWithNoAdditionalRecord { .. },
                )) => {
                    tracing::debug!("no providers found");
                    swarm
                        .behaviour_mut()
                        .kad
                        .get_providers(kad::RecordKey::new(&cid.hash().to_bytes()));
                }
                kad::QueryResult::GetProviders(Ok(kad::GetProvidersOk::FoundProviders {
                    providers,
                    ..
                })) => {
                    if let Some(peer) = providers.iter().next().cloned() {
                        tracing::info!("found provider {}", peer);

                        target = peer;

                        if swarm.is_connected(&peer) {
                            println!("opening connection");

                            let stream = swarm
                                .behaviour()
                                .stream
                                .new_control()
                                .open_stream(peer, StreamProtocol::new(PROTOCOL_GSHARE))
                                .await?;

                            if let Err(e) = handle(stream, opts.to).await {
                                tracing::error!("failed to handle file download: {}", e);
                                return Err(e);
                            }

                            break;
                        } else if local_peers.contains(&peer) {
                            swarm.dial(peer)?;
                        } else {
                            swarm.behaviour_mut().kad.get_closest_peers(peer);
                        }
                    }
                }
                kad::QueryResult::GetProviders(Err(e)) => {
                    tracing::error!("get providers error {}", e);
                    swarm
                        .behaviour_mut()
                        .kad
                        .get_providers(kad::RecordKey::new(&cid.hash().to_bytes()));
                }
                kad::QueryResult::GetClosestPeers(Ok(kad::GetClosestPeersOk { peers, .. })) => {
                    if peers.iter().any(|p| p.peer_id == target) {
                        swarm.dial(target)?;
                    }
                }
                kad::QueryResult::GetClosestPeers(Err(e)) => {
                    tracing::error!("failed to get closest peers: {}", e)
                }
                res => tracing::debug!("query result: {:?}", res),
            },
            ev => tracing::trace!("{:?}", ev),
        }
    }

    Ok(())
}

async fn handle(mut stream: Stream, to: Option<PathBuf>) -> color_eyre::Result<()> {
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
