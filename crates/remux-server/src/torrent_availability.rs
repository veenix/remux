use std::{
    collections::{HashMap, HashSet},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use futures_util::{StreamExt, stream};
use remux_utils::Store;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpStream, UdpSocket},
    time::timeout,
};
use url::Url;

const UDP_TRACKER_CONNECT_MAGIC: u64 = 0x4172_7101_980;
const ACTION_CONNECT: u32 = 0;
const ACTION_ANNOUNCE: u32 = 1;
const ACTION_ERROR: u32 = 3;
const MAX_HASHES_PER_TRACKER: usize = 50;
const TRACKER_TIMEOUT: Duration = Duration::from_millis(900);
const ANNOUNCE_TIMEOUT: Duration = Duration::from_millis(1400);
const ANNOUNCE_NUM_WANT: i32 = 100;
const MAX_PEERS_PER_SWARM: usize = 100;
const CACHE_TTL: Duration = Duration::from_secs(45);
const FAILURE_CACHE_TTL: Duration = Duration::from_secs(10);
const MAX_TRACKERS: usize = 3;
const MAX_ACTIVE_PEER_PROBES: usize = 12;
const PEER_CONNECT_TIMEOUT: Duration = Duration::from_millis(700);
const PEER_MESSAGE_TIMEOUT: Duration = Duration::from_millis(1400);
const MAX_PEER_MESSAGE_BYTES: usize = 256 * 1024;
const PEER_PROBE_CACHE_TTL: Duration = Duration::from_secs(20);

#[derive(Clone, Debug, Default)]
pub(crate) struct TrackerAvailability {
    pub seeders: u32,
    pub leechers: u32,
    pub completed: u32,
    pub responses: u16,
    pub peers: Vec<SocketAddr>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct PeerAvailability {
    /// Peers that completed a valid BitTorrent handshake right now.
    pub responsive: usize,
    /// Responsive peers that advertised a complete piece bitfield (`have all`).
    pub seeders: usize,
    /// Responsive peers, with complete seeders first. Passing these addresses
    /// into the real torrent session avoids repeating tracker/DHT discovery and
    /// then trying the same dead peers again on the latency-sensitive path.
    pub peers: Vec<SocketAddr>,
}

impl TrackerAvailability {
    fn merge(&mut self, other: &Self) {
        // The same swarm is commonly announced to multiple trackers, so summing
        // counts would double-count peers. The maximum is a better availability
        // signal for ranking.
        self.seeders = self
            .seeders
            .max(other.seeders);
        self.leechers = self
            .leechers
            .max(other.leechers);
        self.completed = self
            .completed
            .max(other.completed);
        self.responses = self
            .responses
            .saturating_add(other.responses);
        for peer in &other.peers {
            if self
                .peers
                .len()
                >= MAX_PEERS_PER_SWARM
            {
                break;
            }
            if !self
                .peers
                .contains(peer)
            {
                self.peers
                    .push(*peer);
            }
        }
    }
}

/// Batch-query UDP trackers for many info hashes without adding torrents,
/// resolving metadata, allocating files, or downloading pieces. Announce
/// responses provide both swarm counts and peer addresses; the latter let the
/// metadata-only preflight skip slow tracker/DHT discovery. Results are cached
/// briefly because Auto can be resolved more than once during startup.
pub(crate) async fn scrape_torrents(
    store: &Store,
    info_hashes: &[String],
    trackers: &[String],
) -> HashMap<String, TrackerAvailability> {
    let mut unique = Vec::new();
    let mut seen = HashSet::new();
    let mut results = HashMap::new();

    for hash in info_hashes {
        let normalized = hash.to_ascii_lowercase();
        if !seen.insert(normalized.clone()) || decode_info_hash(&normalized).is_none() {
            continue;
        }
        let cache_key = cache_key(&normalized);
        if let Some(cached) = store.get::<TrackerAvailability>(&cache_key) {
            results.insert(normalized, (*cached).clone());
        } else {
            unique.push(normalized);
        }
    }

    if unique.is_empty() {
        return results;
    }

    let tracker_urls: Vec<String> = trackers
        .iter()
        .filter_map(|tracker| {
            let parsed = Url::parse(tracker).ok()?;
            (parsed.scheme() == "udp").then(|| tracker.clone())
        })
        .take(MAX_TRACKERS)
        .collect();

    if tracker_urls.is_empty() {
        return results;
    }

    let queries = tracker_urls
        .into_iter()
        .map(|tracker| {
            let hashes = unique.clone();
            async move { scrape_tracker(&tracker, &hashes).await }
        });

    let tracker_results = futures_util::future::join_all(queries).await;
    let mut fresh: HashMap<String, TrackerAvailability> = unique
        .iter()
        .cloned()
        .map(|hash| (hash, TrackerAvailability::default()))
        .collect();

    for response in tracker_results
        .into_iter()
        .flatten()
    {
        for (hash, availability) in response {
            fresh
                .entry(hash)
                .or_default()
                .merge(&availability);
        }
    }

    for (hash, availability) in fresh {
        let ttl = if availability.responses > 0 {
            CACHE_TTL
        } else {
            FAILURE_CACHE_TTL
        };
        store.save(cache_key(&hash), availability.clone(), ttl);
        results.insert(hash, availability);
    }

    results
}

/// Verify a small sample of tracker-returned peers with the BitTorrent wire
/// protocol. This is deliberately much cheaper than adding a torrent: it sends
/// one handshake, reads the initial bitfield, downloads no pieces, and creates
/// no files. Tracker/provider seed counts are often stale; a peer that answers
/// now and advertises every piece is a substantially stronger startup signal.
pub(crate) async fn probe_active_peers(
    store: &Store,
    info_hash: &str,
    peers: &[SocketAddr],
) -> PeerAvailability {
    let normalized = info_hash.to_ascii_lowercase();
    let cache_key = format!("torrent-active-peers:{normalized}");
    if let Some(cached) = store.get::<PeerAvailability>(&cache_key) {
        return (*cached).clone();
    }
    let Some(info_hash) = decode_info_hash(&normalized) else {
        return PeerAvailability::default();
    };

    // Preflight and tracker peers arrive in source-specific clusters. Sampling
    // only the first addresses biases the result toward whichever discovery
    // mechanism happened to finish first, so spread the probes across the full
    // combined pool while keeping the same small connection budget.
    let public_peers: Vec<_> = peers
        .iter()
        .copied()
        .filter(is_public_peer)
        .collect();
    let probes = stream::iter(sample_peers(&public_peers, MAX_ACTIVE_PEER_PROBES))
        .map(|peer| async move { (peer, probe_peer(peer, info_hash).await) })
        .buffer_unordered(MAX_ACTIVE_PEER_PROBES)
        .collect::<Vec<_>>()
        .await;

    let mut availability = PeerAvailability::default();
    let mut responsive = Vec::new();
    for (peer, result) in probes {
        let Some(is_seeder) = result else {
            continue;
        };
        availability.responsive += 1;
        availability.seeders += usize::from(is_seeder);
        responsive.push((!is_seeder, peer));
    }
    responsive.sort_unstable();
    availability.peers = responsive
        .into_iter()
        .map(|(_, peer)| peer)
        .collect();
    store.save(cache_key, availability.clone(), PEER_PROBE_CACHE_TTL);
    availability
}

fn sample_peers(peers: &[SocketAddr], limit: usize) -> Vec<SocketAddr> {
    if limit == 0 || peers.is_empty() {
        return Vec::new();
    }
    if peers.len() <= limit {
        return peers.to_vec();
    }
    if limit == 1 {
        return vec![peers[0]];
    }

    (0..limit)
        .map(|index| {
            let peer_index = index.saturating_mul(peers.len() - 1) / (limit - 1);
            peers[peer_index]
        })
        .collect()
}

fn is_public_peer(peer: &SocketAddr) -> bool {
    if peer.port() == 0 {
        return false;
    }
    match peer.ip() {
        IpAddr::V4(ip) => {
            let [a, b, _, _] = ip.octets();
            !(a == 0
                || a == 10
                || a == 127
                || (a == 100 && (64..=127).contains(&b))
                || (a == 169 && b == 254)
                || (a == 172 && (16..=31).contains(&b))
                || (a == 192 && matches!(b, 0 | 168))
                || (a == 198 && matches!(b, 18 | 19 | 51))
                || (a == 203 && b == 0)
                || a >= 224)
        }
        IpAddr::V6(ip) => {
            if let Some(mapped) = ip.to_ipv4_mapped() {
                return is_public_peer(&SocketAddr::new(
                    IpAddr::V4(mapped),
                    peer.port(),
                ));
            }
            let segments = ip.segments();
            !ip.is_unspecified()
                && !ip.is_loopback()
                && !ip.is_multicast()
                && segments[0] & 0xfe00 != 0xfc00
                && segments[0] & 0xffc0 != 0xfe80
                && !(segments[0] == 0x2001 && segments[1] == 0x0db8)
        }
    }
}

async fn probe_peer(peer: SocketAddr, info_hash: [u8; 20]) -> Option<bool> {
    let mut socket = timeout(PEER_CONNECT_TIMEOUT, TcpStream::connect(peer))
        .await
        .ok()?
        .ok()?;
    let _ = socket.set_nodelay(true);

    let mut handshake = [0u8; 68];
    handshake[0] = 19;
    handshake[1..20].copy_from_slice(b"BitTorrent protocol");
    handshake[28..48].copy_from_slice(&info_hash);
    let random_suffix = rand::random::<[u8; 12]>();
    handshake[48..56].copy_from_slice(b"-RM0001-");
    handshake[56..].copy_from_slice(&random_suffix);
    timeout(PEER_CONNECT_TIMEOUT, socket.write_all(&handshake))
        .await
        .ok()?
        .ok()?;

    let mut response = [0u8; 68];
    timeout(PEER_MESSAGE_TIMEOUT, socket.read_exact(&mut response))
        .await
        .ok()?
        .ok()?;
    if response[0] != 19
        || &response[1..20] != b"BitTorrent protocol"
        || response[28..48] != info_hash
    {
        return None;
    }

    // Interested peers are more likely to promptly send their availability
    // state (and an unchoke) than a connection that only handshakes and waits.
    let interested = [0u8, 0, 0, 1, 2];
    let _ = timeout(PEER_CONNECT_TIMEOUT, socket.write_all(&interested)).await;

    let deadline = Instant::now() + PEER_MESSAGE_TIMEOUT;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Some(false);
        }
        let mut length_bytes = [0u8; 4];
        let Ok(read_result) =
            timeout(remaining, socket.read_exact(&mut length_bytes)).await
        else {
            return Some(false);
        };
        if read_result.is_err() {
            return Some(false);
        }
        let length = u32::from_be_bytes(length_bytes) as usize;
        if length == 0 {
            continue;
        }
        if length > MAX_PEER_MESSAGE_BYTES {
            return Some(false);
        }
        let mut message = vec![0u8; length];
        let Ok(read_result) = timeout(remaining, socket.read_exact(&mut message)).await
        else {
            return Some(false);
        };
        if read_result.is_err() {
            return Some(false);
        }
        match message[0] {
            // Standard bitfield message.
            5 => return Some(is_complete_bitfield(&message[1..])),
            // Fast extension: have all / have none.
            14 => return Some(true),
            15 => return Some(false),
            _ => {}
        }
    }
}

fn is_complete_bitfield(bitfield: &[u8]) -> bool {
    let Some((&last, prefix)) = bitfield.split_last() else {
        return false;
    };
    prefix
        .iter()
        .all(|byte| *byte == u8::MAX)
        && last != 0
        && (last | last.wrapping_sub(1)) == u8::MAX
}

fn cache_key(info_hash: &str) -> String {
    format!("torrent-scrape:{info_hash}")
}

async fn scrape_tracker(
    tracker: &str,
    hashes: &[String],
) -> Result<HashMap<String, TrackerAvailability>> {
    let url = Url::parse(tracker).context("invalid UDP tracker URL")?;
    let host = url
        .host_str()
        .context("UDP tracker missing host")?;
    let port = url
        .port()
        .context("UDP tracker missing port")?;
    let addr = tokio::net::lookup_host((host, port))
        .await?
        .find(SocketAddr::is_ipv4)
        .context("UDP tracker has no IPv4 address")?;
    let bind_addr = match addr.ip() {
        IpAddr::V4(_) => "0.0.0.0:0",
        IpAddr::V6(_) => "[::]:0",
    };
    let socket = UdpSocket::bind(bind_addr).await?;
    socket
        .connect(addr)
        .await?;

    let connection_id = tracker_connect(&socket).await?;
    announce_hashes(&socket, connection_id, hashes).await
}

async fn announce_hashes(
    socket: &UdpSocket,
    connection_id: u64,
    hashes: &[String],
) -> Result<HashMap<String, TrackerAvailability>> {
    let random_suffix = rand::random::<[u8; 12]>();
    let mut peer_id = [0u8; 20];
    peer_id[..8].copy_from_slice(b"-RM0001-");
    peer_id[8..].copy_from_slice(&random_suffix);

    let mut pending = HashMap::new();
    for hash in hashes
        .iter()
        .take(MAX_HASHES_PER_TRACKER)
    {
        let transaction_id = rand::random::<u32>();
        let mut request = Vec::with_capacity(98);
        request.extend_from_slice(&connection_id.to_be_bytes());
        request.extend_from_slice(&ACTION_ANNOUNCE.to_be_bytes());
        request.extend_from_slice(&transaction_id.to_be_bytes());
        request.extend_from_slice(
            &decode_info_hash(hash).context("invalid info hash in announce batch")?,
        );
        request.extend_from_slice(&peer_id);
        request.extend_from_slice(&0u64.to_be_bytes()); // downloaded
        request.extend_from_slice(&1u64.to_be_bytes()); // left (metadata check only)
        request.extend_from_slice(&0u64.to_be_bytes()); // uploaded
        request.extend_from_slice(&0u32.to_be_bytes()); // event: none
        request.extend_from_slice(&0u32.to_be_bytes()); // IP: tracker infers it
        request.extend_from_slice(&rand::random::<u32>().to_be_bytes());
        request.extend_from_slice(&ANNOUNCE_NUM_WANT.to_be_bytes());
        request.extend_from_slice(&6881u16.to_be_bytes());
        socket
            .send(&request)
            .await?;
        pending.insert(transaction_id, hash.clone());
    }

    let deadline = Instant::now() + ANNOUNCE_TIMEOUT;
    let mut output = HashMap::new();
    let mut response = vec![0u8; 64 * 1024];
    while !pending.is_empty() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        let Ok(received) = timeout(remaining, socket.recv(&mut response)).await else {
            break;
        };
        let len = received?;
        if len < 8 {
            continue;
        }
        let action = u32::from_be_bytes(
            response[0..4]
                .try_into()
                .unwrap(),
        );
        let transaction_id = u32::from_be_bytes(
            response[4..8]
                .try_into()
                .unwrap(),
        );
        let Some(hash) = pending.remove(&transaction_id) else {
            continue;
        };
        if action == ACTION_ERROR || action != ACTION_ANNOUNCE || len < 20 {
            continue;
        }

        let mut peers = Vec::new();
        for compact in response[20..len].chunks_exact(6) {
            let port = u16::from_be_bytes(
                compact[4..6]
                    .try_into()
                    .unwrap(),
            );
            let peer = SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(
                    compact[0], compact[1], compact[2], compact[3],
                )),
                port,
            );
            if is_public_peer(&peer) && !peers.contains(&peer) {
                peers.push(peer);
            }
            if peers.len() >= MAX_PEERS_PER_SWARM {
                break;
            }
        }
        output.insert(
            hash,
            TrackerAvailability {
                seeders: u32::from_be_bytes(
                    response[16..20]
                        .try_into()
                        .unwrap(),
                ),
                leechers: u32::from_be_bytes(
                    response[12..16]
                        .try_into()
                        .unwrap(),
                ),
                completed: 0,
                responses: 1,
                peers,
            },
        );
    }

    Ok(output)
}

async fn tracker_connect(socket: &UdpSocket) -> Result<u64> {
    let transaction_id = rand::random::<u32>();
    let mut request = Vec::with_capacity(16);
    request.extend_from_slice(&UDP_TRACKER_CONNECT_MAGIC.to_be_bytes());
    request.extend_from_slice(&ACTION_CONNECT.to_be_bytes());
    request.extend_from_slice(&transaction_id.to_be_bytes());
    socket
        .send(&request)
        .await?;

    let mut response = [0u8; 512];
    let len = timeout(TRACKER_TIMEOUT, socket.recv(&mut response))
        .await
        .context("UDP tracker connect timed out")??;
    if len < 8 {
        bail!("short UDP tracker connect response");
    }
    let action = u32::from_be_bytes(
        response[0..4]
            .try_into()
            .unwrap(),
    );
    let response_transaction = u32::from_be_bytes(
        response[4..8]
            .try_into()
            .unwrap(),
    );
    if response_transaction != transaction_id {
        bail!("UDP tracker transaction mismatch");
    }
    if action == ACTION_ERROR {
        bail!(
            "UDP tracker error: {}",
            String::from_utf8_lossy(&response[8..len])
        );
    }
    if action != ACTION_CONNECT || len < 16 {
        bail!("unexpected UDP tracker connect response");
    }
    Ok(u64::from_be_bytes(
        response[8..16]
            .try_into()
            .unwrap(),
    ))
}

fn decode_info_hash(hash: &str) -> Option<[u8; 20]> {
    if hash.len() != 40 {
        return None;
    }
    let mut output = [0u8; 20];
    for (idx, byte) in output
        .iter_mut()
        .enumerate()
    {
        *byte = u8::from_str_radix(&hash[idx * 2..idx * 2 + 2], 16).ok()?;
    }
    Some(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_invalid_info_hashes() {
        assert!(decode_info_hash("abc").is_none());
        assert!(decode_info_hash(&"z".repeat(40)).is_none());
        assert!(decode_info_hash(&"a".repeat(40)).is_some());
    }

    #[test]
    fn recognizes_complete_peer_bitfields() {
        assert!(is_complete_bitfield(&[0xff]));
        assert!(is_complete_bitfield(&[0xff, 0xf0]));
        assert!(is_complete_bitfield(&[0x80]));
        assert!(!is_complete_bitfield(&[]));
        assert!(!is_complete_bitfield(&[0xff, 0]));
        assert!(!is_complete_bitfield(&[0xfe, 0xff]));
        assert!(!is_complete_bitfield(&[0xff, 0xa0]));
    }

    #[test]
    fn active_probe_sample_spans_the_full_peer_pool() {
        let peers = (1..=100)
            .map(|last_octet| SocketAddr::from(([192, 0, 2, last_octet], 6881)))
            .collect::<Vec<_>>();

        let sampled = sample_peers(&peers, 12);
        assert_eq!(sampled.len(), 12);
        assert_eq!(sampled.first(), peers.first());
        assert_eq!(sampled.last(), peers.last());
    }

    #[test]
    fn active_probes_reject_local_and_reserved_addresses() {
        for peer in [
            SocketAddr::from(([127, 0, 0, 1], 6881)),
            SocketAddr::from(([10, 0, 0, 1], 6881)),
            SocketAddr::from(([192, 168, 1, 2], 6881)),
            SocketAddr::from(([169, 254, 1, 2], 6881)),
            SocketAddr::from(([192, 0, 2, 1], 6881)),
        ] {
            assert!(!is_public_peer(&peer), "{peer} must not be probed");
        }
        assert!(is_public_peer(&SocketAddr::from(([8, 8, 8, 8], 6881))));
    }
}
