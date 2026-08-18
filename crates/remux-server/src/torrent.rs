use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use bytes::Bytes;
use chrono::{NaiveDateTime, Utc};
use dashmap::DashMap;
use librqbit::{
    AddTorrent, AddTorrentOptions, AddTorrentResponse, PeerConnectionOptions, Session,
    SessionOptions, SessionPersistenceConfig,
    api::{Api, ApiTorrentListOpts, TorrentIdOrHash},
    dht::PersistentDhtConfig,
    http_api::HttpApi,
};
use moka::sync::Cache;
use tracing::{debug, info, warn};

#[derive(Clone, Debug, Default)]
pub(crate) struct ManagedTorrentAvailability {
    pub finished: bool,
    pub live_peers: usize,
    pub progress_bytes: u64,
    pub total_bytes: u64,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct TorrentPreflight {
    pub elapsed_ms: u64,
    pub seen_peers: usize,
    pub peers: Vec<SocketAddr>,
    pub from_cache: bool,
    pub selected_file_bytes: Option<u64>,
    pub total_bytes: u64,
    pub file_count: usize,
    pub managed: Option<ManagedTorrentAvailability>,
}

#[derive(Clone, Debug)]
struct CachedTorrentFile {
    name: String,
    length: u64,
}

#[derive(Clone)]
struct CachedTorrentMetadata {
    torrent_bytes: Bytes,
    seen_peers: Vec<SocketAddr>,
    trackers: Vec<String>,
    files: Vec<CachedTorrentFile>,
}

struct TorrentActivity {
    session: Arc<Session>,
    active_requests: DashMap<usize, usize>,
    playback_torrents: DashMap<String, usize>,
    last_watched: DashMap<usize, NaiveDateTime>,
}

impl TorrentActivity {
    fn is_active(&self, torrent_id: usize) -> bool {
        self.active_requests
            .get(&torrent_id)
            .is_some_and(|count| *count > 0)
            || self
                .playback_torrents
                .iter()
                .any(|entry| *entry.value() == torrent_id)
    }

    fn active_ids(&self) -> HashSet<usize> {
        let mut active: HashSet<usize> = self
            .active_requests
            .iter()
            .filter(|entry| *entry.value() > 0)
            .map(|entry| *entry.key())
            .collect();
        active.extend(
            self.playback_torrents
                .iter()
                .map(|entry| *entry.value()),
        );
        active
    }

    async fn pause_if_idle(self: Arc<Self>, torrent_id: usize) {
        if self.is_active(torrent_id) {
            return;
        }
        let api = Api::new(
            self.session
                .clone(),
            None,
            None,
        );
        if let Err(error) = api
            .api_torrent_action_pause(TorrentIdOrHash::Id(torrent_id))
            .await
        {
            debug!(torrent_id, %error, "torrent was already stopped or removed");
        } else {
            debug!(torrent_id, "paused torrent with no active playback");
        }
    }
}

/// Keeps a torrent active for the lifetime of one proxied HTTP request.
/// Playback ownership is tracked separately so a paused client can keep its
/// torrent alive even while it temporarily makes no byte-range request.
pub struct TorrentStreamGuard {
    activity: Arc<TorrentActivity>,
    torrent_id: usize,
}

impl Drop for TorrentStreamGuard {
    fn drop(&mut self) {
        let mut should_pause = false;
        if let Some(mut count) = self
            .activity
            .active_requests
            .get_mut(&self.torrent_id)
        {
            *count = count.saturating_sub(1);
            if *count == 0 {
                drop(count);
                self.activity
                    .active_requests
                    .remove(&self.torrent_id);
                should_pause = true;
            }
        }
        if should_pause {
            let activity = self
                .activity
                .clone();
            let torrent_id = self.torrent_id;
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn(activity.pause_if_idle(torrent_id));
            }
        }
    }
}

pub struct ResolvedTorrent {
    pub url: String,
    pub id: usize,
    pub info_hash: String,
    pub file_idx: usize,
    pub guard: TorrentStreamGuard,
}

pub struct TorrentManager {
    session: Arc<Session>,
    http_port: u16,
    data_dir: PathBuf,
    preflight_cache: Cache<String, CachedTorrentMetadata>,
    activity: Arc<TorrentActivity>,
    cleanup_lock: tokio::sync::Mutex<()>,
}

impl TorrentManager {
    pub async fn new(
        data_dir: PathBuf,
        cache_dir: PathBuf,
        http_port: Option<u16>,
        disable_dht: bool,
        peer_port: Option<u16>,
    ) -> Result<Self> {
        let session = Session::new_with_opts(
            data_dir.clone(),
            SessionOptions {
                disable_dht,
                disable_dht_persistence: disable_dht,
                listen_port_range: peer_port.map(|p| p..p + 10),
                persistence: Some(SessionPersistenceConfig::Json {
                    folder: Some(cache_dir.join("rqbit")),
                }),
                dht_config: Some(PersistentDhtConfig {
                    config_filename: Some(cache_dir.join("dht.json")),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await?;

        // None → let the OS pick a free ephemeral port.
        let bind_port = http_port.unwrap_or(0);
        let listener =
            tokio::net::TcpListener::bind(format!("127.0.0.1:{}", bind_port)).await?;

        let bound_port = listener
            .local_addr()?
            .port();

        let api = Api::new(session.clone(), None, None);
        let http_api = HttpApi::new(api, None);
        tokio::spawn(http_api.make_http_api_and_run(listener, None));

        let activity = Arc::new(TorrentActivity {
            session: session.clone(),
            active_requests: DashMap::new(),
            playback_torrents: DashMap::new(),
            last_watched: DashMap::new(),
        });

        // Persistence remembers the previous live/paused state. Playback
        // ownership does not survive a server restart, so every restored
        // torrent must begin paused until a real stream request claims it.
        let restored_api = Api::new(session.clone(), None, None);
        let restored_ids: Vec<usize> = restored_api
            .api_torrent_list()
            .torrents
            .into_iter()
            .filter_map(|torrent| torrent.id)
            .collect();
        for torrent_id in &restored_ids {
            let _ = restored_api
                .api_torrent_action_pause(TorrentIdOrHash::Id(*torrent_id))
                .await;
        }
        if !restored_ids.is_empty() {
            info!(
                torrents = restored_ids.len(),
                "paused restored torrents until playback claims them"
            );
        }

        // Session restoration can finish after Session::new_with_opts returns.
        // Reconcile a few times during startup so a persisted live torrent
        // cannot spring back to life after the first pause raced initialization.
        let reconcile_activity = activity.clone();
        tokio::spawn(async move {
            for delay_secs in [1, 3, 10] {
                tokio::time::sleep(Duration::from_secs(delay_secs)).await;
                let api = Api::new(
                    reconcile_activity
                        .session
                        .clone(),
                    None,
                    None,
                );
                for torrent_id in api
                    .api_torrent_list()
                    .torrents
                    .into_iter()
                    .filter_map(|torrent| torrent.id)
                {
                    if !reconcile_activity.is_active(torrent_id) {
                        let _ = api
                            .api_torrent_action_pause(TorrentIdOrHash::Id(torrent_id))
                            .await;
                    }
                }
            }
        });

        debug!(port = bound_port, "torrent HTTP server listening");
        Ok(Self {
            session,
            http_port: bound_port,
            data_dir,
            preflight_cache: Cache::builder()
                .max_capacity(64)
                .time_to_live(Duration::from_secs(120))
                .build(),
            activity,
            cleanup_lock: tokio::sync::Mutex::new(()),
        })
    }

    /// Gracefully shut down the librqbit session, releasing all sockets
    /// (including the DHT UDP socket). Call this before dropping the manager
    /// to avoid "address already in use" errors on restart.
    pub async fn shutdown(&self) {
        self.session
            .stop()
            .await;
    }

    pub fn active_torrent_ids(&self) -> HashSet<usize> {
        self.activity
            .active_ids()
    }

    async fn release_playback_torrent(&self, play_session_id: &str) -> Option<usize> {
        let Some((_, torrent_id)) = self
            .activity
            .playback_torrents
            .remove(play_session_id)
        else {
            return None;
        };
        self.activity
            .clone()
            .pause_if_idle(torrent_id)
            .await;
        Some(torrent_id)
    }

    /// Release playback ownership. The torrent is paused as soon as the last
    /// playback session and the last in-flight HTTP request are gone.
    pub async fn release_playback(&self, play_session_id: &str) {
        let _ = self
            .release_playback_torrent(play_session_id)
            .await;
    }

    /// Record recency only after a client reports real playback progress.
    /// Merely opening a torrent stream is download activity, not watching.
    pub fn mark_playback_watched(&self, play_session_id: &str) {
        let Some(torrent_id) = self
            .activity
            .playback_torrents
            .get(play_session_id)
            .map(|entry| *entry)
        else {
            return;
        };
        self.activity
            .last_watched
            .insert(torrent_id, Utc::now().naive_utc());
    }

    async fn source_was_watched(
        &self,
        db: &sqlx::SqlitePool,
        torrent_id: usize,
        source_id: uuid::Uuid,
    ) -> Result<bool> {
        if self
            .activity
            .last_watched
            .contains_key(&torrent_id)
        {
            return Ok(true);
        }
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(SELECT 1 FROM user_media_state \
             WHERE stream_id = ?1 AND (playback_position > 0 \
             OR play_count > 0 OR played_at IS NOT NULL \
             OR last_played_at IS NOT NULL))",
        )
        .bind(source_id)
        .fetch_one(db)
        .await?
            != 0)
    }

    /// Detach one playback from its torrent and stop that torrent immediately
    /// unless another playback session still owns it. Unlike the normal idle
    /// release path, this deliberately ignores the stale HTTP request guard:
    /// during an Auto failover that guard belongs to the FFmpeg process we are
    /// about to kill and must not keep downloading the rejected source.
    pub async fn pause_playback_for_switch(
        &self,
        play_session_id: &str,
    ) -> Option<usize> {
        let (_, torrent_id) = self
            .activity
            .playback_torrents
            .remove(play_session_id)?;
        let shared_by_other_playback = self
            .activity
            .playback_torrents
            .iter()
            .any(|entry| *entry.value() == torrent_id);
        if !shared_by_other_playback {
            let api = Api::new(
                self.session
                    .clone(),
                None,
                None,
            );
            if let Err(error) = api
                .api_torrent_action_pause(TorrentIdOrHash::Id(torrent_id))
                .await
            {
                debug!(torrent_id, %error, "failed to pause rejected playback torrent");
            } else {
                info!(torrent_id, %play_session_id, "paused rejected playback torrent for source switch");
            }
        }
        Some(torrent_id)
    }

    /// Remove a failed Auto torrent after its FFmpeg request has stopped.
    /// Previously watched candidates stay available for retention policy to
    /// manage, and a torrent shared by another playback is never deleted.
    pub async fn discard_failed_torrent_if_unwatched(
        &self,
        db: &sqlx::SqlitePool,
        torrent_id: usize,
        source_id: uuid::Uuid,
    ) -> Result<bool> {
        let info_hash = Api::new(
            self.session
                .clone(),
            None,
            None,
        )
        .api_torrent_list()
        .torrents
        .into_iter()
        .find(|torrent| torrent.id == Some(torrent_id))
        .map(|torrent| torrent.info_hash);
        let Some(info_hash) = info_hash else {
            return Ok(false);
        };

        if self
            .source_was_watched(db, torrent_id, source_id)
            .await?
        {
            return Ok(false);
        }

        // The ffmpeg process has already stopped, but allow its proxied HTTP
        // response guard a brief moment to drop before deleting the torrent.
        for _ in 0..20 {
            if !self
                .activity
                .is_active(torrent_id)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        if self
            .activity
            .is_active(torrent_id)
        {
            return Ok(false);
        }

        self.delete_torrent_by_hash(&info_hash)
            .await?;
        self.activity
            .last_watched
            .remove(&torrent_id);
        info!(
            torrent_id,
            %source_id,
            "deleted failed Auto torrent that was never watched"
        );
        Ok(true)
    }

    /// Release and remove an Auto candidate that failed before the user ever
    /// watched it.
    pub async fn discard_failed_playback_if_unwatched(
        &self,
        db: &sqlx::SqlitePool,
        play_session_id: &str,
        source_id: uuid::Uuid,
    ) -> Result<bool> {
        let Some(torrent_id) = self
            .pause_playback_for_switch(play_session_id)
            .await
        else {
            return Ok(false);
        };
        self.discard_failed_torrent_if_unwatched(db, torrent_id, source_id)
            .await
    }

    /// Delete a bounded startup hedge after its warm-standby window. The
    /// torrent is kept if any playback claimed it or if it has real watch
    /// history; merely probing a prefix is intentionally not recency.
    pub async fn discard_warm_standby_if_unwatched(
        &self,
        db: &sqlx::SqlitePool,
        source_id: uuid::Uuid,
        info_hash: &str,
    ) -> Result<bool> {
        let torrent_id = Api::new(
            self.session
                .clone(),
            None,
            None,
        )
        .api_torrent_list()
        .torrents
        .into_iter()
        .find(|torrent| {
            torrent
                .info_hash
                .eq_ignore_ascii_case(info_hash)
        })
        .and_then(|torrent| torrent.id);
        let Some(torrent_id) = torrent_id else {
            return Ok(false);
        };

        if self
            .source_was_watched(db, torrent_id, source_id)
            .await?
        {
            return Ok(false);
        }
        for _ in 0..20 {
            if !self
                .activity
                .is_active(torrent_id)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        if self
            .activity
            .is_active(torrent_id)
        {
            return Ok(false);
        }

        self.delete_torrent_by_hash(info_hash)
            .await?;
        self.activity
            .last_watched
            .remove(&torrent_id);
        info!(
            torrent_id,
            %source_id,
            %info_hash,
            "deleted expired Auto warm standby that was never watched"
        );
        Ok(true)
    }

    async fn acquire_stream(
        &self,
        torrent_id: usize,
        playback_ids: &[String],
    ) -> TorrentStreamGuard {
        self.activity
            .active_requests
            .entry(torrent_id)
            .and_modify(|count| *count += 1)
            .or_insert(1);
        // Construct the guard before the first await. Cancelling a startup
        // request must release the request count instead of leaving a torrent
        // permanently marked active.
        let guard = TorrentStreamGuard {
            activity: self
                .activity
                .clone(),
            torrent_id,
        };

        let mut replaced = Vec::new();
        for play_session_id in playback_ids {
            if let Some(old_id) = self
                .activity
                .playback_torrents
                .insert(play_session_id.clone(), torrent_id)
            {
                if old_id != torrent_id {
                    replaced.push(old_id);
                }
            }
        }
        let api = Api::new(
            self.session
                .clone(),
            None,
            None,
        );
        if let Err(error) = api
            .api_torrent_action_start(TorrentIdOrHash::Id(torrent_id))
            .await
        {
            debug!(torrent_id, %error, "torrent was already running");
        }

        // A new playback owns the bandwidth. Keep genuinely concurrent
        // playback sessions running, but pause every unowned/background torrent.
        if !playback_ids.is_empty() {
            let active = self
                .activity
                .active_ids();
            for other_id in api
                .api_torrent_list()
                .torrents
                .into_iter()
                .filter_map(|torrent| torrent.id)
                .filter(|id| *id != torrent_id && !active.contains(id))
            {
                let _ = api
                    .api_torrent_action_pause(TorrentIdOrHash::Id(other_id))
                    .await;
            }
        }
        for old_id in replaced {
            self.activity
                .clone()
                .pause_if_idle(old_id)
                .await;
        }

        guard
    }

    /// Return current local/live state without changing the torrent session.
    pub(crate) fn managed_availability(
        &self,
        info_hash: &str,
    ) -> Option<ManagedTorrentAvailability> {
        let api = Api::new(
            self.session
                .clone(),
            None,
            None,
        );
        let torrent = api
            .api_torrent_list_ext(ApiTorrentListOpts { with_stats: true })
            .torrents
            .into_iter()
            .find(|torrent| {
                torrent
                    .info_hash
                    .eq_ignore_ascii_case(info_hash)
            })?;
        let stats = torrent.stats?;
        Some(ManagedTorrentAvailability {
            // With stream-only downloads `only_files` is intentionally empty.
            // rqbit considers that empty natural queue "finished" even when
            // the movie file is only partially present. Only trust completion
            // when the selected byte total is non-zero and fully downloaded.
            finished: file_download_complete(stats.progress_bytes, stats.total_bytes),
            live_peers: stats
                .live
                .as_ref()
                .map(|live| {
                    live.snapshot
                        .peer_stats
                        .live
                })
                .unwrap_or(0),
            progress_bytes: stats.progress_bytes,
            total_bytes: stats.total_bytes,
        })
    }

    /// Put peers that answered a live wire-protocol probe at the front of the
    /// cached peer list used by the eventual download. Metadata preflight can
    /// discover many stale addresses; preserving the responsive subset avoids
    /// paying for that discovery twice when playback starts.
    pub(crate) fn prioritize_preflight_peers(
        &self,
        info_hash: &str,
        responsive_peers: &[SocketAddr],
    ) {
        if responsive_peers.is_empty() {
            return;
        }
        let info_hash = info_hash.to_ascii_lowercase();
        let Some(mut cached) = self
            .preflight_cache
            .get(&info_hash)
        else {
            return;
        };
        let mut seen = HashSet::new();
        cached.seen_peers = responsive_peers
            .iter()
            .chain(
                cached
                    .seen_peers
                    .iter(),
            )
            .copied()
            .filter(|peer| seen.insert(*peer))
            .collect();
        self.preflight_cache
            .insert(info_hash, cached);
    }

    /// Resolve torrent metadata and discover peers without starting a download
    /// or constructing storage. Successful metadata is cached so the real add
    /// on the playback path can skip the same network round trip.
    pub(crate) async fn preflight(
        &self,
        magnet: &str,
        initial_peers: &[SocketAddr],
        budget: Duration,
    ) -> Result<TorrentPreflight> {
        let info_hash = parse_info_hash_param(magnet)
            .context("torrent magnet has no v1 info hash")?;

        if let Some(mut managed) = self.managed_availability(&info_hash) {
            let torrent = Api::new(
                self.session
                    .clone(),
                None,
                None,
            )
            .api_torrent_list_ext(ApiTorrentListOpts { with_stats: true })
            .torrents
            .into_iter()
            .find(|torrent| {
                torrent
                    .info_hash
                    .eq_ignore_ascii_case(&info_hash)
            });
            let file_progress = torrent
                .as_ref()
                .and_then(|torrent| {
                    torrent
                        .stats
                        .as_ref()
                })
                .map(|stats| {
                    stats
                        .file_progress
                        .clone()
                })
                .unwrap_or_default();
            let files = torrent
                .and_then(|torrent| torrent.files)
                .unwrap_or_default()
                .into_iter()
                .map(|file| CachedTorrentFile {
                    name: file.name,
                    length: file.length,
                })
                .collect::<Vec<_>>();
            let selected_idx = select_file_index(
                &files,
                parse_file_idx_param(magnet),
                parse_file_param(magnet).as_deref(),
            )
            .ok();
            if let Some(idx) = selected_idx {
                if let Some(file) = files.get(idx) {
                    let progress = file_progress
                        .get(idx)
                        .copied()
                        .unwrap_or(0);
                    managed.progress_bytes = progress;
                    managed.total_bytes = file.length;
                    managed.finished = file_download_complete(progress, file.length);
                }
            } else {
                managed.finished = false;
            }
            return Ok(TorrentPreflight {
                selected_file_bytes: selected_idx
                    .and_then(|idx| files.get(idx))
                    .map(|file| file.length),
                total_bytes: files
                    .iter()
                    .map(|file| file.length)
                    .sum(),
                file_count: files.len(),
                managed: Some(managed),
                ..Default::default()
            });
        }

        if let Some(cached) = self
            .preflight_cache
            .get(&info_hash)
        {
            let selected_idx = select_file_index(
                &cached.files,
                parse_file_idx_param(magnet),
                parse_file_param(magnet).as_deref(),
            )
            .ok();
            return Ok(TorrentPreflight {
                seen_peers: cached
                    .seen_peers
                    .len(),
                peers: cached
                    .seen_peers
                    .iter()
                    .copied()
                    .take(32)
                    .collect(),
                from_cache: true,
                selected_file_bytes: selected_idx
                    .and_then(|idx| {
                        cached
                            .files
                            .get(idx)
                    })
                    .map(|file| file.length),
                total_bytes: cached
                    .files
                    .iter()
                    .map(|file| file.length)
                    .sum(),
                file_count: cached
                    .files
                    .len(),
                ..Default::default()
            });
        }

        let trackers = parse_tracker_params(magnet);
        let started = Instant::now();
        let response = tokio::time::timeout(
            budget,
            self.session
                .add_torrent(
                    AddTorrent::from_url(magnet.to_owned()),
                    Some(AddTorrentOptions {
                        list_only: true,
                        initial_peers: (!initial_peers.is_empty())
                            .then(|| initial_peers.to_vec()),
                        peer_opts: Some(PeerConnectionOptions {
                            connect_timeout: Some(Duration::from_millis(1200)),
                            read_write_timeout: Some(Duration::from_millis(2500)),
                            keep_alive_interval: None,
                        }),
                        ..Default::default()
                    }),
                ),
        )
        .await
        .context("torrent metadata preflight timed out")??;
        let elapsed_ms = started
            .elapsed()
            .as_millis() as u64;

        match response {
            AddTorrentResponse::ListOnly(response) => {
                let seen_peers = response
                    .seen_peers
                    .len();
                let peers = response
                    .seen_peers
                    .iter()
                    .copied()
                    .take(32)
                    .collect();
                let files = response
                    .info
                    .iter_file_details()
                    .context("invalid torrent file list")?
                    .map(|file| CachedTorrentFile {
                        name: file
                            .filename
                            .to_string()
                            .unwrap_or_else(|_| "<invalid filename>".to_string()),
                        length: file.len,
                    })
                    .collect::<Vec<_>>();
                let selected_idx = select_file_index(
                    &files,
                    parse_file_idx_param(magnet),
                    parse_file_param(magnet).as_deref(),
                )
                .ok();
                let selected_file_bytes = selected_idx
                    .and_then(|idx| files.get(idx))
                    .map(|file| file.length);
                let total_bytes = files
                    .iter()
                    .map(|file| file.length)
                    .sum();
                let file_count = files.len();
                self.preflight_cache
                    .insert(
                        info_hash,
                        CachedTorrentMetadata {
                            torrent_bytes: response.torrent_bytes,
                            seen_peers: response.seen_peers,
                            trackers,
                            files,
                        },
                    );
                Ok(TorrentPreflight {
                    elapsed_ms,
                    seen_peers,
                    peers,
                    selected_file_bytes,
                    total_bytes,
                    file_count,
                    ..Default::default()
                })
            }
            AddTorrentResponse::AlreadyManaged(_, _) => Ok(TorrentPreflight {
                elapsed_ms,
                managed: self.managed_availability(&info_hash),
                ..Default::default()
            }),
            AddTorrentResponse::Added(_, _) => {
                anyhow::bail!("torrent preflight unexpectedly started a download")
            }
        }
    }

    /// Resolve a magnet URI (possibly with `&tr=`, `&file_idx=`, `&file=` params
    /// we encode) to a local `http://127.0.0.1:<port>/torrents/<id>/stream/<file_idx>` URL
    pub async fn resolve_url(
        &self,
        magnet: &str,
        playback_ids: &[String],
    ) -> Result<ResolvedTorrent> {
        let info_hash = parse_info_hash_param(magnet)
            .context("torrent magnet has no v1 info hash")?;
        let file_idx_override = parse_file_idx_param(magnet);
        let wanted_file = parse_file_param(magnet);
        let cached_metadata = self
            .preflight_cache
            .get(&info_hash);
        let cached_file_idx = cached_metadata
            .as_ref()
            .map(|cached| {
                select_file_index(
                    &cached.files,
                    file_idx_override,
                    wanted_file.as_deref(),
                )
            })
            .transpose()?;
        debug!(
            magnet,
            ?wanted_file,
            ?file_idx_override,
            ?cached_file_idx,
            cached_metadata = cached_metadata.is_some(),
            "resolving torrent"
        );

        // Add with an empty natural piece queue. Metadata resolution (or a
        // client cancelling it) must never begin downloading a movie or bundle
        // before request ownership has been registered.
        let response = self
            .session
            .add_torrent(
                torrent_input(magnet, cached_metadata.as_ref()),
                Some(add_options(None, None, cached_metadata.as_ref())),
            )
            .await
            .context("failed to add torrent")?;

        let (torrent_id, handle) = match response {
            AddTorrentResponse::Added(id, handle)
            | AddTorrentResponse::AlreadyManaged(id, handle) => (id, handle),
            AddTorrentResponse::ListOnly(_) => {
                anyhow::bail!("unexpected ListOnly response")
            }
        };

        // Own the torrent before waiting for metadata. If the client goes away
        // during magnet resolution, the guard pauses the empty torrent.
        let guard = self
            .acquire_stream(torrent_id, playback_ids)
            .await;

        tokio::time::timeout(Duration::from_secs(30), handle.wait_until_initialized())
            .await
            .context("timed out waiting for torrent metadata")?
            .context("torrent initialization failed")?;

        let files = handle.with_metadata(|metadata| {
            metadata
                .file_infos
                .iter()
                .map(|file| CachedTorrentFile {
                    name: file
                        .relative_filename
                        .to_string_lossy()
                        .into_owned(),
                    length: file.len,
                })
                .collect::<Vec<_>>()
        })?;
        let file_idx =
            select_file_index(&files, file_idx_override, wanted_file.as_deref())?;
        let selected_file = files
            .get(file_idx)
            .context("selected torrent file disappeared")?;

        let api = Api::new(
            self.session
                .clone(),
            None,
            None,
        );
        // The torrent was added with `only_files = []`. Do not temporarily
        // select the movie here: rqbit eagerly queues the entire selected file,
        // and clearing the selection afterward does not reliably cancel those
        // already-scheduled pieces. The HTTP FileStream below must be the only
        // piece owner so its small sequential look-ahead prioritizes startup.
        // Do not require live peers before returning the stream URL. With an
        // empty natural queue rqbit can defer peer connections until FileStream
        // registers its needed pieces; waiting here creates a circular gate.
        // Metadata preflight already proved that the swarm answered recently.
        let (finished, live_peers) = api
            .api_stats_v1(TorrentIdOrHash::Id(torrent_id))
            .map(|stats| {
                (
                    file_download_complete(
                        stats
                            .file_progress
                            .get(file_idx)
                            .copied()
                            .unwrap_or(0),
                        selected_file.length,
                    ),
                    stats
                        .live
                        .as_ref()
                        .map(|live| {
                            live.snapshot
                                .peer_stats
                                .live
                        })
                        .unwrap_or(0),
                )
            })
            .unwrap_or((false, 0));

        info!(
            torrent_id,
            %info_hash,
            file_idx,
            file = %selected_file.name,
            file_bytes = selected_file.length,
            file_count = files.len(),
            live_peers,
            finished,
            playback_sessions = playback_ids.len(),
            "torrent ready in stream-only mode"
        );

        Ok(ResolvedTorrent {
            url: format!(
                "http://127.0.0.1:{}/torrents/{}/stream/{}",
                self.http_port, torrent_id, file_idx
            ),
            id: torrent_id,
            info_hash,
            file_idx,
            guard,
        })
    }

    /// Delete a single managed torrent by id after a candidate is discarded.
    pub async fn delete_torrent(&self, id: usize) -> anyhow::Result<()> {
        let api = Api::new(
            self.session
                .clone(),
            None,
            None,
        );
        api.api_torrent_action_delete(TorrentIdOrHash::Id(id))
            .await?;
        Ok(())
    }

    async fn delete_torrent_by_hash(&self, info_hash: &str) -> anyhow::Result<()> {
        let api = Api::new(
            self.session
                .clone(),
            None,
            None,
        );
        api.api_torrent_action_delete(TorrentIdOrHash::parse(info_hash)?)
            .await?;
        Ok(())
    }

    /// Parse the torrent ID out of a librqbit stream URL.
    /// Format: `http://127.0.0.1:{port}/torrents/{id}/stream/{file_idx}`
    pub fn torrent_id_from_url(url: &str) -> Option<usize> {
        let after_host = url
            .split_once("//")?
            .1
            .split_once('/')?
            .1;
        let mut parts = after_host.splitn(3, '/');
        if parts.next()? != "torrents" {
            return None;
        }
        parts
            .next()?
            .parse()
            .ok()
    }

    /// Apply upload/download speed limits.  0 = no limit (for download) or
    /// effectively-disabled (for upload — 1 bps is used since the API requires
    /// `NonZeroU32`).
    pub fn update_limits(&self, upload_kbps: i64, download_kbps: i64) {
        use std::num::NonZeroU32;
        // upload: 0 means "don't seed" — clamp to 1 bps (librqbit requires NonZero)
        let upload = NonZeroU32::new(if upload_kbps <= 0 {
            1
        } else {
            (upload_kbps as u32).saturating_mul(1024)
        });
        // download: 0 means unlimited → None
        let download = if download_kbps <= 0 {
            None
        } else {
            NonZeroU32::new((download_kbps as u32).saturating_mul(1024))
        };
        self.session
            .ratelimits
            .set_upload_bps(upload);
        self.session
            .ratelimits
            .set_download_bps(download);
    }
}

struct StorageEntry {
    id: usize,
    info_hash: String,
    path: PathBuf,
    allocated_bytes: u64,
    last_watched: Option<NaiveDateTime>,
}

fn retention_delete_ids(
    entries: &[StorageEntry],
    active: &HashSet<usize>,
    now: NaiveDateTime,
    keep_days: Option<u32>,
    keep_count: Option<usize>,
) -> HashSet<usize> {
    let mut inactive: Vec<&StorageEntry> = entries
        .iter()
        .filter(|entry| !active.contains(&entry.id))
        .collect();
    inactive.sort_by_key(|entry| entry.last_watched);
    let mut delete = HashSet::new();

    if let Some(days) = keep_days {
        let cutoff = now - chrono::Duration::days(days as i64);
        for entry in &inactive {
            if entry
                .last_watched
                .is_none_or(|watched| watched < cutoff)
            {
                delete.insert(entry.id);
            }
        }
    }

    if let Some(keep_n) = keep_count {
        let keep_ids: HashSet<usize> = inactive
            .iter()
            .rev()
            .filter(|entry| {
                entry
                    .last_watched
                    .is_some()
            })
            .take(keep_n)
            .map(|entry| entry.id)
            .collect();
        for entry in inactive {
            if !keep_ids.contains(&entry.id) {
                delete.insert(entry.id);
            }
        }
    }

    delete
}

fn allocated_size(path: &std::path::Path) -> u64 {
    let Ok(metadata) = std::fs::symlink_metadata(path) else {
        return 0;
    };
    if metadata
        .file_type()
        .is_symlink()
    {
        return 0;
    }
    if metadata.is_file() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            return metadata
                .blocks()
                .saturating_mul(512);
        }
        #[cfg(not(unix))]
        {
            return metadata.len();
        }
    }
    if !metadata.is_dir() {
        return 0;
    }
    std::fs::read_dir(path)
        .map(|entries| {
            entries
                .filter_map(|entry| entry.ok())
                .map(|entry| allocated_size(&entry.path()))
                .sum()
        })
        .unwrap_or(0)
}

#[cfg(unix)]
fn free_bytes(path: &std::path::Path) -> Option<u64> {
    let path = std::ffi::CString::new(
        path.to_string_lossy()
            .as_bytes(),
    )
    .ok()?;
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    (unsafe { libc::statvfs(path.as_ptr(), &mut stat) } == 0)
        .then(|| (stat.f_bavail as u64).saturating_mul(stat.f_bsize as u64))
}

#[cfg(not(unix))]
fn free_bytes(_path: &std::path::Path) -> Option<u64> {
    None
}

fn torrent_input<'a>(
    magnet: &'a str,
    cached: Option<&CachedTorrentMetadata>,
) -> AddTorrent<'a> {
    cached
        .map(|cached| {
            AddTorrent::from_bytes(
                cached
                    .torrent_bytes
                    .clone(),
            )
        })
        .unwrap_or_else(|| AddTorrent::from_url(magnet))
}

fn add_options(
    file_idx: Option<usize>,
    wanted_file: Option<&str>,
    cached: Option<&CachedTorrentMetadata>,
) -> AddTorrentOptions {
    let (only_files, only_files_regex) = match (file_idx, wanted_file) {
        (Some(idx), _) => (Some(vec![idx]), None),
        (None, Some(name)) => (None, Some(format!("(?i){}$", regex::escape(name)))),
        // No selection must mean no natural piece queue. librqbit interprets
        // `None` as selecting every file in a bundle.
        _ => (Some(Vec::new()), None),
    };

    AddTorrentOptions {
        only_files,
        only_files_regex,
        initial_peers: cached
            .filter(|cached| {
                !cached
                    .seen_peers
                    .is_empty()
            })
            .map(|cached| {
                cached
                    .seen_peers
                    .clone()
            }),
        trackers: cached
            .filter(|cached| {
                !cached
                    .trackers
                    .is_empty()
            })
            .map(|cached| {
                cached
                    .trackers
                    .clone()
            }),
        peer_opts: Some(PeerConnectionOptions {
            connect_timeout: Some(Duration::from_millis(1200)),
            read_write_timeout: None,
            keep_alive_interval: None,
        }),
        ..Default::default()
    }
}

fn file_download_complete(progress_bytes: u64, file_bytes: u64) -> bool {
    file_bytes > 0 && progress_bytes >= file_bytes
}

fn is_video_file(name: &str) -> bool {
    let extension = std::path::Path::new(name)
        .extension()
        .and_then(|extension| extension.to_str())
        .unwrap_or_default();
    remux_sdks::remux::VideoContainer::parse_known(extension).is_some()
}

fn select_file_index(
    files: &[CachedTorrentFile],
    requested_idx: Option<usize>,
    wanted_file: Option<&str>,
) -> Result<usize> {
    if files.is_empty() {
        anyhow::bail!("torrent contains no files");
    }
    if let Some(wanted) = wanted_file {
        if let Some((idx, _)) = files
            .iter()
            .enumerate()
            .find(|(_, file)| {
                is_video_file(&file.name)
                    && (file
                        .name
                        .eq_ignore_ascii_case(wanted)
                        || std::path::Path::new(&file.name)
                            .file_name()
                            .and_then(|name| name.to_str())
                            .is_some_and(|name| name.eq_ignore_ascii_case(wanted)))
            })
        {
            return Ok(idx);
        }
    }
    if let Some(idx) = requested_idx.filter(|idx| {
        files
            .get(*idx)
            .is_some_and(|file| is_video_file(&file.name))
    }) {
        return Ok(idx);
    }

    let mut videos: Vec<(usize, &CachedTorrentFile)> = files
        .iter()
        .enumerate()
        .filter(|(_, file)| is_video_file(&file.name))
        .collect();
    if videos.len() == 1 {
        return Ok(videos[0].0);
    }
    videos.sort_by_key(|(_, file)| std::cmp::Reverse(file.length));
    if let [largest, second, ..] = videos.as_slice() {
        // A release with one main feature plus samples/extras is unambiguous.
        // A true movie bundle has similarly-sized video files and therefore
        // requires the provider's real file index or filename hint.
        if largest
            .1
            .length
            >= second
                .1
                .length
                .saturating_mul(2)
        {
            return Ok(largest.0);
        }
    }
    match requested_idx {
        Some(idx) => anyhow::bail!(
            "torrent file index {idx} does not identify a video in {} files and no unique video could be selected",
            files.len()
        ),
        None => anyhow::bail!(
            "multi-file torrent has {} video files; a valid file index or filename is required",
            videos.len()
        ),
    }
}

fn parse_info_hash_param(magnet: &str) -> Option<String> {
    if magnet.len() == 40
        && magnet
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Some(magnet.to_ascii_lowercase());
    }
    let query = magnet
        .split_once('?')?
        .1;
    url::form_urlencoded::parse(query.as_bytes()).find_map(|(key, value)| {
        if key != "xt" {
            return None;
        }
        let hash = value
            .rsplit(':')
            .next()?;
        (hash.len() == 40
            && hash
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit()))
        .then(|| hash.to_ascii_lowercase())
    })
}

fn parse_tracker_params(magnet: &str) -> Vec<String> {
    let Some((_, query)) = magnet.split_once('?') else {
        return Vec::new();
    };
    let mut trackers = Vec::new();
    for (_, value) in
        url::form_urlencoded::parse(query.as_bytes()).filter(|(key, _)| key == "tr")
    {
        let tracker = value.into_owned();
        if !trackers.contains(&tracker) {
            trackers.push(tracker);
        }
    }
    trackers
}

/// Extract the `file=` query parameter we encode into our magnet URIs.
fn parse_file_param(magnet: &str) -> Option<String> {
    let query = magnet
        .split_once('?')?
        .1;
    url::form_urlencoded::parse(query.as_bytes())
        .find(|(k, _)| k == "file")
        .map(|(_, v)| v.into_owned())
}

/// Extract the `file_idx=` query parameter we encode into our magnet URIs.
fn parse_file_idx_param(magnet: &str) -> Option<usize> {
    let query = magnet
        .split_once('?')?
        .1;
    url::form_urlencoded::parse(query.as_bytes())
        .find(|(k, _)| k == "file_idx")
        .and_then(|(_, v)| {
            v.parse()
                .ok()
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(name: &str, length: u64) -> CachedTorrentFile {
        CachedTorrentFile {
            name: name.to_string(),
            length,
        }
    }

    #[test]
    fn parses_magnet_preflight_parameters() {
        let magnet = "magnet:?xt=urn:btih:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA&tr=udp%3A%2F%2Ftracker.example%3A80%2Fannounce&tr=udp%3A%2F%2Ftracker.example%3A80%2Fannounce";
        assert_eq!(
            parse_info_hash_param(magnet).as_deref(),
            Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        );
        assert_eq!(
            parse_tracker_params(magnet),
            vec!["udp://tracker.example:80/announce"]
        );
    }

    #[test]
    fn bundle_uses_exact_requested_file_and_rejects_invalid_ambiguous_index() {
        let files = vec![
            file("Bundle/Movie.One.1080p.mkv", 2_000),
            file("Bundle/Movie.Two.1080p.mkv", 2_100),
            file("Bundle/Movie.Three.1080p.mkv", 1_900),
        ];
        assert_eq!(select_file_index(&files, Some(1), None).unwrap(), 1);
        assert!(select_file_index(&files, Some(99), None).is_err());
    }

    #[test]
    fn bundle_filename_overrides_non_video_provider_index() {
        let files = vec![
            file("Bundle/Movie.One.1080p.mkv", 2_000),
            file("Bundle/release.nfo", 1),
            file("Bundle/Movie.Two.1080p.mkv", 2_100),
        ];
        assert_eq!(
            select_file_index(&files, Some(1), Some("Movie.Two.1080p.mkv")).unwrap(),
            2
        );
        assert!(select_file_index(&files, Some(1), None).is_err());
    }

    #[test]
    fn multi_file_release_can_choose_unique_main_feature() {
        let files = vec![
            file("Release/sample.mkv", 100),
            file("Release/Movie.1080p.mkv", 2_000),
            file("Release/subtitles.srt", 2),
        ];
        assert_eq!(select_file_index(&files, None, None).unwrap(), 1);
    }

    #[test]
    fn no_file_selection_means_stream_only_not_all_files() {
        let options = add_options(None, None, None);
        assert_eq!(options.only_files, Some(Vec::new()));
    }

    #[test]
    fn empty_stream_only_queue_is_not_a_completed_movie() {
        assert!(!file_download_complete(0, 0));
        assert!(!file_download_complete(50, 100));
        assert!(file_download_complete(100, 100));
        assert!(file_download_complete(120, 100));
    }

    #[test]
    fn retention_uses_watch_history_and_never_deletes_active_torrents() {
        let now = chrono::NaiveDate::from_ymd_opt(2026, 8, 17)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap();
        let entry = |id, age_hours: Option<i64>| StorageEntry {
            id,
            info_hash: id.to_string(),
            path: PathBuf::new(),
            allocated_bytes: 0,
            last_watched: age_hours.map(|hours| now - chrono::Duration::hours(hours)),
        };
        let entries = vec![
            entry(1, None),
            entry(2, Some(1)),
            entry(3, Some(24)),
            entry(4, Some(72)),
            entry(5, None),
        ];
        let active = HashSet::from([1]);

        assert_eq!(
            retention_delete_ids(&entries, &active, now, Some(2), Some(2)),
            HashSet::from([4, 5])
        );
        assert_eq!(
            retention_delete_ids(&entries, &active, now, None, Some(1)),
            HashSet::from([3, 4, 5])
        );
    }
}
