use chrono::{DateTime, Utc};
use dashmap::DashMap;
use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::{common, db, db::auth, playback::session::TranscodeSession};
use remux_sdks::remux::{PlayMethod, PlaybackInfo, QueueItem};

#[derive(Clone)]
pub struct PlaybackSession {
    pub play_session_id: String,
    pub user_id: Uuid,
    pub item_id: Uuid,
    pub media_source_id: Option<String>,
    pub device_id: String,
    pub client_name: String,
    pub position_ticks: i64,
    pub can_seek: bool,
    pub is_paused: bool,
    pub last_paused_at: Option<DateTime<Utc>>,
    pub is_muted: bool,
    pub volume_level: Option<i32>,
    pub audio_stream_index: Option<i32>,
    pub subtitle_stream_index: Option<i32>,
    pub play_method: Option<String>,
    pub now_playing_queue: Option<Vec<QueueItem>>,
    pub playlist_item_id: Option<String>,
    pub started_at: DateTime<Utc>,
    pub last_activity: DateTime<Utc>,
    /// Active transcode session owned by this playback session, if any.
    pub transcode: Option<Arc<tokio::sync::RwLock<TranscodeSession>>>,
    /// Stream group UUID that the selected source belongs to, if any.
    pub group_id: Option<Uuid>,
    /// Kind of the item being played, used to populate NowPlayingItem in session broadcasts.
    pub item_kind: Option<db::MediaKind>,
}

#[derive(Clone)]
struct PlaybackStartup {
    started_at: DateTime<Utc>,
    started: Instant,
    item_id: Uuid,
    user_id: Uuid,
    device_id: String,
    client_name: String,
    media_source_id: Option<Uuid>,
    playback_info_ready: Option<Duration>,
    hls_requested: Option<Duration>,
    source_selected: Option<Duration>,
    transcode_started: Option<Duration>,
    first_segment_served: Option<Duration>,
}

#[derive(Clone)]
struct PlaybackStartupFailure {
    item_id: Uuid,
    user_id: Uuid,
    failed_at: Instant,
}

#[derive(Clone, Debug)]
pub struct PlaybackStartupReport {
    pub play_session_id: String,
    pub item_id: Uuid,
    pub media_source_id: Option<Uuid>,
    pub user_id: Uuid,
    pub device_id: String,
    pub client_name: String,
    pub outcome: String,
    pub started_at: DateTime<Utc>,
    pub playback_info_ms: Option<i64>,
    pub hls_requested_ms: Option<i64>,
    pub source_selected_ms: Option<i64>,
    pub transcode_started_ms: Option<i64>,
    pub first_segment_served_ms: Option<i64>,
    pub first_progress_ms: Option<i64>,
    pub estimated_actual_playback_ms: Option<i64>,
    pub confirmation_delay_ms: Option<i64>,
    pub failure_reason: Option<String>,
}

#[derive(Clone, Debug)]
pub struct PlaybackStartupProgress {
    pub play_session_id: String,
    pub user_id: Uuid,
    pub elapsed: Duration,
    pub playback_info_ready: bool,
    pub hls_requested: bool,
    pub source_selected: bool,
    pub transcode_started: bool,
    pub first_segment_served: bool,
}

#[derive(Clone)]
pub struct PlaybackSessionManager {
    sessions: Arc<DashMap<String, PlaybackSession>>,
    startups: Arc<DashMap<String, PlaybackStartup>>,
    startup_failures: Arc<DashMap<String, PlaybackStartupFailure>>,
    base_dir: PathBuf,
}

#[cfg(unix)]
fn cleanup_stale_transcodes(base_dir: &std::path::Path) {
    use std::collections::HashSet;

    let mut pids = HashSet::new();
    // Restrict the process scan to ffmpeg commands that explicitly write
    // beneath this exact transcode directory. We intentionally do not trust
    // stale .pid files: their PID may have been reused by an unrelated process.
    if let Ok(output) = std::process::Command::new("ps")
        .args(["-axo", "pid=,command="])
        .output()
    {
        let base = base_dir.to_string_lossy();
        for line in String::from_utf8_lossy(&output.stdout).lines() {
            let line = line.trim_start();
            let Some((pid, command)) = line.split_once(char::is_whitespace) else {
                continue;
            };
            if command.contains("ffmpeg") && command.contains(base.as_ref()) {
                if let Ok(pid) = pid.parse::<libc::pid_t>() {
                    if pid > 0 {
                        pids.insert(pid);
                    }
                }
            }
        }
    }

    for pid in &pids {
        unsafe {
            libc::kill(*pid, libc::SIGCONT);
            libc::kill(*pid, libc::SIGKILL);
        }
    }
    if !pids.is_empty() {
        info!(
            count = pids.len(),
            "killed stale transcode processes at startup"
        );
    }
    let _ = std::fs::remove_dir_all(base_dir);
}

#[cfg(not(unix))]
fn cleanup_stale_transcodes(base_dir: &std::path::Path) {
    let _ = std::fs::remove_dir_all(base_dir);
}

impl PlaybackSessionManager {
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        let base_dir = base_dir.into();
        cleanup_stale_transcodes(&base_dir);
        let _ = std::fs::create_dir_all(&base_dir);
        Self {
            sessions: Arc::new(DashMap::new()),
            startups: Arc::new(DashMap::new()),
            startup_failures: Arc::new(DashMap::new()),
            base_dir,
        }
    }

    /// Start a server-side latency trace at the first PlaybackInfo request. The
    /// client does not expose its click timestamp through Jellyfin's API, so
    /// this is the earliest consistent boundary available for every client.
    pub fn begin_startup(
        &self,
        play_session_id: &str,
        item_id: Uuid,
        user_id: Uuid,
        device_id: &str,
        client_name: &str,
    ) {
        // Jellyfin may automatically request PlaybackInfo again for the same
        // item after its no-stream placeholder starts. Keep the terminal
        // failure visible across that retry; a different item or an explicit
        // client acknowledgement clears it.
        let preserve_failure = self
            .recent_startup_failure(device_id, user_id)
            .is_some_and(|failure| failure.item_id == item_id);
        if !preserve_failure {
            self.startup_failures
                .remove(device_id);
        }
        if self
            .startups
            .len()
            > 128
        {
            self.startups
                .retain(|_, startup| {
                    startup
                        .started
                        .elapsed()
                        < Duration::from_secs(30 * 60)
                });
        }
        self.startups
            .insert(
                play_session_id.to_string(),
                PlaybackStartup {
                    started_at: Utc::now(),
                    started: Instant::now(),
                    item_id,
                    user_id,
                    device_id: device_id.to_string(),
                    client_name: client_name.to_string(),
                    media_source_id: None,
                    playback_info_ready: None,
                    hls_requested: None,
                    source_selected: None,
                    transcode_started: None,
                    first_segment_served: None,
                },
            );
    }

    /// Whether PlaybackInfo created an in-progress startup for this ID. This
    /// exists before clients can send PlaybackStart, while HLS may already be
    /// resolving and buffering its selected source.
    pub fn has_startup(&self, play_session_id: &str) -> bool {
        self.startups
            .contains_key(play_session_id)
    }

    /// Return the newest unfinished startup for a device. A client can begin a
    /// replacement attempt before the prior one has fully unwound.
    pub fn startup_progress_for_device(
        &self,
        device_id: &str,
    ) -> Option<PlaybackStartupProgress> {
        let startup = self
            .startups
            .iter()
            .filter(|entry| entry.device_id == device_id)
            .max_by_key(|entry| entry.started)?;
        Some(PlaybackStartupProgress {
            play_session_id: startup
                .key()
                .clone(),
            user_id: startup.user_id,
            elapsed: startup
                .started
                .elapsed(),
            playback_info_ready: startup
                .playback_info_ready
                .is_some(),
            hls_requested: startup
                .hls_requested
                .is_some(),
            source_selected: startup
                .source_selected
                .is_some(),
            transcode_started: startup
                .transcode_started
                .is_some(),
            first_segment_served: startup
                .first_segment_served
                .is_some(),
        })
    }

    fn recent_startup_failure(
        &self,
        device_id: &str,
        user_id: Uuid,
    ) -> Option<PlaybackStartupFailure> {
        let failure = self
            .startup_failures
            .get(device_id)
            .map(|failure| {
                failure
                    .value()
                    .clone()
            })?;
        let expired = failure
            .failed_at
            .elapsed()
            > Duration::from_secs(5 * 60);
        if expired {
            self.startup_failures
                .remove(device_id);
            return None;
        }
        (failure.user_id == user_id).then_some(failure)
    }

    pub fn has_recent_startup_failure(&self, device_id: &str, user_id: Uuid) -> bool {
        self.recent_startup_failure(device_id, user_id)
            .is_some()
    }

    pub fn clear_startup_failure(&self, device_id: &str) {
        self.startup_failures
            .remove(device_id);
    }

    fn mark_startup<F>(&self, play_session_id: &str, update: F)
    where
        F: FnOnce(&mut PlaybackStartup, Duration),
    {
        if let Some(mut startup) = self
            .startups
            .get_mut(play_session_id)
        {
            let elapsed = startup
                .started
                .elapsed();
            update(startup.value_mut(), elapsed);
        }
    }

    pub fn mark_playback_info_ready(&self, play_session_id: &str) {
        self.mark_startup(play_session_id, |startup, elapsed| {
            startup
                .playback_info_ready
                .get_or_insert(elapsed);
        });
    }

    pub fn mark_hls_requested(&self, play_session_id: &str) {
        self.mark_startup(play_session_id, |startup, elapsed| {
            startup
                .hls_requested
                .get_or_insert(elapsed);
        });
    }

    pub fn mark_source_selected(&self, play_session_id: &str, media_source_id: Uuid) {
        self.mark_startup(play_session_id, |startup, elapsed| {
            startup.source_selected = Some(elapsed);
            startup.media_source_id = Some(media_source_id);
        });
    }

    pub fn mark_transcode_started(&self, play_session_id: &str) {
        self.mark_startup(play_session_id, |startup, elapsed| {
            startup
                .transcode_started
                .get_or_insert(elapsed);
        });
    }

    pub fn mark_first_segment_served(&self, play_session_id: &str) {
        self.mark_startup(play_session_id, |startup, elapsed| {
            startup
                .first_segment_served
                .get_or_insert(elapsed);
        });
    }

    fn duration_ms(duration: Option<Duration>) -> Option<i64> {
        duration.map(|duration| {
            duration
                .as_millis()
                .min(i64::MAX as u128) as i64
        })
    }

    fn startup_report(
        play_session_id: String,
        startup: PlaybackStartup,
        outcome: &str,
        first_progress: Option<Duration>,
        estimated_actual_playback: Option<Duration>,
        failure_reason: Option<String>,
    ) -> PlaybackStartupReport {
        let confirmation_delay = first_progress
            .zip(estimated_actual_playback)
            .map(|(confirmed, estimated)| confirmed.saturating_sub(estimated));
        PlaybackStartupReport {
            play_session_id,
            item_id: startup.item_id,
            media_source_id: startup.media_source_id,
            user_id: startup.user_id,
            device_id: startup.device_id,
            client_name: startup.client_name,
            outcome: outcome.to_string(),
            started_at: startup.started_at,
            playback_info_ms: Self::duration_ms(startup.playback_info_ready),
            hls_requested_ms: Self::duration_ms(startup.hls_requested),
            source_selected_ms: Self::duration_ms(startup.source_selected),
            transcode_started_ms: Self::duration_ms(startup.transcode_started),
            first_segment_served_ms: Self::duration_ms(startup.first_segment_served),
            first_progress_ms: Self::duration_ms(first_progress),
            estimated_actual_playback_ms: Self::duration_ms(estimated_actual_playback),
            confirmation_delay_ms: Self::duration_ms(confirmation_delay),
            failure_reason,
        }
    }

    /// Confirm real playback from the first advancing, unpaused progress report.
    /// Jellyfin clients report current media time rather than the instant the
    /// first frame was rendered, so backdate the report by the amount advanced.
    /// The first served media segment is a hard lower bound for that estimate.
    pub fn confirm_startup(
        &self,
        play_session_id: &str,
        initial_position_ticks: i64,
        position_ticks: Option<i64>,
        is_paused: bool,
    ) -> Option<PlaybackStartupReport> {
        if is_paused {
            return None;
        }
        let advanced_ticks = position_ticks?
            .saturating_sub(initial_position_ticks)
            .max(0);
        if advanced_ticks == 0 {
            return None;
        }
        let (_, startup) = self
            .startups
            .remove(play_session_id)?;
        self.startup_failures
            .remove(&startup.device_id);
        let confirmed = startup
            .started
            .elapsed();
        let advanced =
            Duration::from_nanos((advanced_ticks as u64).saturating_mul(100));
        let mut estimated = confirmed.saturating_sub(advanced);
        if let Some(first_segment) = startup.first_segment_served {
            estimated = estimated.max(first_segment);
        }
        Some(Self::startup_report(
            play_session_id.to_string(),
            startup,
            "started",
            Some(confirmed),
            Some(estimated),
            None,
        ))
    }

    pub fn fail_startup(
        &self,
        play_session_id: &str,
        reason: impl Into<String>,
    ) -> Option<PlaybackStartupReport> {
        let report = self.end_startup(play_session_id, "failed", reason)?;
        self.startup_failures
            .insert(
                report
                    .device_id
                    .clone(),
                PlaybackStartupFailure {
                    item_id: report.item_id,
                    user_id: report.user_id,
                    failed_at: Instant::now(),
                },
            );
        Some(report)
    }

    pub fn abandon_startup(
        &self,
        play_session_id: &str,
        reason: impl Into<String>,
    ) -> Option<PlaybackStartupReport> {
        self.end_startup(play_session_id, "abandoned", reason)
    }

    fn end_startup(
        &self,
        play_session_id: &str,
        outcome: &str,
        reason: impl Into<String>,
    ) -> Option<PlaybackStartupReport> {
        let (_, startup) = self
            .startups
            .remove(play_session_id)?;
        Some(Self::startup_report(
            play_session_id.to_string(),
            startup,
            outcome,
            None,
            None,
            Some(reason.into()),
        ))
    }

    /// Handle a `POST /sessions/playing` report.
    ///
    /// Enforces the per-user session limit, resolves the optional StreamGroup
    /// source, builds and inserts the `PlaybackSession`, and emits the playback-
    /// session-start log line (skipped for transcode — the HLS handler logs after
    /// it has codec/bitrate/reason details).
    pub async fn start(
        &self,
        db: &sqlx::SqlitePool,
        auth_session: &auth::AuthSession,
        data: &PlaybackInfo,
    ) -> anyhow::Result<Vec<String>> {
        let play_session_id = data
            .play_session_id
            .clone()
            .unwrap_or_else(|| {
                common::get_uuid()
                    .as_simple()
                    .to_string()
            });

        // Enforce per-user concurrent-stream limit.
        let max_sessions = auth_session
            .user
            .policy
            .as_ref()
            .map(|p| p.max_active_sessions)
            .unwrap_or(0);
        if max_sessions > 0 {
            // Exclude the caller's own device: insert() will replace any existing
            // session for that device, so it doesn't consume an extra slot.
            let current = self.count_for_user(
                auth_session
                    .user
                    .id,
                Some(
                    &auth_session
                        .device
                        .id,
                ),
            );
            if current >= max_sessions as usize {
                return Err(anyhow::anyhow!("Stream limit reached")
                    .context("Maximum concurrent streams reached"));
            }
        }

        let item_id = data.item_id;
        if item_id.is_nil() {
            warn!(
                client = %auth_session.device.app_name,
                "PlaybackStart missing item_id, skipping session creation"
            );
            return Ok(Vec::new());
        }

        // If the client selected a StreamGroup source, record its group UUID.
        let group_id: Option<Uuid> = if let Some(ref sid) = data.media_source_id {
            if let Ok(uid) = sid.parse::<Uuid>() {
                db::Media::get_by_id(db, &uid)
                    .await
                    .ok()
                    .flatten()
                    .filter(|m| m.kind == db::MediaKind::StreamGroup)
                    .map(|_| uid)
            } else {
                None
            }
        } else {
            None
        };

        let item_kind = db::Media::get_by_id(db, &item_id)
            .await
            .ok()
            .flatten()
            .map(|m| m.kind);

        let ps = PlaybackSession {
            play_session_id: play_session_id.clone(),
            user_id: auth_session
                .user
                .id,
            item_id,
            media_source_id: data
                .media_source_id
                .clone(),
            device_id: auth_session
                .device
                .id
                .clone(),
            client_name: auth_session
                .device
                .app_name
                .clone(),
            position_ticks: data
                .position_ticks
                .unwrap_or(0),
            can_seek: data.can_seek,
            is_paused: data.is_paused,
            last_paused_at: if data.is_paused {
                Some(Utc::now())
            } else {
                None
            },
            is_muted: data.is_muted,
            volume_level: data.volume_level,
            audio_stream_index: data.audio_stream_index,
            subtitle_stream_index: data.subtitle_stream_index,
            play_method: data
                .play_method
                .as_ref()
                .map(|m| m.to_string()),
            now_playing_queue: data
                .now_playing_queue
                .clone(),
            playlist_item_id: data
                .playlist_item_id
                .clone(),
            started_at: Utc::now(),
            last_activity: Utc::now(),
            transcode: None,
            group_id,
            item_kind,
        };

        let stopped_sessions = self
            .stop_other_for_device(
                &auth_session
                    .device
                    .id,
                &play_session_id,
            )
            .await;
        self.insert(ps);

        // For transcode sessions, master_hls_video fires the info log once it
        // has full codec/bitrate/reasons info. For direct play/stream, log here.
        let is_transcode = matches!(data.play_method, Some(PlayMethod::Transcode));
        if !is_transcode {
            // Best-effort: fetch media title and source path for the log line.
            let media_title = db::Media::get_by_id(db, &item_id)
                .await
                .ok()
                .flatten()
                .map(|m| m.title)
                .unwrap_or_default();

            let (source_title, source_path) =
                if let Some(ref sid) = data.media_source_id {
                    if let Ok(source_uuid) = sid.parse::<Uuid>() {
                        let m = db::Media::get_by_id(db, &source_uuid)
                            .await
                            .ok()
                            .flatten();
                        (
                            m.as_ref()
                                .map(|m| {
                                    m.title
                                        .clone()
                                }),
                            m.and_then(|m| {
                                m.stream_info
                                    .map(|si| si.descriptor)
                            }),
                        )
                    } else {
                        (None, None)
                    }
                } else {
                    (None, None)
                };

            let log_session_id = play_session_id
                .trim_start_matches("audio-")
                .trim_start_matches("video-");
            let position_secs = data
                .position_ticks
                .unwrap_or(0)
                / 10_000_000;
            info!(
                play_session_id = log_session_id,
                %item_id,
                title = %media_title,
                source = ?source_title,
                path = ?source_path,
                user = %auth_session.user.username,
                client = %auth_session.device.app_name,
                play_method = ?data.play_method,
                audio_stream = ?data.audio_stream_index,
                subtitle_stream = ?data.subtitle_stream_index,
                position_secs,
                "Playback session started"
            );
        }

        Ok(stopped_sessions)
    }

    /// Handle a `POST /sessions/playing/progress` report.
    ///
    /// Updates the in-memory session state, notifies the transcode buffer
    /// monitor, persists the playback position to the DB, and logs stream-
    /// selection changes.
    pub async fn progress(
        &self,
        db: &sqlx::SqlitePool,
        user: &db::User,
        psid: &str,
        data: &PlaybackInfo,
    ) -> anyhow::Result<()> {
        let ps_snapshot = self.get(psid);
        let ps = match ps_snapshot.as_ref() {
            Some(ps) => ps,
            None => return Ok(()),
        };

        // If the session has no valid item and the progress report can't supply one,
        // it's a ghost (e.g. an unclaimed transcode stub). Evict it instead of
        // keeping it alive via last_activity.
        if ps
            .item_id
            .is_nil()
            && data
                .item_id
                .is_nil()
        {
            self.sessions
                .remove(psid);
            return Ok(());
        }

        let item_id = if !data
            .item_id
            .is_nil()
        {
            data.item_id
        } else {
            ps.item_id
        };

        if let Some(report) = self.confirm_startup(
            psid,
            ps.position_ticks,
            data.position_ticks,
            data.is_paused,
        ) {
            if let Err(error) = db::record_playback_startup(db, &report).await {
                warn!(%error, play_session_id = psid, "failed to persist playback startup metric");
            }
            info!(
                play_session_id = %report.play_session_id,
                item_id = %report.item_id,
                media_source_id = ?report.media_source_id,
                user_id = %report.user_id,
                client = %report.client_name,
                playback_info_ms = ?report.playback_info_ms,
                hls_requested_ms = ?report.hls_requested_ms,
                source_selected_ms = ?report.source_selected_ms,
                transcode_started_ms = ?report.transcode_started_ms,
                first_segment_served_ms = ?report.first_segment_served_ms,
                first_progress_ms = ?report.first_progress_ms,
                estimated_actual_playback_ms = ?report.estimated_actual_playback_ms,
                confirmation_delay_ms = ?report.confirmation_delay_ms,
                "▶ Actual playback confirmed"
            );
        }

        // Detect encode-parameter changes and log them once.
        // We ignore pause/unpause — those are not encode changes.
        let audio_changed = data
            .audio_stream_index
            .is_some()
            && data.audio_stream_index != ps.audio_stream_index;
        let subtitle_changed = data
            .subtitle_stream_index
            .is_some()
            && data.subtitle_stream_index != ps.subtitle_stream_index;
        let method_changed = data
            .play_method
            .is_some()
            && data
                .play_method
                .as_ref()
                .map(|m| m.to_string())
                != ps.play_method;
        if audio_changed || subtitle_changed || method_changed {
            info!(
                play_session_id = psid.trim_start_matches("audio-").trim_start_matches("video-"),
                item_id = %item_id,
                user = %user.username,
                audio_stream = if audio_changed {
                    format!("{:?} → {:?}", ps.audio_stream_index, data.audio_stream_index)
                } else {
                    format!("{:?}", ps.audio_stream_index)
                },
                subtitle_stream = if subtitle_changed {
                    format!("{:?} → {:?}", ps.subtitle_stream_index, data.subtitle_stream_index)
                } else {
                    format!("{:?}", ps.subtitle_stream_index)
                },
                play_method = if method_changed {
                    format!("{:?} → {:?}", ps.play_method, data.play_method)
                } else {
                    format!("{:?}", ps.play_method)
                },
                "⟳ Playback params changed"
            );
        }

        self.update(psid, |ps| {
            if !data
                .item_id
                .is_nil()
            {
                ps.item_id = data.item_id;
            }
            ps.position_ticks = data
                .position_ticks
                .unwrap_or(ps.position_ticks);
            if data.is_paused && !ps.is_paused {
                ps.last_paused_at = Some(Utc::now());
            } else if !data.is_paused {
                ps.last_paused_at = None;
            }
            ps.is_paused = data.is_paused;
            ps.is_muted = data.is_muted;
            ps.volume_level = data
                .volume_level
                .or(ps.volume_level);
            ps.audio_stream_index = data
                .audio_stream_index
                .or(ps.audio_stream_index);
            ps.subtitle_stream_index = data
                .subtitle_stream_index
                .or(ps.subtitle_stream_index);
            if let Some(ref m) = data.play_method {
                ps.play_method = Some(m.to_string());
            }
            ps.last_activity = Utc::now();
        });

        // Update transcode buffer monitor with actual playback position.
        if let Some(position_ticks) = data.position_ticks {
            if let Some(ref ts_lock) = ps.transcode {
                if let Ok(ts) = ts_lock.try_read() {
                    let position_secs = (position_ticks / 10_000_000) as u32;
                    let offset = position_secs.saturating_sub(ts.start_time_secs);
                    ts.playback_offset_secs
                        .store(offset, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }

        // Persist position to DB (no watched-threshold check on progress).
        let position_ticks = data
            .position_ticks
            .unwrap_or(ps.position_ticks);
        let selected_stream_id = if let Some(transcode) = ps
            .transcode
            .as_ref()
        {
            Some(
                transcode
                    .read()
                    .await
                    .media_source_id,
            )
        } else {
            data.media_source_id
                .as_deref()
                .or(ps
                    .media_source_id
                    .as_deref())
                .and_then(|id| {
                    id.parse::<Uuid>()
                        .ok()
                })
        };
        if let Ok(Some(media)) = db::Media::get_by_id(db, &item_id).await {
            let cfg = user
                .configuration
                .as_ref()
                .map(|c| {
                    c.0.clone()
                })
                .unwrap_or_default();

            let audio_idx = if cfg.remember_audio_selections {
                data.audio_stream_index
                    .or(ps.audio_stream_index)
                    .map(|x| x as i64)
            } else {
                None
            };
            let subtitle_idx = if cfg.remember_subtitle_selections {
                data.subtitle_stream_index
                    .or(ps.subtitle_stream_index)
                    .map(|x| x as i64)
            } else {
                None
            };

            db::UserMediaState::update_playback(
                db,
                user,
                &media,
                position_ticks,
                selected_stream_id,
                audio_idx,
                subtitle_idx,
                media.runtime,
            )
            .await?;
        }

        Ok(())
    }

    /// Handle a `POST /sessions/playing/stopped` report.
    ///
    /// Removes the playback session (stopping any active transcode), persists
    /// the final position to the DB (with the 90 % watched-mark check), and
    /// emits a debug log line.
    ///
    /// Returns whether this stop crossed the watched threshold.
    pub async fn stopped(
        &self,
        db: &sqlx::SqlitePool,
        user: &db::User,
        psid: &str,
        data: &PlaybackInfo,
    ) -> anyhow::Result<bool> {
        let ps = self
            .stop(psid)
            .await;

        let item_id = Some(data.item_id)
            .filter(|id| !id.is_nil())
            .or_else(|| {
                ps.as_ref()
                    .map(|s| s.item_id)
            });
        let final_ticks = data
            .position_ticks
            .or_else(|| {
                ps.as_ref()
                    .map(|s| s.position_ticks)
            });
        let selected_stream_id = if let Some(session) = ps.as_ref() {
            if let Some(transcode) = session
                .transcode
                .as_ref()
            {
                Some(
                    transcode
                        .read()
                        .await
                        .media_source_id,
                )
            } else {
                data.media_source_id
                    .as_deref()
                    .or(session
                        .media_source_id
                        .as_deref())
                    .and_then(|id| {
                        id.parse::<Uuid>()
                            .ok()
                    })
            }
        } else {
            data.media_source_id
                .as_deref()
                .and_then(|id| {
                    id.parse::<Uuid>()
                        .ok()
                })
        };

        let mut played = false;
        if let Some(item_id) = item_id {
            if let Ok(Some(media)) = db::Media::get_by_id(db, &item_id).await {
                played = db::UserMediaState::update_playback(
                    db,
                    user,
                    &media,
                    final_ticks.unwrap_or(0),
                    selected_stream_id,
                    None, // don't overwrite stream selections on stop
                    None,
                    media.runtime, // Some(runtime) triggers watched-threshold check
                )
                .await?;
            }
        }

        debug!(play_session_id = psid, "Playback stopped");
        Ok(played)
    }

    /// Insert (or replace) a playback session, preserving any transcode that was
    /// pre-attached before `report_playback_start` fired.
    /// Removes stale sessions for the same device so `get_sessions` always
    /// finds the most recent playback.
    pub fn insert(&self, mut session: PlaybackSession) {
        if session
            .transcode
            .is_none()
        {
            if let Some(existing) = self
                .sessions
                .get(&session.play_session_id)
            {
                session.transcode = existing
                    .value()
                    .transcode
                    .clone();
            }
        }
        // Remove any previous session for this device (different play_session_id).
        if !session
            .device_id
            .is_empty()
        {
            let stale: Vec<String> = self
                .sessions
                .iter()
                .filter(|e| {
                    e.value()
                        .device_id
                        == session.device_id
                        && e.key() != &session.play_session_id
                })
                .map(|e| {
                    e.key()
                        .clone()
                })
                .collect();
            for id in stale {
                self.sessions
                    .remove(&id);
            }
        }
        self.sessions
            .insert(
                session
                    .play_session_id
                    .clone(),
                session,
            );
    }

    /// Stop and remove playback sessions from the same device before a new
    /// video starts. This is intentionally async so stale ffmpeg processes are
    /// actually killed instead of merely disappearing from the session map.
    pub async fn stop_other_for_device(
        &self,
        device_id: &str,
        except_play_session_id: &str,
    ) -> Vec<String> {
        if device_id.is_empty() {
            return Vec::new();
        }
        let stale: Vec<String> = self
            .sessions
            .iter()
            .filter(|entry| {
                entry
                    .value()
                    .device_id
                    == device_id
                    && entry.key() != except_play_session_id
            })
            .map(|entry| {
                entry
                    .key()
                    .clone()
            })
            .collect();
        for id in &stale {
            self.stop(id)
                .await;
        }
        stale
    }

    /// Playback sessions that currently own a concrete stream source. HLS
    /// stubs use the resolved TranscodeSession media_source_id; direct-play
    /// sessions use PlaybackSession.media_source_id.
    pub async fn playback_ids_for_media_source(&self, source_id: Uuid) -> Vec<String> {
        let sessions = self.get_all();
        let mut ids = Vec::new();
        for session in sessions {
            let direct_match = session
                .media_source_id
                .as_deref()
                .and_then(|id| {
                    id.parse::<Uuid>()
                        .ok()
                })
                == Some(source_id);
            let transcode_match = if let Some(transcode) = session
                .transcode
                .as_ref()
            {
                transcode
                    .read()
                    .await
                    .media_source_id
                    == source_id
            } else {
                false
            };
            if direct_match || transcode_match {
                ids.push(session.play_session_id);
            }
        }
        ids
    }

    /// Resolve the playback owners for a concrete stream request. PlaybackInfo
    /// creates a startup before clients can send PlaybackStart, so a requested
    /// ID is valid when either phase currently knows about it.
    pub async fn playback_ids_for_stream(
        &self,
        source_id: Uuid,
        requested_play_session_id: Option<&str>,
    ) -> Vec<String> {
        let mut ids = self
            .playback_ids_for_media_source(source_id)
            .await;
        let Some(play_session_id) = requested_play_session_id else {
            return ids;
        };
        let session_exists = self
            .get(play_session_id)
            .is_some();
        if !session_exists && !self.has_startup(play_session_id) {
            return ids;
        }
        if session_exists {
            self.update(play_session_id, |session| {
                session.media_source_id = Some(source_id.to_string());
            });
        }
        if !ids
            .iter()
            .any(|id| id == play_session_id)
        {
            ids.push(play_session_id.to_string());
        }
        ids
    }

    /// Return a clone of the session, if it exists.
    pub fn get(&self, id: &str) -> Option<PlaybackSession> {
        self.sessions
            .get(id)
            .map(|e| {
                e.value()
                    .clone()
            })
    }

    /// Return a clone of the transcode session attached to this playback session.
    pub fn get_transcode(
        &self,
        id: &str,
    ) -> Option<Arc<tokio::sync::RwLock<TranscodeSession>>> {
        self.sessions
            .get(id)?
            .value()
            .transcode
            .clone()
    }

    /// Return clones of all active sessions.
    pub fn get_all(&self) -> Vec<PlaybackSession> {
        self.sessions
            .iter()
            .map(|e| {
                e.value()
                    .clone()
            })
            .collect()
    }

    /// Return the most recently active session for a device.
    /// Used as a fallback when the client omits PlaySessionId (e.g. DirectPlay).
    pub fn get_by_device(&self, device_id: &str) -> Option<PlaybackSession> {
        self.sessions
            .iter()
            .filter(|e| {
                e.value()
                    .device_id
                    == device_id
            })
            .max_by_key(|e| {
                e.value()
                    .last_activity
            })
            .map(|e| {
                e.value()
                    .clone()
            })
    }

    /// Count active sessions for a user, optionally excluding sessions from a
    /// specific device. Excluding the caller's device is correct when checking
    /// before `insert()`, since insert() replaces any existing session for that
    /// device and it shouldn't count toward the limit.
    pub fn count_for_user(&self, user_id: Uuid, exclude_device: Option<&str>) -> usize {
        self.sessions
            .iter()
            .filter(|e| {
                let s = e.value();
                s.user_id == user_id
                    && exclude_device.map_or(true, |d| s.device_id != d)
            })
            .count()
    }

    /// Update a session in-place via a closure.
    pub fn update<F: FnOnce(&mut PlaybackSession)>(&self, id: &str, f: F) {
        if let Some(mut entry) = self
            .sessions
            .get_mut(id)
        {
            f(entry.value_mut());
        }
    }

    /// Update `last_activity` on the session.
    pub fn ping(&self, id: &str) {
        self.update(id, |s| s.last_activity = Utc::now());
    }

    /// Attach a transcode session. If no playback session exists yet (the client
    /// calls master.m3u8 before POST /sessions/playing), a stub is inserted so the
    /// transcode isn't lost. `insert` will later overwrite the stub fields while
    /// preserving the transcode.
    ///
    /// When creating a stub, we inherit the `device_id` from the most recently
    /// active session that currently has no transcode. This covers quality-switch
    /// flows where the client sends a new play_session_id without first calling
    /// POST /Sessions/Playing — without this, `get_sessions` can't find the new
    /// transcode by device_id until the client reports playback again.
    pub fn attach_transcode(
        &self,
        id: &str,
        ts: Arc<tokio::sync::RwLock<TranscodeSession>>,
    ) {
        if let Some(mut entry) = self
            .sessions
            .get_mut(id)
        {
            entry
                .value_mut()
                .transcode = Some(ts);
        } else {
            // Inherit device_id from the freshest transcodeless session so that
            // get_sessions can find this stub by device_id immediately.
            let inherited_device_id = self
                .sessions
                .iter()
                .filter(|e| {
                    e.value()
                        .transcode
                        .is_none()
                        && !e
                            .value()
                            .device_id
                            .is_empty()
                })
                .max_by_key(|e| {
                    e.value()
                        .last_activity
                })
                .map(|e| {
                    e.value()
                        .device_id
                        .clone()
                })
                .unwrap_or_default();

            self.sessions
                .insert(
                    id.to_string(),
                    PlaybackSession {
                        play_session_id: id.to_string(),
                        transcode: Some(ts),
                        user_id: Uuid::nil(),
                        item_id: Uuid::nil(),
                        media_source_id: None,
                        device_id: inherited_device_id,
                        client_name: String::new(),
                        position_ticks: 0,
                        can_seek: true,
                        is_paused: false,
                        last_paused_at: None,
                        is_muted: false,
                        volume_level: None,
                        audio_stream_index: None,
                        subtitle_stream_index: None,
                        play_method: None,
                        now_playing_queue: None,
                        playlist_item_id: None,
                        started_at: Utc::now(),
                        last_activity: Utc::now(),
                        group_id: None,
                        item_kind: None,
                    },
                );
        }
    }

    /// Stop and remove the transcode from a session (e.g. on seek or client stop),
    /// but keep the playback session itself alive.
    pub async fn stop_transcode(&self, id: &str) {
        let ts = self
            .sessions
            .get_mut(id)
            .and_then(|mut e| {
                e.value_mut()
                    .transcode
                    .take()
            });
        if let Some(ts) = ts {
            kill_transcode(ts).await;
        }
    }

    /// Stop the transcode (if any) and remove the playback session entirely.
    /// Returns the removed session so callers can read final position/item data.
    pub async fn stop(&self, id: &str) -> Option<PlaybackSession> {
        let (_, session) = self
            .sessions
            .remove(id)?;
        if let Some(ts) = session
            .transcode
            .clone()
        {
            kill_transcode(ts).await;
        }
        Some(session)
    }

    /// Path where a given HLS segment lives on disk (used for disk-based recovery).
    pub fn segment_path(&self, play_session_id: &str, segment_id: &str) -> PathBuf {
        let session_dir = self
            .base_dir
            .join(play_session_id);

        if let Ok(entries) = std::fs::read_dir(&session_dir) {
            let mut latest_dir: Option<(PathBuf, std::time::SystemTime)> = None;
            for entry in entries.flatten() {
                if let Ok(metadata) = entry.metadata() {
                    if metadata.is_dir() {
                        let modified = metadata
                            .modified()
                            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
                        if latest_dir
                            .as_ref()
                            .map_or(true, |(_, max)| modified > *max)
                        {
                            latest_dir = Some((entry.path(), modified));
                        }
                    }
                }
            }
            if let Some((dir, _)) = latest_dir {
                return dir.join(format!("{}.ts", segment_id));
            }
        }

        session_dir.join(format!("{}.ts", segment_id))
    }

    pub fn base_dir(&self) -> &std::path::Path {
        &self.base_dir
    }

    pub fn active_session_ids(&self) -> Vec<String> {
        self.sessions
            .iter()
            .map(|e| {
                e.key()
                    .clone()
            })
            .collect()
    }

    /// Spawn a background task that reaps sessions idle longer than `max_age`.
    pub fn spawn_cleanup_task(
        self,
        interval: Duration,
        max_age: Duration,
        torrent: Arc<crate::torrent::TorrentManager>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker
                    .tick()
                    .await;
                let cutoff = Utc::now()
                    - chrono::Duration::from_std(max_age).unwrap_or_default();
                let stale: Vec<String> = self
                    .sessions
                    .iter()
                    .filter(|e| {
                        e.value()
                            .last_activity
                            < cutoff
                    })
                    .map(|e| {
                        e.key()
                            .clone()
                    })
                    .collect();
                for id in stale {
                    info!("Cleaning up idle session: {}", id);
                    self.stop(&id)
                        .await;
                    torrent
                        .release_playback(&id)
                        .await;
                }
            }
        })
    }
}

/// Kill an ffmpeg process and wait for it to exit before returning.
async fn kill_transcode(ts: Arc<tokio::sync::RwLock<TranscodeSession>>) {
    let (kill_tx, wait_done, output_dir) = {
        let mut s = ts
            .write()
            .await;
        (
            s.kill_tx
                .take(),
            s.wait_done
                .clone(),
            s.output_dir
                .clone(),
        )
    };
    if let Some(kill_tx) = kill_tx {
        let notification = wait_done.notified();
        let _ = kill_tx.send(());
        notification.await;
    }
    let _ = std::fs::remove_dir_all(&output_dir);
}

#[cfg(test)]
mod startup_metric_tests {
    use super::*;

    #[test]
    fn startup_progress_exposes_in_flight_milestones() {
        let temp = tempfile::tempdir().unwrap();
        let manager = PlaybackSessionManager::new(temp.path());
        let play_session_id = "progress-test";
        let user_id = Uuid::new_v4();
        let source_id = Uuid::new_v4();

        manager.begin_startup(
            play_session_id,
            Uuid::new_v4(),
            user_id,
            "device",
            "client",
        );
        manager.mark_playback_info_ready(play_session_id);
        manager.mark_hls_requested(play_session_id);
        manager.mark_source_selected(play_session_id, source_id);

        let progress = manager
            .startup_progress_for_device("device")
            .unwrap();
        assert_eq!(progress.play_session_id, play_session_id);
        assert_eq!(progress.user_id, user_id);
        assert!(progress.playback_info_ready);
        assert!(progress.hls_requested);
        assert!(progress.source_selected);
        assert!(!progress.transcode_started);
        assert!(!progress.first_segment_served);
    }

    #[test]
    fn startup_progress_uses_the_newest_attempt_for_a_device() {
        let temp = tempfile::tempdir().unwrap();
        let manager = PlaybackSessionManager::new(temp.path());
        let user_id = Uuid::new_v4();

        manager.begin_startup("older", Uuid::new_v4(), user_id, "device", "client");
        manager.begin_startup("newer", Uuid::new_v4(), user_id, "device", "client");
        manager
            .startups
            .get_mut("older")
            .unwrap()
            .started = Instant::now() - Duration::from_secs(1);

        assert_eq!(
            manager
                .startup_progress_for_device("device")
                .unwrap()
                .play_session_id,
            "newer"
        );
    }

    #[test]
    fn failed_startup_remains_visible_across_automatic_same_item_retry() {
        let temp = tempfile::tempdir().unwrap();
        let manager = PlaybackSessionManager::new(temp.path());
        let user_id = Uuid::new_v4();
        let item_id = Uuid::new_v4();

        manager.begin_startup("failed", item_id, user_id, "device", "client");
        manager
            .fail_startup("failed", "no playable source")
            .unwrap();

        assert!(manager.has_recent_startup_failure("device", user_id));
        manager.begin_startup("retry", item_id, user_id, "device", "client");
        assert!(manager.has_recent_startup_failure("device", user_id));
    }

    #[test]
    fn different_item_or_explicit_acknowledgement_clears_startup_failure() {
        let temp = tempfile::tempdir().unwrap();
        let manager = PlaybackSessionManager::new(temp.path());
        let user_id = Uuid::new_v4();

        manager.begin_startup("failed", Uuid::new_v4(), user_id, "device", "client");
        manager
            .fail_startup("failed", "no playable source")
            .unwrap();
        manager.begin_startup("different", Uuid::new_v4(), user_id, "device", "client");
        assert!(!manager.has_recent_startup_failure("device", user_id));

        manager
            .fail_startup("different", "no playable source")
            .unwrap();
        assert!(manager.has_recent_startup_failure("device", user_id));
        manager.clear_startup_failure("device");
        assert!(!manager.has_recent_startup_failure("device", user_id));
    }

    #[tokio::test]
    async fn startup_can_own_a_stream_until_it_reaches_a_terminal_outcome() {
        let temp = tempfile::tempdir().unwrap();
        let manager = PlaybackSessionManager::new(temp.path());
        let play_session_id = "ownership-test";
        let source_id = Uuid::new_v4();

        manager.begin_startup(
            play_session_id,
            Uuid::new_v4(),
            Uuid::new_v4(),
            "device",
            "client",
        );
        assert!(manager.has_startup(play_session_id));
        assert_eq!(
            manager
                .playback_ids_for_stream(source_id, Some(play_session_id))
                .await,
            vec![play_session_id.to_string()]
        );

        manager
            .fail_startup(play_session_id, "test complete")
            .unwrap();
        assert!(!manager.has_startup(play_session_id));
        assert!(
            manager
                .playback_ids_for_stream(source_id, Some(play_session_id))
                .await
                .is_empty()
        );
    }

    #[test]
    fn progress_backdates_actual_playback_but_not_before_first_segment() {
        let temp = tempfile::tempdir().unwrap();
        let manager = PlaybackSessionManager::new(temp.path());
        let play_session_id = "startup-test";
        manager.begin_startup(
            play_session_id,
            Uuid::new_v4(),
            Uuid::new_v4(),
            "device",
            "client",
        );
        manager.mark_first_segment_served(play_session_id);
        {
            let mut startup = manager
                .startups
                .get_mut(play_session_id)
                .unwrap();
            startup.started = Instant::now() - Duration::from_secs(10);
            startup.first_segment_served = Some(Duration::from_secs(3));
        }

        let report = manager
            .confirm_startup(play_session_id, 0, Some(9 * 10_000_000), false)
            .unwrap();

        assert_eq!(report.outcome, "started");
        assert_eq!(report.estimated_actual_playback_ms, Some(3_000));
        assert!(
            report
                .first_progress_ms
                .is_some_and(|value| value >= 9_900)
        );
        assert!(
            report
                .confirmation_delay_ms
                .is_some_and(|value| value >= 6_900)
        );
    }

    #[test]
    fn paused_or_nonadvancing_progress_does_not_confirm_playback() {
        let temp = tempfile::tempdir().unwrap();
        let manager = PlaybackSessionManager::new(temp.path());
        manager.begin_startup(
            "paused-test",
            Uuid::new_v4(),
            Uuid::new_v4(),
            "device",
            "client",
        );

        assert!(
            manager
                .confirm_startup("paused-test", 0, Some(10_000_000), true)
                .is_none()
        );
        assert!(
            manager
                .confirm_startup("paused-test", 0, Some(0), false)
                .is_none()
        );
    }

    #[test]
    fn stopping_before_progress_records_an_abandoned_attempt() {
        let temp = tempfile::tempdir().unwrap();
        let manager = PlaybackSessionManager::new(temp.path());
        manager.begin_startup(
            "abandoned-test",
            Uuid::new_v4(),
            Uuid::new_v4(),
            "device",
            "client",
        );
        manager.mark_hls_requested("abandoned-test");

        let report = manager
            .abandon_startup("abandoned-test", "client stopped")
            .unwrap();

        assert_eq!(report.outcome, "abandoned");
        assert_eq!(
            report
                .failure_reason
                .as_deref(),
            Some("client stopped")
        );
        assert!(
            report
                .hls_requested_ms
                .is_some()
        );
        assert!(
            manager
                .abandon_startup("abandoned-test", "duplicate stop")
                .is_none()
        );
    }
}
