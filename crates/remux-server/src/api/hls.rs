use std::{
    collections::VecDeque,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use axum::{
    Json,
    body::Body,
    extract::{Path, State},
    response::IntoResponse,
};
use axum_anyhow::ApiResult as Result;
use axum_extra::extract::Query;
use futures_util::StreamExt;
use http::{Response, StatusCode, header};
use remux_macros::get;
use serde::Serialize;
use tokio_util::io::ReaderStream;
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

use remux_sdks::remux::{HardwareAccelerationType, lang_to_two_letter};

use crate::{
    AppState, IntoApiError, OptionExt, ResultExt, api, common,
    common::{TickUnit, ToRunTimeTicks},
    db,
    db::auth,
    playback::{
        hw_accel,
        session::{TranscodeSession, TranscodeState},
    },
    services::StreamService,
};

/// Serializes the lookup-or-create-transcode sequence per play_session_id so
/// two racing requests for the same session can't each spawn their own
/// ffmpeg process.
static TRANSCODE_CREATE_LOCKS: crate::keyed_lock::KeyedLock<String> =
    crate::keyed_lock::KeyedLock::new();

const HEDGE_PREFIX_BYTES: u64 = 2 * 1024 * 1024;
const HEDGE_MIN_SAMPLE_BYTES: u64 = 512 * 1024;
const HEDGE_STEADY_SAMPLE_BYTES: u64 = 512 * 1024;
// Read beyond the initial startup prefix so a previously cached prefix cannot
// satisfy the network-throughput probe by itself.
const HEDGE_PROBE_BYTES: u64 = HEDGE_PREFIX_BYTES + HEDGE_STEADY_SAMPLE_BYTES;
// Require enough wall-clock observation after peer warmup to distinguish
// sustained transfer from one buffered torrent-piece burst.
const HEDGE_MIN_STEADY_SAMPLE_TIME: Duration = Duration::from_millis(250);
const HEDGE_PRIMARY_HEAD_START: Duration = Duration::from_secs(1);
// The primary keeps a head start, then alternatives overlap progressively. In
// the worst case the initial wave samples 10 MiB across four losing swarms,
// while the fourth probe starts only if no earlier candidate proved decisive.
const HEDGE_TERTIARY_DELAY: Duration = Duration::from_millis(1_500);
const HEDGE_LATE_DELAY: Duration = Duration::from_secs(3);
const HEDGE_PROBE_WINDOW: Duration = Duration::from_secs(8);
// Jellyfin Web retries an unanswered master playlist at roughly 20 seconds.
// Keep the fast path below that deadline; slower patient probes continue in
// request-independent session creation and retries wait for the same result.
const HEDGE_TOTAL_BUDGET: Duration = Duration::from_secs(15);
// If the fast hedge cannot find a streamable source, keep every candidate
// connected long enough for tracker/DHT discovery and slow peers to mature.
// Four MiB across at most twelve swarms caps the speculative patient phase at
// 48 MiB while giving each source a full minute to prove it can move data.
const PATIENT_PROBE_BYTES: u64 = 4 * 1024 * 1024;
const PATIENT_PROBE_WINDOW: Duration = Duration::from_secs(60);
const PATIENT_PROBE_CONCURRENCY: usize = 12;
const PATIENT_SELECTION_GRACE: Duration = Duration::from_secs(1);
const HEDGE_REQUIRED_HEADROOM: f64 = 1.35;
const HEDGE_VIABLE_HEADROOM: f64 = 1.15;
// Torrent pieces can arrive in a single buffered burst after a long peer
// stall. Allow some cold-start recovery, but never let that burst alone make a
// source look sustainable.
const HEDGE_COLD_RECOVERY_FACTOR: f64 = 3.0;
const HEDGE_MIN_COLD_HEADROOM: f64 = 1.0 / HEDGE_COLD_RECOVERY_FACTOR;
const HEDGE_MIN_DEFICIT_RUNWAY: Duration = Duration::from_secs(60);
const HEDGE_WARM_STANDBY: Duration = Duration::from_secs(60);
const PREBUFFER_MIN_MEDIA_RUNWAY: Duration = Duration::from_secs(60);
const PREBUFFER_RATE_GRACE: Duration = Duration::from_secs(3);
const PREBUFFER_RATE_SAMPLE: Duration = Duration::from_secs(15);
const PREBUFFER_THROUGHPUT_SAFETY: f64 = 0.90;
const PREBUFFER_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const PREBUFFER_MAX_WAIT: Duration = Duration::from_secs(15 * 60);
const PREBUFFER_LOG_INTERVAL: Duration = Duration::from_secs(30);
const PREBUFFER_PLAN_TTL: Duration = Duration::from_secs(6 * 60 * 60);

static STARTUP_HEDGE_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .user_agent("remux-startup-hedge/1.0")
        .connect_timeout(Duration::from_secs(2))
        .pool_max_idle_per_host(4)
        .build()
        .expect("failed to build startup hedge client")
});

#[derive(Clone, Debug)]
struct StartupProbeResult {
    source_id: Uuid,
    title: String,
    range_start: u64,
    source_size_bytes: Option<u64>,
    bytes: u64,
    elapsed: Duration,
    warmup_elapsed: Option<Duration>,
    verified_live_sample: bool,
    required_bitrate_bps: Option<f64>,
    error: Option<String>,
}

#[derive(Debug)]
struct AutoPrebufferPlan {
    source_id: Uuid,
    range_start: u64,
    source_size_bytes: u64,
    required_bitrate_bps: f64,
    remaining_duration_seconds: f64,
    next_offset: AtomicU64,
    estimated_goodput_bps: AtomicU64,
    rate_trusted: AtomicBool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
struct PlaybackStartupStatusResponse {
    phase: &'static str,
    elapsed_milliseconds: u64,
    prebuffer: Option<PrebufferStatusResponse>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
struct PrebufferStatusResponse {
    downloaded_bytes: u64,
    target_bytes: u64,
    estimated_goodput_bits_per_second: u64,
    estimated_wait_seconds: Option<u64>,
    rate_trusted: bool,
}

impl AutoPrebufferPlan {
    fn from_probe(result: &StartupProbeResult) -> Option<Self> {
        let source_size_bytes = result.source_size_bytes?;
        let required_bitrate_bps = result
            .required_bitrate_bps
            .filter(|bitrate| *bitrate > 0.0)?;
        let next_offset = result
            .range_start
            .saturating_add(result.bytes)
            .min(source_size_bytes);
        let remaining_bytes = source_size_bytes.saturating_sub(result.range_start);
        let remaining_duration_seconds =
            remaining_bytes as f64 * 8.0 / required_bitrate_bps;

        Some(Self {
            source_id: result.source_id,
            range_start: result.range_start,
            source_size_bytes,
            required_bitrate_bps,
            remaining_duration_seconds,
            next_offset: AtomicU64::new(next_offset),
            estimated_goodput_bps: AtomicU64::new(
                result
                    .goodput_bps()
                    .to_bits(),
            ),
            // The short hedge proves that bytes are moving, but a longer
            // uncached sample is required before deciding how much runway is
            // safe for the rest of the movie.
            rate_trusted: AtomicBool::new(false),
        })
    }

    fn remaining_bytes(&self) -> u64 {
        self.source_size_bytes
            .saturating_sub(self.range_start)
    }

    fn downloaded_bytes(&self) -> u64 {
        self.next_offset
            .load(Ordering::Relaxed)
            .saturating_sub(self.range_start)
            .min(self.remaining_bytes())
    }

    fn estimated_goodput_bps(&self) -> f64 {
        f64::from_bits(
            self.estimated_goodput_bps
                .load(Ordering::Relaxed),
        )
    }

    fn target_bytes(&self) -> u64 {
        let remaining_bytes = self.remaining_bytes();
        if !self
            .rate_trusted
            .load(Ordering::Relaxed)
        {
            return remaining_bytes;
        }

        let safe_goodput = self.estimated_goodput_bps() * PREBUFFER_THROUGHPUT_SAFETY;
        let deficit_bps = (self.required_bitrate_bps - safe_goodput).max(0.0);
        let deficit_buffer =
            (deficit_bps * self.remaining_duration_seconds / 8.0).ceil() as u64;
        let minimum_runway = (self.required_bitrate_bps
            * PREBUFFER_MIN_MEDIA_RUNWAY.as_secs_f64()
            / 8.0)
            .ceil() as u64;

        deficit_buffer
            .max(minimum_runway)
            .min(remaining_bytes)
    }
}

impl PrebufferStatusResponse {
    fn from_plan(plan: &AutoPrebufferPlan) -> Self {
        let downloaded_bytes = plan.downloaded_bytes();
        let target_bytes = plan.target_bytes();
        let estimated_goodput = plan.estimated_goodput_bps();
        let rate_trusted = plan
            .rate_trusted
            .load(Ordering::Relaxed);
        let remaining_bytes = target_bytes.saturating_sub(downloaded_bytes);
        let estimated_wait_seconds = (rate_trusted
            && estimated_goodput.is_finite()
            && estimated_goodput > 0.0
            && remaining_bytes > 0)
            .then(|| {
                (remaining_bytes as f64 * 8.0 / estimated_goodput)
                    .ceil()
                    .min(u64::MAX as f64) as u64
            });

        Self {
            downloaded_bytes,
            target_bytes,
            estimated_goodput_bits_per_second: finite_u64(estimated_goodput),
            estimated_wait_seconds,
            rate_trusted,
        }
    }
}

fn finite_u64(value: f64) -> u64 {
    if value.is_finite() && value > 0.0 {
        value
            .round()
            .min(u64::MAX as f64) as u64
    } else {
        0
    }
}

fn duration_milliseconds(value: Duration) -> u64 {
    value
        .as_millis()
        .min(u64::MAX as u128) as u64
}

fn startup_phase(
    progress: &crate::playback_session::PlaybackStartupProgress,
    has_prebuffer: bool,
) -> &'static str {
    if progress.first_segment_served {
        "starting_playback"
    } else if progress.transcode_started {
        "preparing_video"
    } else if has_prebuffer {
        "prebuffering"
    } else if progress.source_selected {
        "opening_stream"
    } else if progress.hls_requested {
        "selecting_source"
    } else if progress.playback_info_ready {
        "starting_player"
    } else {
        "preparing_playback"
    }
}

impl StartupProbeResult {
    fn cold_goodput_bps(&self) -> f64 {
        self.bytes as f64 * 8.0
            / self
                .elapsed
                .as_secs_f64()
                .max(0.001)
    }

    fn goodput_bps(&self) -> f64 {
        let steady_bytes = self
            .bytes
            .saturating_sub(HEDGE_PREFIX_BYTES);
        if steady_bytes >= HEDGE_STEADY_SAMPLE_BYTES {
            if let Some(warmup_elapsed) = self.warmup_elapsed {
                let steady_elapsed = self
                    .elapsed
                    .saturating_sub(warmup_elapsed);
                if self.verified_live_sample
                    || steady_elapsed >= HEDGE_MIN_STEADY_SAMPLE_TIME
                {
                    return steady_bytes as f64 * 8.0
                        / steady_elapsed
                            .as_secs_f64()
                            .max(0.001);
                }
            }
        }
        self.cold_goodput_bps()
    }

    fn headroom(&self) -> Option<f64> {
        self.required_bitrate_bps
            .filter(|required| *required > 0.0)
            .map(|required| self.goodput_bps() / required)
    }

    fn cold_headroom(&self) -> Option<f64> {
        self.required_bitrate_bps
            .filter(|required| *required > 0.0)
            .map(|required| self.cold_goodput_bps() / required)
    }

    fn effective_goodput_bps(&self) -> f64 {
        self.goodput_bps()
            .min(self.cold_goodput_bps() * HEDGE_COLD_RECOVERY_FACTOR)
    }

    fn effective_headroom(&self) -> Option<f64> {
        self.required_bitrate_bps
            .filter(|required| *required > 0.0)
            .map(|required| self.effective_goodput_bps() / required)
    }

    fn is_strong(&self) -> bool {
        self.bytes >= HEDGE_PROBE_BYTES
            && !self.sample_is_inconclusive()
            && self
                .headroom()
                .is_some_and(|headroom| headroom >= HEDGE_REQUIRED_HEADROOM)
            && self
                .cold_headroom()
                .is_some_and(|headroom| headroom >= HEDGE_MIN_COLD_HEADROOM)
    }

    fn is_viable(&self) -> bool {
        self.bytes >= HEDGE_PROBE_BYTES
            && !self.sample_is_inconclusive()
            && self
                .headroom()
                .is_some_and(|headroom| headroom >= HEDGE_VIABLE_HEADROOM)
            && self
                .cold_headroom()
                .is_some_and(|headroom| headroom >= HEDGE_MIN_COLD_HEADROOM)
    }

    fn is_decisive(&self) -> bool {
        self.is_strong() || self.is_viable()
    }

    fn can_start_without_prebuffer(&self) -> bool {
        self.is_decisive()
            && self
                .cold_headroom()
                .is_some_and(|headroom| headroom >= HEDGE_VIABLE_HEADROOM)
    }

    fn is_stalled(&self) -> bool {
        self.bytes < HEDGE_MIN_SAMPLE_BYTES
    }

    fn sample_is_inconclusive(&self) -> bool {
        self.bytes >= HEDGE_PROBE_BYTES
            && !self.verified_live_sample
            && self.elapsed < HEDGE_MIN_STEADY_SAMPLE_TIME
    }

    fn deficit_runway(&self) -> Option<Duration> {
        let required = self
            .required_bitrate_bps
            .filter(|required| *required > 0.0)?;
        let deficit = required - self.effective_goodput_bps();
        (deficit > 0.0)
            .then(|| Duration::from_secs_f64(self.bytes as f64 * 8.0 / deficit))
    }

    fn is_unsustainable(&self) -> bool {
        self.is_stalled()
            || self.sample_is_inconclusive()
            || self
                .deficit_runway()
                .is_some_and(|runway| runway < HEDGE_MIN_DEFICIT_RUNWAY)
    }
}

fn auto_prebuffer_plan_key(play_session_id: &str) -> String {
    format!("auto-prebuffer:{play_session_id}")
}

fn auto_prebuffer_plan(
    state: &AppState,
    play_session_id: &str,
) -> Option<Arc<AutoPrebufferPlan>> {
    state
        .ctx
        .store
        .get(auto_prebuffer_plan_key(play_session_id))
}

fn save_auto_prebuffer_plan(
    state: &AppState,
    play_session_id: &str,
    result: &StartupProbeResult,
) -> Option<Arc<AutoPrebufferPlan>> {
    let plan = Arc::new(AutoPrebufferPlan::from_probe(result)?);
    state
        .ctx
        .store
        .save_arc_with_weight(
            auto_prebuffer_plan_key(play_session_id),
            plan.clone(),
            1,
            PREBUFFER_PLAN_TTL,
        );
    Some(plan)
}

fn clear_auto_prebuffer_plan(state: &AppState, play_session_id: &str) {
    state
        .ctx
        .store
        .delete(auto_prebuffer_plan_key(play_session_id));
}

#[get("/remux/playback/startup")]
pub async fn playback_startup_status(
    State(state): State<AppState>,
    session: auth::AuthSession,
) -> Result<impl IntoResponse> {
    let progress = state
        .ctx
        .sessions
        .startup_progress_for_device(
            &session
                .device
                .id,
        )
        .context_not_found("playback startup not found")?;
    if progress.user_id
        != session
            .user
            .id
    {
        return Err(anyhow::anyhow!("playback startup not found")
            .context_not_found("playback startup not found"));
    }

    let prebuffer = auto_prebuffer_plan(&state, &progress.play_session_id)
        .map(|plan| PrebufferStatusResponse::from_plan(&plan));
    let response = PlaybackStartupStatusResponse {
        phase: startup_phase(&progress, prebuffer.is_some()),
        elapsed_milliseconds: duration_milliseconds(progress.elapsed),
        prebuffer,
    };

    Ok(([(header::CACHE_CONTROL, "no-store")], Json(response)))
}

fn retain_prebuffer_candidate(
    candidates: &mut Vec<(db::Media, StartupProbeResult)>,
    candidate: &db::Media,
    result: &StartupProbeResult,
) -> bool {
    if !can_prebuffer_probe(result) {
        return false;
    }
    candidates.push((candidate.clone(), result.clone()));
    true
}

fn can_prebuffer_probe(result: &StartupProbeResult) -> bool {
    result.bytes >= HEDGE_PROBE_BYTES
        && result.goodput_bps() > 0.0
        && AutoPrebufferPlan::from_probe(result).is_some()
}

fn auto_prebuffer_url(port: u16, media_id: Uuid, play_session_id: &str) -> String {
    format!(
        "http://127.0.0.1:{port}/stream/{media_id}?PlaySessionId={}",
        urlencoding::encode(play_session_id)
    )
}

struct AutoPrebufferOwnership {
    torrent: Arc<crate::torrent::TorrentManager>,
    play_session_id: String,
    handed_off: bool,
}

impl AutoPrebufferOwnership {
    fn new(state: &AppState, play_session_id: &str) -> Self {
        Self {
            torrent: state
                .ctx
                .torrent
                .clone(),
            play_session_id: play_session_id.to_string(),
            handed_off: false,
        }
    }

    fn hand_off(&mut self) {
        self.handed_off = true;
    }

    async fn release(mut self) {
        self.handed_off = true;
        self.torrent
            .release_playback(&self.play_session_id)
            .await;
    }
}

impl Drop for AutoPrebufferOwnership {
    fn drop(&mut self) {
        if self.handed_off {
            return;
        }
        let torrent = self
            .torrent
            .clone();
        let play_session_id = self
            .play_session_id
            .clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                torrent
                    .release_playback(&play_session_id)
                    .await;
            });
        }
    }
}

async fn prebuffer_auto_source(
    state: &AppState,
    play_session_id: &str,
    media: &db::Media,
    plan: Arc<AutoPrebufferPlan>,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        plan.source_id == media.id,
        "Auto prebuffer source changed unexpectedly"
    );

    // Use the playback-aware stream endpoint so the prebuffer owns this
    // torrent for the user's session. Otherwise cleanup can treat the source
    // as idle and delete it while the user is still waiting for playback.
    let url = auto_prebuffer_url(
        state
            .ctx
            .config
            .port,
        media.id,
        play_session_id,
    );
    let mut next_offset = plan
        .next_offset
        .load(Ordering::Relaxed);
    if next_offset >= plan.source_size_bytes {
        return Ok(());
    }

    info!(
        %play_session_id,
        source_id = %media.id,
        source_title = %media.title,
        downloaded_bytes = plan.downloaded_bytes(),
        source_bytes = plan.remaining_bytes(),
        initial_goodput_mbps = plan.estimated_goodput_bps() / 1_000_000.0,
        required_mbps = plan.required_bitrate_bps / 1_000_000.0,
        "Auto source needs prebuffering before playback"
    );
    // The playback-aware endpoint claims the torrent during this request. If
    // prebuffering fails or is cancelled, release that provisional claim. On
    // success, transfer it to the HLS input request that follows immediately.
    let mut ownership = AutoPrebufferOwnership::new(state, play_session_id);
    let outcome: anyhow::Result<()> = async {
        let response = tokio::time::timeout(
            PREBUFFER_IDLE_TIMEOUT,
            STARTUP_HEDGE_CLIENT
                .get(&url)
                .header(http::header::RANGE, format!("bytes={next_offset}-"))
                .send(),
        )
        .await
        .map_err(|_| anyhow::anyhow!("Auto prebuffer timed out opening the source"))??
        .error_for_status()?;
        anyhow::ensure!(
            response.status() == StatusCode::PARTIAL_CONTENT,
            "Auto prebuffer server ignored byte range at offset {next_offset}"
        );
        if let Some(actual_size) = response_total_size(response.headers()) {
            anyhow::ensure!(
                actual_size == plan.source_size_bytes,
                "Auto prebuffer source size changed from {} to {} bytes",
                plan.source_size_bytes,
                actual_size
            );
        }

        let request_started = Instant::now();
        let mut rate_window_started = request_started + PREBUFFER_RATE_GRACE;
        let mut rate_window_bytes = 0u64;
        let mut last_log = Instant::now();
        let mut stream = response.bytes_stream();
        loop {
            let remaining =
                PREBUFFER_MAX_WAIT.saturating_sub(request_started.elapsed());
            anyhow::ensure!(
                !remaining.is_zero(),
                "Auto prebuffer exceeded {} seconds",
                PREBUFFER_MAX_WAIT.as_secs()
            );
            let wait = PREBUFFER_IDLE_TIMEOUT.min(remaining);
            let next = match tokio::time::timeout(wait, stream.next()).await {
                Ok(next) => next,
                Err(_) if remaining <= PREBUFFER_IDLE_TIMEOUT => {
                    anyhow::bail!(
                        "Auto prebuffer exceeded {} seconds",
                        PREBUFFER_MAX_WAIT.as_secs()
                    )
                }
                Err(_) => {
                    anyhow::bail!(
                        "Auto prebuffer received no data for {} seconds",
                        PREBUFFER_IDLE_TIMEOUT.as_secs()
                    )
                }
            };
            let Some(chunk) = next else {
                break;
            };
            let chunk = chunk?;
            let accepted = (chunk.len() as u64).min(
                plan.source_size_bytes
                    .saturating_sub(next_offset),
            );
            if accepted == 0 {
                break;
            }
            next_offset = next_offset.saturating_add(accepted);
            plan.next_offset
                .store(next_offset, Ordering::Relaxed);

            let now = Instant::now();
            if now >= rate_window_started {
                rate_window_bytes = rate_window_bytes.saturating_add(accepted);
                let sample_elapsed = now.saturating_duration_since(rate_window_started);
                if sample_elapsed >= PREBUFFER_RATE_SAMPLE {
                    let observed_bps = rate_window_bytes as f64 * 8.0
                        / sample_elapsed
                            .as_secs_f64()
                            .max(0.001);
                    let estimate = if plan
                        .rate_trusted
                        .load(Ordering::Relaxed)
                    {
                        plan.estimated_goodput_bps() * 0.70 + observed_bps * 0.30
                    } else {
                        observed_bps
                    };
                    plan.estimated_goodput_bps
                        .store(estimate.to_bits(), Ordering::Relaxed);
                    plan.rate_trusted
                        .store(true, Ordering::Relaxed);
                    rate_window_started = now;
                    rate_window_bytes = 0;
                }
            }

            let downloaded_bytes = plan.downloaded_bytes();
            let target_bytes = plan.target_bytes();
            if last_log.elapsed() >= PREBUFFER_LOG_INTERVAL {
                let estimated_bps = plan.estimated_goodput_bps();
                let remaining_to_target = target_bytes.saturating_sub(downloaded_bytes);
                let estimated_wait_seconds = (estimated_bps > 0.0)
                    .then(|| remaining_to_target as f64 * 8.0 / estimated_bps);
                info!(
                    %play_session_id,
                    source_id = %media.id,
                    downloaded_bytes,
                    target_bytes,
                    source_bytes = plan.remaining_bytes(),
                    estimated_goodput_mbps = estimated_bps / 1_000_000.0,
                    required_mbps = plan.required_bitrate_bps / 1_000_000.0,
                    estimated_wait_seconds,
                    rate_trusted = plan.rate_trusted.load(Ordering::Relaxed),
                    "Auto prebuffer progress"
                );
                last_log = now;
            }

            if downloaded_bytes >= plan.remaining_bytes()
                || (plan
                    .rate_trusted
                    .load(Ordering::Relaxed)
                    && downloaded_bytes >= target_bytes)
            {
                info!(
                    %play_session_id,
                    source_id = %media.id,
                    downloaded_bytes,
                    target_bytes,
                    source_bytes = plan.remaining_bytes(),
                    estimated_goodput_mbps = plan.estimated_goodput_bps() / 1_000_000.0,
                    required_mbps = plan.required_bitrate_bps / 1_000_000.0,
                    "Auto prebuffer reached safe playback runway"
                );
                return Ok(());
            }
        }

        anyhow::bail!(
            "Auto prebuffer ended after {} of {} bytes",
            plan.downloaded_bytes(),
            plan.target_bytes()
        )
    }
    .await;
    match outcome {
        Ok(()) => {
            ownership.hand_off();
            Ok(())
        }
        Err(error) => {
            ownership
                .release()
                .await;
            Err(error)
        }
    }
}

fn average_bitrate_bps(size_bytes: i64, duration_seconds: f64) -> Option<f64> {
    (size_bytes > 0 && duration_seconds > 0.0)
        .then_some(size_bytes as f64 * 8.0 / duration_seconds)
}

fn media_duration_seconds(media: &db::Media) -> Option<f64> {
    media
        .probe_data
        .as_ref()
        .and_then(|probe| probe.run_time_ticks)
        .filter(|ticks| *ticks > 0)
        .map(|ticks| ticks as f64 / 10_000_000.0)
        .or_else(|| {
            media
                .runtime
                .filter(|seconds| *seconds > 0)
                .map(|seconds| seconds as f64)
        })
        .or_else(|| {
            media
                .stream_info
                .as_ref()
                .and_then(|info| info.duration)
                .filter(|seconds| *seconds > 0)
                .map(|seconds| seconds as f64)
        })
}

fn required_source_bitrate_bps(
    media: &db::Media,
    fallback_duration_seconds: Option<f64>,
    observed_size_bytes: Option<u64>,
) -> Option<f64> {
    if let Some(bitrate) = media
        .probe_data
        .as_ref()
        .and_then(|probe| probe.bitrate)
        .filter(|bitrate| *bitrate > 0)
    {
        return Some(bitrate as f64);
    }

    let size = observed_size_bytes
        .and_then(|size| i64::try_from(size).ok())
        .or_else(|| {
            media
                .probe_data
                .as_ref()
                .and_then(|probe| probe.size)
        })
        .or_else(|| {
            media
                .stream_info
                .as_ref()
                .and_then(|info| info.size)
        })?;
    let duration_seconds =
        media_duration_seconds(media).or(fallback_duration_seconds)?;
    average_bitrate_bps(size, duration_seconds)
}

fn startup_probe_range_start(
    media: &db::Media,
    fallback_duration_seconds: Option<f64>,
    start_time_ticks: Option<i64>,
) -> u64 {
    let Some(start_time_ticks) = start_time_ticks.filter(|ticks| *ticks > 0) else {
        return 0;
    };
    let Some(duration_seconds) =
        media_duration_seconds(media).or(fallback_duration_seconds)
    else {
        return 0;
    };
    let Some(size_bytes) = media
        .probe_data
        .as_ref()
        .and_then(|probe| probe.size)
        .or_else(|| {
            media
                .stream_info
                .as_ref()
                .and_then(|info| info.size)
        })
        .filter(|size| *size > HEDGE_PROBE_BYTES as i64)
    else {
        return 0;
    };

    startup_probe_range_start_for_size(
        size_bytes as u64,
        duration_seconds,
        start_time_ticks,
    )
}

fn startup_probe_range_start_for_size(
    size_bytes: u64,
    duration_seconds: f64,
    start_time_ticks: i64,
) -> u64 {
    if size_bytes <= HEDGE_PROBE_BYTES || duration_seconds <= 0.0 {
        return 0;
    }
    let start_seconds = start_time_ticks.max(0) as f64 / 10_000_000.0;
    let fraction = (start_seconds / duration_seconds).clamp(0.0, 1.0);
    let estimated_offset = (size_bytes as f64 * fraction) as u64;
    estimated_offset.min(size_bytes.saturating_sub(HEDGE_PROBE_BYTES))
}

fn cache_bust_probe_range_start(
    source_size_bytes: u64,
    startup_range_start: u64,
) -> Option<u64> {
    let max_start = source_size_bytes.checked_sub(HEDGE_STEADY_SAMPLE_BYTES)?;
    let slots = max_start.checked_add(1)?;
    let nonce = common::get_uuid().as_u128() as u64;
    let mut candidate = nonce % slots;
    let startup_end = startup_range_start.saturating_add(HEDGE_PROBE_BYTES);
    let candidate_end = candidate.saturating_add(HEDGE_STEADY_SAMPLE_BYTES);
    let overlaps_startup =
        candidate < startup_end && candidate_end > startup_range_start;
    if overlaps_startup {
        if startup_end <= max_start {
            candidate = startup_end;
        } else if startup_range_start >= HEDGE_STEADY_SAMPLE_BYTES {
            candidate = startup_range_start - HEDGE_STEADY_SAMPLE_BYTES;
        } else {
            return None;
        }
    }
    Some(candidate)
}

fn response_total_size(headers: &http::HeaderMap) -> Option<u64> {
    headers
        .get(http::header::CONTENT_RANGE)?
        .to_str()
        .ok()?
        .rsplit_once('/')?
        .1
        .parse()
        .ok()
}

async fn probe_torrent_prefix(
    port: u16,
    media: db::Media,
    window: Duration,
    probe_bytes: u64,
    fallback_duration_seconds: Option<f64>,
    start_time_ticks: Option<i64>,
) -> StartupProbeResult {
    let started = Instant::now();
    let mut bytes = 0u64;
    let mut warmup_elapsed = None;
    let mut verified_live_sample = false;
    let mut range_start =
        startup_probe_range_start(&media, fallback_duration_seconds, start_time_ticks);
    let mut source_size_bytes = None;
    let url = format!("http://127.0.0.1:{port}/stream/{}", media.id);
    let outcome = tokio::time::timeout(window, async {
        let send_range = |range_start: u64, length: u64| {
            STARTUP_HEDGE_CLIENT
                .get(&url)
                .header(
                    http::header::RANGE,
                    format!(
                        "bytes={range_start}-{}",
                        range_start.saturating_add(length - 1)
                    ),
                )
                .send()
        };
        let mut response = send_range(range_start, probe_bytes).await?;
        source_size_bytes = response_total_size(response.headers());

        // Provider metadata may omit size or report a whole bundle. The stream
        // response knows the exact selected-file length, so correct a resumed
        // probe before downloading its sample.
        if let (Some(size_bytes), Some(start_time_ticks), Some(duration_seconds)) = (
            source_size_bytes,
            start_time_ticks.filter(|ticks| *ticks > 0),
            media_duration_seconds(&media).or(fallback_duration_seconds),
        ) {
            let corrected_range_start = startup_probe_range_start_for_size(
                size_bytes,
                duration_seconds,
                start_time_ticks,
            );
            if corrected_range_start != range_start {
                range_start = corrected_range_start;
                response = send_range(range_start, probe_bytes).await?;
                source_size_bytes =
                    response_total_size(response.headers()).or(source_size_bytes);
            }
        }
        let response = response.error_for_status()?;
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream
            .next()
            .await
        {
            let chunk = chunk?;
            bytes = bytes.saturating_add(chunk.len() as u64);
            if warmup_elapsed.is_none() && bytes >= HEDGE_PREFIX_BYTES {
                warmup_elapsed = Some(started.elapsed());
            }
            if bytes >= probe_bytes {
                break;
            }
        }

        // A complete prefix that arrives almost instantly may be entirely
        // cached from an earlier attempt. Verify current swarm throughput at a
        // random, non-overlapping file offset before calling it sustainable.
        if bytes >= probe_bytes && started.elapsed() < HEDGE_MIN_STEADY_SAMPLE_TIME {
            if let Some(live_range_start) = source_size_bytes
                .and_then(|size| cache_bust_probe_range_start(size, range_start))
            {
                bytes = HEDGE_PREFIX_BYTES;
                warmup_elapsed = Some(started.elapsed());
                verified_live_sample = true;
                let response = send_range(live_range_start, HEDGE_STEADY_SAMPLE_BYTES)
                    .await?
                    .error_for_status()?;
                let mut live_bytes = 0u64;
                let mut stream = response.bytes_stream();
                while let Some(chunk) = stream
                    .next()
                    .await
                {
                    let chunk = chunk?;
                    live_bytes = live_bytes.saturating_add(chunk.len() as u64);
                    if live_bytes >= HEDGE_STEADY_SAMPLE_BYTES {
                        break;
                    }
                }
                bytes = HEDGE_PREFIX_BYTES
                    .saturating_add(live_bytes.min(HEDGE_STEADY_SAMPLE_BYTES));
            }
        }
        Ok::<(), anyhow::Error>(())
    })
    .await;
    let error = match outcome {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(error.to_string()),
        Err(_) => Some("probe timed out".to_string()),
    };
    StartupProbeResult {
        source_id: media.id,
        title: media
            .title
            .clone(),
        range_start,
        source_size_bytes,
        bytes,
        elapsed: started.elapsed(),
        warmup_elapsed,
        verified_live_sample,
        required_bitrate_bps: required_source_bitrate_bps(
            &media,
            fallback_duration_seconds,
            source_size_bytes,
        ),
        error,
    }
}

fn log_startup_probe(role: &str, result: &StartupProbeResult) {
    info!(
        role,
        source_id = %result.source_id,
        title = %result.title,
        range_start = result.range_start,
        source_size_bytes = result.source_size_bytes,
        bytes = result.bytes,
        elapsed_ms = result.elapsed.as_millis() as u64,
        warmup_ms = result.warmup_elapsed.map(|elapsed| elapsed.as_millis() as u64),
        verified_live_sample = result.verified_live_sample,
        cold_goodput_mbps = result.cold_goodput_bps() / 1_000_000.0,
        goodput_mbps = result.goodput_bps() / 1_000_000.0,
        required_mbps = result.required_bitrate_bps.map(|bitrate| bitrate / 1_000_000.0),
        headroom = result.headroom(),
        cold_headroom = result.cold_headroom(),
        effective_headroom = result.effective_headroom(),
        deficit_runway_secs = result.deficit_runway().map(|runway| runway.as_secs_f64()),
        strong = result.is_strong(),
        viable = result.is_viable(),
        ready_without_prebuffer = result.can_start_without_prebuffer(),
        stalled = result.is_stalled(),
        error = ?result.error,
        "Auto startup hedge probe"
    );
}

async fn probe_patient_candidates(
    port: u16,
    candidates: &[db::Media],
    fallback_duration_seconds: Option<f64>,
    start_time_ticks: Option<i64>,
) -> Vec<(db::Media, StartupProbeResult)> {
    let mut probes = futures_util::stream::iter(
        candidates
            .iter()
            .cloned()
            .map(|candidate| async move {
                let result = probe_torrent_prefix(
                    port,
                    candidate.clone(),
                    PATIENT_PROBE_WINDOW,
                    PATIENT_PROBE_BYTES,
                    fallback_duration_seconds,
                    start_time_ticks,
                )
                .await;
                log_startup_probe("patient", &result);
                (candidate, result)
            }),
    )
    .buffer_unordered(PATIENT_PROBE_CONCURRENCY);
    let mut results = Vec::new();
    let mut selection_deadline: Option<Instant> = None;

    loop {
        let next = if let Some(deadline) = selection_deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, probes.next()).await {
                Ok(next) => next,
                Err(_) => break,
            }
        } else {
            probes
                .next()
                .await
        };
        let Some((candidate, result)) = next else {
            break;
        };
        let ready_without_prebuffer = result.can_start_without_prebuffer();
        if selection_deadline.is_none() && can_prebuffer_probe(&result) {
            selection_deadline = Some(Instant::now() + PATIENT_SELECTION_GRACE);
        }
        results.push((candidate, result));
        if ready_without_prebuffer {
            break;
        }
    }

    results
}

fn prefer_backup_probe(
    primary: &StartupProbeResult,
    backup: &StartupProbeResult,
) -> bool {
    let (primary_score, backup_score) =
        match (primary.effective_headroom(), backup.effective_headroom()) {
            (Some(primary), Some(backup)) => (primary, backup),
            _ => (
                primary.effective_goodput_bps(),
                backup.effective_goodput_bps(),
            ),
        };
    backup_score > primary_score * 1.10
}

fn preferred_probe_index(results: &[&StartupProbeResult]) -> usize {
    let mut preferred = 0;
    for candidate in 1..results.len() {
        if prefer_backup_probe(results[preferred], results[candidate]) {
            preferred = candidate;
        }
    }
    preferred
}

fn prefer_patient_probe(
    primary: &StartupProbeResult,
    backup: &StartupProbeResult,
) -> bool {
    let (primary_score, backup_score) = match (primary.headroom(), backup.headroom()) {
        (Some(primary), Some(backup)) => (primary, backup),
        _ => (primary.goodput_bps(), backup.goodput_bps()),
    };
    backup_score > primary_score * 1.10
}

fn preferred_patient_probe_index(results: &[&StartupProbeResult]) -> usize {
    let mut preferred = 0;
    for candidate in 1..results.len() {
        if prefer_patient_probe(results[preferred], results[candidate]) {
            preferred = candidate;
        }
    }
    preferred
}

fn schedule_warm_standby_cleanup(
    state: &AppState,
    play_session_id: &str,
    loser: &db::Media,
) {
    let Some(info_hash) = loser
        .stream_info
        .as_ref()
        .and_then(|info| {
            info.descriptor
                .torrent_info_hash()
        })
        .map(str::to_owned)
    else {
        return;
    };
    let ctx = state
        .ctx
        .clone();
    let play_session_id = play_session_id.to_string();
    let source_id = loser.id;
    tokio::spawn(async move {
        tokio::time::sleep(HEDGE_WARM_STANDBY).await;
        if StreamService::auto_session_choice(&ctx.store, &play_session_id)
            .is_some_and(|choice| choice.winner_id == source_id)
        {
            debug!(
                %play_session_id,
                %source_id,
                "keeping Auto warm standby because it became the active winner"
            );
            return;
        }
        match ctx
            .torrent
            .discard_warm_standby_if_unwatched(&ctx.db, source_id, &info_hash)
            .await
        {
            Ok(true) => {}
            Ok(false) => debug!(
                %play_session_id,
                %source_id,
                "kept or already removed Auto warm standby"
            ),
            Err(error) => warn!(
                %play_session_id,
                %source_id,
                %error,
                "failed to clean up Auto warm standby"
            ),
        }
    });
}

fn schedule_failed_probe_cleanup(state: &AppState, candidate: &db::Media) {
    let Some(info_hash) = candidate
        .stream_info
        .as_ref()
        .and_then(|info| {
            info.descriptor
                .torrent_info_hash()
        })
        .map(str::to_owned)
    else {
        return;
    };
    let ctx = state
        .ctx
        .clone();
    let source_id = candidate.id;
    tokio::spawn(async move {
        match ctx
            .torrent
            .discard_warm_standby_if_unwatched(&ctx.db, source_id, &info_hash)
            .await
        {
            Ok(true) => {}
            Ok(false) => debug!(
                %source_id,
                "kept or already removed failed Auto startup probe"
            ),
            Err(error) => warn!(
                %source_id,
                %error,
                "failed to clean up failed Auto startup probe"
            ),
        }
    });
}

async fn maybe_hedge_auto_startup(
    state: &AppState,
    play_session_id: &str,
    primary: db::Media,
    start_time_ticks: Option<i64>,
) -> anyhow::Result<db::Media> {
    let Some(choice) = StreamService::auto_session_choice(
        &state
            .ctx
            .store,
        play_session_id,
    ) else {
        return Ok(primary);
    };
    if choice.winner_id != primary.id {
        return Ok(primary);
    }
    if let Some(plan) = auto_prebuffer_plan(state, play_session_id) {
        if plan.source_id == primary.id {
            prebuffer_auto_source(state, play_session_id, &primary, plan).await?;
            clear_auto_prebuffer_plan(state, play_session_id);
            return Ok(primary);
        }
        clear_auto_prebuffer_plan(state, play_session_id);
    }
    let primary_hash = primary
        .stream_info
        .as_ref()
        .and_then(|info| {
            info.descriptor
                .torrent_info_hash()
        });
    let Some(primary_hash) = primary_hash else {
        return Ok(primary);
    };
    if state
        .ctx
        .torrent
        .managed_availability(primary_hash)
        .is_some_and(|availability| availability.finished)
    {
        clear_auto_prebuffer_plan(state, play_session_id);
        return Ok(primary);
    }

    let mut fallbacks = VecDeque::new();
    for fallback_id in choice
        .fallback_ids
        .iter()
        .copied()
        .filter(|fallback_id| *fallback_id != primary.id)
    {
        match db::Media::get_by_id(
            &state
                .ctx
                .db,
            &fallback_id,
        )
        .await
        {
            Ok(Some(fallback))
                if fallback
                    .stream_info
                    .as_ref()
                    .is_some_and(|info| info.is_p2p())
                    && !StreamService::is_auto_candidate_failed(
                        &state
                            .ctx
                            .store,
                        choice.item_id,
                        &choice.resolution,
                        &fallback,
                    ) =>
            {
                fallbacks.push_back(fallback);
            }
            Ok(_) => {}
            Err(error) => warn!(
                %play_session_id,
                %fallback_id,
                %error,
                "failed to load Auto hedge fallback"
            ),
        }
    }
    let Some(backup) = fallbacks.pop_front() else {
        return Ok(primary);
    };
    let tertiary = fallbacks.pop_front();
    let late = fallbacks.pop_front();
    let patient_candidates = std::iter::once(primary.clone())
        .chain(std::iter::once(backup.clone()))
        .chain(
            tertiary
                .iter()
                .cloned(),
        )
        .chain(
            late.iter()
                .cloned(),
        )
        .chain(
            fallbacks
                .iter()
                .cloned(),
        )
        .collect::<Vec<_>>();
    let mark_failed = |candidate: &db::Media| {
        StreamService::mark_auto_candidate_failed(
            &state
                .ctx
                .store,
            choice.item_id,
            &choice.resolution,
            candidate,
        );
        schedule_failed_probe_cleanup(state, candidate);
    };
    let finalize_selection =
        |selected: db::Media,
         standbys: Vec<db::Media>,
         remaining: &VecDeque<db::Media>| {
            let mut fallback_ids = standbys
                .iter()
                .map(|candidate| candidate.id)
                .collect::<Vec<_>>();
            fallback_ids.extend(
                remaining
                    .iter()
                    .map(|candidate| candidate.id),
            );
            StreamService::update_auto_session_choice(
                &state
                    .ctx
                    .store,
                play_session_id,
                selected.id,
                fallback_ids,
            );
            StreamService::remember_auto_choice(
                &state
                    .ctx
                    .store,
                choice.item_id,
                &choice.resolution,
                selected.id,
            );
            for standby in &standbys {
                schedule_warm_standby_cleanup(state, play_session_id, standby);
            }
            info!(
                %play_session_id,
                winner_id = %selected.id,
                winner_title = %selected.title,
                standby_ids = ?standbys.iter().map(|candidate| candidate.id).collect::<Vec<_>>(),
                standby_titles = ?standbys.iter().map(|candidate| candidate.title.as_str()).collect::<Vec<_>>(),
                remaining_fallbacks = remaining.len(),
                warm_standby_secs = HEDGE_WARM_STANDBY.as_secs(),
                "Auto startup hedge selected winner"
            );
            selected
        };
    let item_duration_seconds = match db::Media::get_by_id(
        &state
            .ctx
            .db,
        &choice.item_id,
    )
    .await
    {
        Ok(Some(item)) => media_duration_seconds(&item),
        Ok(None) => None,
        Err(error) => {
            debug!(
                %play_session_id,
                item_id = %choice.item_id,
                %error,
                "could not load item duration for startup bitrate estimate"
            );
            None
        }
    };
    let hedge_started = Instant::now();

    info!(
        %play_session_id,
        primary_id = %primary.id,
        backup_id = %backup.id,
        tertiary_id = ?tertiary.as_ref().map(|candidate| candidate.id),
        late_id = ?late.as_ref().map(|candidate| candidate.id),
        remaining_fallbacks = fallbacks.len(),
        head_start_ms = HEDGE_PRIMARY_HEAD_START.as_millis() as u64,
        tertiary_delay_ms = HEDGE_TERTIARY_DELAY.as_millis() as u64,
        late_delay_ms = HEDGE_LATE_DELAY.as_millis() as u64,
        probe_bytes = HEDGE_PROBE_BYTES,
        total_budget_ms = HEDGE_TOTAL_BUDGET.as_millis() as u64,
        "starting bounded Auto startup hedge"
    );
    let mut primary_probe = Box::pin(probe_torrent_prefix(
        state
            .ctx
            .config
            .port,
        primary.clone(),
        HEDGE_PROBE_WINDOW,
        HEDGE_PROBE_BYTES,
        item_duration_seconds,
        start_time_ticks,
    ));
    let mut primary_result = None;
    tokio::select! {
        result = &mut primary_probe => {
            log_startup_probe("primary", &result);
            primary_result = Some(result);
        }
        _ = tokio::time::sleep(HEDGE_PRIMARY_HEAD_START) => {}
    }
    if primary_result
        .as_ref()
        .is_some_and(StartupProbeResult::is_decisive)
    {
        return Ok(primary);
    }

    let mut backup_probe = Box::pin(probe_torrent_prefix(
        state
            .ctx
            .config
            .port,
        backup.clone(),
        HEDGE_PROBE_WINDOW,
        HEDGE_PROBE_BYTES,
        item_duration_seconds,
        start_time_ticks,
    ));
    let tertiary_candidate = tertiary.clone();
    let tertiary_delay = HEDGE_TERTIARY_DELAY.saturating_sub(hedge_started.elapsed());
    let port = state
        .ctx
        .config
        .port;
    let mut tertiary_probe = Box::pin(async move {
        if let Some(candidate) = tertiary_candidate {
            tokio::time::sleep(tertiary_delay).await;
            Some(
                probe_torrent_prefix(
                    port,
                    candidate,
                    HEDGE_PROBE_WINDOW,
                    HEDGE_PROBE_BYTES,
                    item_duration_seconds,
                    start_time_ticks,
                )
                .await,
            )
        } else {
            std::future::pending::<Option<StartupProbeResult>>().await
        }
    });
    let late_candidate = late.clone();
    let late_delay = HEDGE_LATE_DELAY.saturating_sub(hedge_started.elapsed());
    let mut late_probe = Box::pin(async move {
        if let Some(candidate) = late_candidate {
            tokio::time::sleep(late_delay).await;
            Some(
                probe_torrent_prefix(
                    port,
                    candidate,
                    HEDGE_PROBE_WINDOW,
                    HEDGE_PROBE_BYTES,
                    item_duration_seconds,
                    start_time_ticks,
                )
                .await,
            )
        } else {
            std::future::pending::<Option<StartupProbeResult>>().await
        }
    });
    let mut backup_result = None;
    let mut tertiary_result = None;
    let mut late_result = None;
    let selected_index = loop {
        if primary_result
            .as_ref()
            .is_some_and(StartupProbeResult::is_decisive)
        {
            break Some(0);
        }
        if backup_result
            .as_ref()
            .is_some_and(StartupProbeResult::is_decisive)
        {
            break Some(1);
        }
        if tertiary_result
            .as_ref()
            .is_some_and(StartupProbeResult::is_decisive)
        {
            break Some(2);
        }
        if late_result
            .as_ref()
            .is_some_and(StartupProbeResult::is_decisive)
        {
            break Some(3);
        }
        if let (Some(primary_result), Some(backup_result)) =
            (&primary_result, &backup_result)
        {
            let first_two_unsustainable =
                primary_result.is_unsustainable() && backup_result.is_unsustainable();
            let waiting_for_staggered = first_two_unsustainable
                && ((tertiary.is_some() && tertiary_result.is_none())
                    || (late.is_some() && late_result.is_none()));
            if !waiting_for_staggered {
                let mut completed = vec![(0, primary_result), (1, backup_result)];
                if let Some(result) = tertiary_result.as_ref() {
                    completed.push((2, result));
                }
                if let Some(result) = late_result.as_ref() {
                    completed.push((3, result));
                }
                if completed
                    .iter()
                    .all(|(_, result)| result.is_unsustainable())
                {
                    break None;
                }
                let preferred = preferred_probe_index(
                    &completed
                        .iter()
                        .map(|(_, result)| *result)
                        .collect::<Vec<_>>(),
                );
                break Some(completed[preferred].0);
            }
        }
        tokio::select! {
            result = &mut primary_probe, if primary_result.is_none() => {
                log_startup_probe("primary", &result);
                primary_result = Some(result);
            }
            result = &mut backup_probe, if backup_result.is_none() => {
                log_startup_probe("backup", &result);
                backup_result = Some(result);
            }
            result = &mut tertiary_probe, if tertiary.is_some() && tertiary_result.is_none() => {
                if let Some(result) = result {
                    log_startup_probe("tertiary", &result);
                    tertiary_result = Some(result);
                }
            }
            result = &mut late_probe, if late.is_some() && late_result.is_none() => {
                if let Some(result) = result {
                    log_startup_probe("late", &result);
                    late_result = Some(result);
                }
            }
        }
    };

    let mut initial_candidates = vec![primary, backup];
    let mut initial_results = vec![primary_result, backup_result];
    if let Some(tertiary) = tertiary {
        initial_candidates.push(tertiary);
        initial_results.push(tertiary_result);
    }
    if let Some(late) = late {
        initial_candidates.push(late);
        initial_results.push(late_result);
    }

    if let Some(selected_index) = selected_index {
        let selected = initial_candidates.remove(selected_index);
        initial_results.remove(selected_index);
        let mut standbys = Vec::new();
        for (candidate, result) in initial_candidates
            .into_iter()
            .zip(initial_results)
        {
            if result
                .as_ref()
                .is_some_and(StartupProbeResult::is_unsustainable)
            {
                mark_failed(&candidate);
            } else {
                standbys.push(candidate);
            }
        }
        return Ok(finalize_selection(selected, standbys, &fallbacks));
    }

    let failed = initial_results
        .iter()
        .filter(|result| {
            result
                .as_ref()
                .is_none_or(StartupProbeResult::is_unsustainable)
        })
        .count();
    info!(
        %play_session_id,
        failed,
        remaining = fallbacks.len(),
        "initial Auto hedge found no sustainable source; advancing immediately"
    );

    while let Some(first) = fallbacks.pop_front() {
        let second = fallbacks.pop_front();
        let remaining_budget =
            HEDGE_TOTAL_BUDGET.saturating_sub(hedge_started.elapsed());
        if remaining_budget < Duration::from_millis(250) {
            break;
        }
        let probe_window = remaining_budget.min(HEDGE_PROBE_WINDOW);
        info!(
            %play_session_id,
            first_id = %first.id,
            second_id = ?second.as_ref().map(|candidate| candidate.id),
            probe_window_ms = probe_window.as_millis() as u64,
            remaining_after_wave = fallbacks.len(),
            "probing next Auto fallback wave"
        );

        let mut first_probe = Box::pin(probe_torrent_prefix(
            state
                .ctx
                .config
                .port,
            first.clone(),
            probe_window,
            HEDGE_PROBE_BYTES,
            item_duration_seconds,
            start_time_ticks,
        ));
        let Some(second) = second else {
            let first_result = first_probe.await;
            log_startup_probe("fallback", &first_result);
            if first_result.is_unsustainable() {
                continue;
            }
            return Ok(finalize_selection(first, Vec::new(), &fallbacks));
        };
        let mut second_probe = Box::pin(probe_torrent_prefix(
            state
                .ctx
                .config
                .port,
            second.clone(),
            probe_window,
            HEDGE_PROBE_BYTES,
            item_duration_seconds,
            start_time_ticks,
        ));
        let mut first_result = None;
        let mut second_result = None;
        let selected_first = loop {
            if first_result
                .as_ref()
                .is_some_and(StartupProbeResult::is_decisive)
            {
                break true;
            }
            if second_result
                .as_ref()
                .is_some_and(StartupProbeResult::is_decisive)
            {
                break false;
            }
            if let (Some(first_result), Some(second_result)) =
                (&first_result, &second_result)
            {
                break !prefer_backup_probe(first_result, second_result);
            }
            tokio::select! {
                result = &mut first_probe, if first_result.is_none() => {
                    log_startup_probe("fallback-a", &result);
                    first_result = Some(result);
                }
                result = &mut second_probe, if second_result.is_none() => {
                    log_startup_probe("fallback-b", &result);
                    second_result = Some(result);
                }
            }
        };

        let wave_unsustainable = first_result
            .as_ref()
            .is_some_and(StartupProbeResult::is_unsustainable)
            && second_result
                .as_ref()
                .is_some_and(StartupProbeResult::is_unsustainable);
        if wave_unsustainable {
            continue;
        }

        let (selected, loser, loser_result) = if selected_first {
            (first, second, second_result.as_ref())
        } else {
            (second, first, first_result.as_ref())
        };
        let loser = if loser_result.is_some_and(StartupProbeResult::is_unsustainable) {
            if loser_result.is_some_and(|result| {
                result.bytes >= HEDGE_PROBE_BYTES
                    && result.goodput_bps() > 0.0
                    && AutoPrebufferPlan::from_probe(result).is_some()
            }) {
                Some(loser)
            } else {
                mark_failed(&loser);
                None
            }
        } else {
            Some(loser)
        };
        return Ok(finalize_selection(
            selected,
            loser
                .into_iter()
                .collect(),
            &fallbacks,
        ));
    }

    info!(
        %play_session_id,
        candidates = patient_candidates.len(),
        concurrency = PATIENT_PROBE_CONCURRENCY,
        probe_bytes = PATIENT_PROBE_BYTES,
        window_seconds = PATIENT_PROBE_WINDOW.as_secs(),
        "fast Auto hedge found no streamable source; starting patient probe"
    );
    let patient_results = probe_patient_candidates(
        state
            .ctx
            .config
            .port,
        &patient_candidates,
        item_duration_seconds,
        start_time_ticks,
    )
    .await;
    let completed_patient_ids: std::collections::HashSet<_> = patient_results
        .iter()
        .map(|(candidate, _)| candidate.id)
        .collect();
    for candidate in &patient_candidates {
        if !completed_patient_ids.contains(&candidate.id) {
            schedule_failed_probe_cleanup(state, candidate);
        }
    }
    let mut prebuffer_candidates = Vec::new();
    for (candidate, result) in patient_results {
        if !retain_prebuffer_candidate(&mut prebuffer_candidates, &candidate, &result) {
            mark_failed(&candidate);
        }
    }

    if !prebuffer_candidates.is_empty() {
        let preferred = preferred_patient_probe_index(
            &prebuffer_candidates
                .iter()
                .map(|(_, result)| result)
                .collect::<Vec<_>>(),
        );
        let (selected, result) = prebuffer_candidates.remove(preferred);
        let standbys = prebuffer_candidates
            .into_iter()
            .map(|(candidate, _)| candidate)
            .collect::<Vec<_>>();
        let no_remaining = VecDeque::new();
        let selected = finalize_selection(selected, standbys, &no_remaining);
        // A warm burst after a long peer-discovery stall can look fast while
        // the full cold-start sample is still slower than playback. Keep that
        // source, but build a measured runway before handing it to FFmpeg.
        if result.can_start_without_prebuffer() {
            return Ok(selected);
        }
        let plan = save_auto_prebuffer_plan(state, play_session_id, &result)
            .ok_or_else(|| {
                anyhow::anyhow!("could not construct Auto prebuffer plan")
            })?;
        if let Err(error) =
            prebuffer_auto_source(state, play_session_id, &selected, plan).await
        {
            clear_auto_prebuffer_plan(state, play_session_id);
            warn!(
                %play_session_id,
                source_id = %selected.id,
                source_title = %selected.title,
                %error,
                "Auto prebuffer stalled; advancing to another candidate"
            );
            mark_failed(&selected);
            StreamService::clear_auto_session_winner(
                &state
                    .ctx
                    .store,
                play_session_id,
            );
            let replacement = StreamService::resolve_auto(
                &state.ctx,
                choice.item_id,
                &choice.resolution,
                choice.user_id,
            )
            .await?;
            StreamService::pin_auto_session_winner(
                &state
                    .ctx
                    .store,
                play_session_id,
                choice.item_id,
                StreamService::auto_source_id(choice.item_id, &choice.resolution),
                replacement.id,
                choice.user_id,
            );
            return Box::pin(maybe_hedge_auto_startup(
                state,
                play_session_id,
                replacement,
                start_time_ticks,
            ))
            .await;
        }
        clear_auto_prebuffer_plan(state, play_session_id);
        return Ok(selected);
    }

    StreamService::clear_auto_session_winner(
        &state
            .ctx
            .store,
        play_session_id,
    );
    anyhow::bail!(
        "no Auto {} torrent delivered data during the {}-second patient probe",
        choice.resolution,
        PATIENT_PROBE_WINDOW.as_secs()
    )
}

async fn is_auto_media_source(
    ctx: &crate::AppContext,
    media_source_id: Option<uuid::Uuid>,
) -> bool {
    if let Some(id) = media_source_id {
        if ctx
            .store
            .get::<uuid::Uuid>(id.to_string())
            .is_some()
        {
            return true;
        }
        // This layer lacks the parent id required to reconstruct a deterministic
        // Auto UUID. The stored Auto-to-parent mapping is therefore the
        // authoritative marker here; create_hls_session performs full recovery.
    }
    false
}

async fn record_hls_startup_failure(
    state: &AppState,
    play_session_id: &str,
    item_id: Uuid,
    error_text: &str,
) {
    if let Some(report) = state
        .ctx
        .sessions
        .fail_startup(play_session_id, error_text)
    {
        if let Err(persist_error) = db::record_playback_startup(
            &state
                .ctx
                .db,
            &report,
        )
        .await
        {
            warn!(
                %persist_error,
                %play_session_id,
                "failed to persist playback startup failure"
            );
        }
        warn!(
            %play_session_id,
            %item_id,
            error = %error_text,
            "Playback startup failed"
        );
    }
    clear_auto_prebuffer_plan(state, play_session_id);
    StreamService::clear_auto_session_winner(
        &state
            .ctx
            .store,
        play_session_id,
    );
    state
        .ctx
        .torrent
        .release_playback(play_session_id)
        .await;
}

/// Shared session setup: look up or create the transcode session for an HLS
/// request. Returns the session handle and the resolved play_session_id.
async fn create_hls_session(
    state: &AppState,
    auth: &auth::AuthSession,
    id: Uuid,
    q: &api::HlsVideoQuery,
) -> Result<(Arc<tokio::sync::RwLock<TranscodeSession>>, String)> {
    let state = state.clone();
    let auth = auth.clone();
    let q = q.clone();
    let tracked_play_session_id = q
        .play_session_id
        .clone()
        .filter(|play_session_id| {
            state
                .ctx
                .sessions
                .has_startup(play_session_id)
        });

    // Jellyfin can abandon and retry a pending master-playlist request after
    // roughly 20 seconds. Run session creation independently of that request
    // so a patient torrent probe keeps its peers and progress; retries then
    // wait on the same per-session lock and reuse the attached transcode.
    tokio::spawn(async move {
        let creation = create_hls_session_inner(&state, &auth, id, &q);
        let Some(play_session_id) = tracked_play_session_id else {
            return creation.await;
        };
        let device_id = auth
            .device
            .id
            .clone();
        let sessions = state
            .ctx
            .sessions
            .clone();
        let watched_play_session_id = play_session_id.clone();
        tokio::select! {
            result = creation => {
                if let Err(error) = &result {
                    record_hls_startup_failure(
                        &state,
                        &play_session_id,
                        id,
                        &format!("{error:?}"),
                    )
                    .await;
                }
                result
            },
            _ = async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                    let is_current = sessions
                        .startup_progress_for_device(&device_id)
                        .is_some_and(|startup| {
                            startup.play_session_id == watched_play_session_id
                        });
                    if !is_current {
                        break;
                    }
                }
            } => {
                super::session::abandon_playback_startup(
                    &state,
                    &play_session_id,
                    "playback startup was cancelled",
                )
                .await;
                clear_auto_prebuffer_plan(&state, &play_session_id);
                state.ctx.torrent.release_playback(&play_session_id).await;
                Err(anyhow::anyhow!("playback startup was cancelled").into())
            }
        }
    })
    .await?
}

async fn create_hls_session_inner(
    state: &AppState,
    auth: &auth::AuthSession,
    id: Uuid,
    q: &api::HlsVideoQuery,
) -> Result<(Arc<tokio::sync::RwLock<TranscodeSession>>, String)> {
    let play_session_id = q
        .play_session_id
        .clone()
        .unwrap_or_else(|| {
            common::get_uuid()
                .as_simple()
                .to_string()
        });

    state
        .ctx
        .sessions
        .mark_hls_requested(&play_session_id);
    debug!("Using play session ID: {}", play_session_id);

    // Switching videos on one device must stop the previous ffmpeg input and
    // release its torrent before resolving the new source. Removing the map
    // entry alone leaves the old process and download running.
    let stopped_sessions = state
        .ctx
        .sessions
        .stop_other_for_device(
            &auth
                .device
                .id,
            &play_session_id,
        )
        .await;
    for stopped_id in stopped_sessions {
        super::session::abandon_playback_startup(
            state,
            &stopped_id,
            "replaced by another video on the same device",
        )
        .await;
        state
            .ctx
            .torrent
            .release_playback(&stopped_id)
            .await;
    }

    let encoding_opts_hls = crate::db::Settings::get_encoding_config(
        &state
            .ctx
            .db,
    )
    .await
    .unwrap_or_default();
    let video_transcode_enabled_hls = encoding_opts_hls
        .enable_video_transcoding
        .unwrap_or(true);
    let video_codec_raw = q
        .video_codec
        .as_deref()
        .unwrap_or("copy");
    let video_codec = if video_codec_raw == "copy" || !video_transcode_enabled_hls {
        "copy".to_string()
    } else {
        "h264".to_string()
    };
    let audio_codec = q
        .audio_codec
        .clone()
        .unwrap_or_else(|| "aac".to_string());
    let segment_length = q
        .segment_length
        .unwrap_or(6) as u32;

    // Look up existing session or create a new one.
    // When the client seeks it sends the same PlaySessionId but with a new
    // StartTimeTicks.  In that case we must stop the old transcode job and
    // restart from the requested position — otherwise the player waits for
    // segments that the old job will never produce at the new offset.
    //
    // Serialize the whole stop/lookup/create/attach sequence per
    // play_session_id: the lookup-or-create path below awaits DB queries and
    // filesystem ops with no lock held, so two requests racing for the same
    // session would otherwise both see no existing transcode and each spawn
    // their own ffmpeg process, with the loser's session silently overwritten
    // (and its ffmpeg process orphaned) by attach_transcode.
    let _create_guard = TRANSCODE_CREATE_LOCKS
        .lock(play_session_id.clone())
        .await;
    let requested_start_secs = q
        .start_time_ticks
        .unwrap_or(0)
        .max(0) as u64
        / 10_000_000;
    let existing_start_secs = if let Some(existing) = state
        .ctx
        .sessions
        .get_transcode(&play_session_id)
    {
        Some(
            existing
                .read()
                .await
                .start_time_secs as u64,
        )
    } else {
        None
    };
    let is_seek_restart =
        existing_start_secs.is_some_and(|current| current != requested_start_secs);
    if is_seek_restart {
        debug!(
            play_session_id = %play_session_id,
            previous_start_secs = ?existing_start_secs,
            requested_start_secs,
            "seek detected — stopping old transcode session and restarting"
        );
        state
            .ctx
            .sessions
            .stop_transcode(&play_session_id)
            .await;
    }
    let session = if let Some(existing) = state
        .ctx
        .sessions
        .get_transcode(&play_session_id)
    {
        existing
    } else {
        // Fetch media info to get the stream URL
        let requested_media_source_id = q
            .media_source_id
            .unwrap_or(id);
        let media_source_id = StreamService::auto_session_winner(
            &state
                .ctx
                .store,
            &play_session_id,
        )
        .unwrap_or(requested_media_source_id);
        if media_source_id != requested_media_source_id {
            debug!(
                play_session_id,
                winner_id = %media_source_id,
                "using auto winner pinned by PlaybackInfo"
            );
        }
        let mut media = match db::Media::get_by_id(
            &state
                .ctx
                .db,
            &media_source_id,
        )
        .await?
        {
            Some(m) => m,
            None => {
                // Synthetic Auto sources are not database rows. Recover their
                // parent and resolution tier so source selection can run again.
                let parent_from_store = state
                    .ctx
                    .store
                    .get::<uuid::Uuid>(media_source_id.to_string())
                    .map(|u| *u);
                let mut found = None;
                let mut parent_id = id;
                if let Some(pid) = parent_from_store {
                    if let Some(res) =
                        StreamService::auto_resolution(pid, media_source_id)
                    {
                        found = Some(res);
                        parent_id = pid;
                    }
                }
                if found.is_none() {
                    if let Some(res) =
                        StreamService::auto_resolution(id, media_source_id)
                    {
                        found = Some(res);
                        parent_id = id;
                    }
                }
                // Some clients place the synthetic id in both the route and
                // MediaSourceId, so recover the parent from either value.
                if found.is_none() {
                    if let Some(pid) = state
                        .ctx
                        .store
                        .get::<uuid::Uuid>(id.to_string())
                        .map(|u| *u)
                    {
                        if let Some(res) =
                            StreamService::auto_resolution(pid, media_source_id)
                                .or_else(|| StreamService::auto_resolution(pid, id))
                        {
                            found = Some(res);
                            parent_id = pid;
                        }
                    }
                }
                if let Some(res) = found {
                    // Re-run selection because the previously chosen candidate
                    // may no longer be available.
                    if let Ok(w) = crate::services::StreamService::resolve_auto(
                        &state.ctx, parent_id, res, None,
                    )
                    .await
                    {
                        w
                    } else {
                        crate::db::Media {
                            id: media_source_id,
                            title: format!("{} (auto)", res),
                            kind: crate::db::MediaKind::Stream,
                            ..Default::default()
                        }
                    }
                } else {
                    None::<crate::db::Media>.context_not_found("media not found")?
                }
            }
        };

        // A placeholder can survive the first pass when source discovery was
        // temporarily empty. Resolve it again before constructing FFmpeg input.
        if media
            .title
            .ends_with("(auto)")
        {
            let res = media
                .title
                .trim_end_matches(" (auto)")
                .trim()
                .to_string();
            // Prefer the stored parent mapping; the route id is the fallback
            // for clients that address the parent item directly.
            let parent = state
                .ctx
                .store
                .get::<uuid::Uuid>(
                    media
                        .id
                        .to_string(),
                )
                .map(|u| *u)
                .or_else(|| {
                    state
                        .ctx
                        .store
                        .get::<uuid::Uuid>(id.to_string())
                        .map(|u| *u)
                })
                .unwrap_or(id);
            if let Ok(w) = crate::services::StreamService::resolve_auto(
                &state.ctx, parent, &res, None,
            )
            .await
            {
                media = w;
            }
        }

        let mut resolved_media = media.clone();
        if resolved_media.kind == db::MediaKind::StreamGroup {
            let gid = resolved_media.id;
            let candidates = db::StreamGroup::streams_for(
                &state
                    .ctx
                    .db,
                &gid,
                &id,
            )
            .await?;
            resolved_media = candidates
                .into_iter()
                .next()
                .context_not_found("no streams available for this group")?;
        }
        if matches!(
            resolved_media.kind,
            db::MediaKind::Movie | db::MediaKind::Episode
        ) {
            let sources = resolved_media
                .streams(
                    &state
                        .ctx
                        .db,
                )
                .await?;
            resolved_media = if let Some(wanted) = q.media_source_id {
                sources
                    .iter()
                    .find(|s| s.id == wanted)
                    .cloned()
            } else {
                None
            }
            .or_else(|| {
                sources
                    .into_iter()
                    .next()
            })
            .context_not_found("no playable source found")?;
        } else if resolved_media.kind == db::MediaKind::Track {
            let sources = resolved_media
                .streams(
                    &state
                        .ctx
                        .db,
                )
                .await?;
            resolved_media = sources
                .into_iter()
                .next()
                .context_not_found("no stream found for track")?;
        }

        // PlaybackInfo's numeric audio index belongs to the source selected at
        // that time. Auto hedging may replace it with a different release where
        // the same number denotes another language, so retain the semantic
        // language preference before the source can change.
        let requested_audio_stream_index = q
            .audio_stream_index
            .map(|value| value as i32)
            .filter(|index| *index >= 0);
        let parent_original_language = db::Media::get_by_id(
            &state
                .ctx
                .db,
            &id,
        )
        .await
        .ok()
        .flatten()
        .and_then(|parent| parent.original_language)
        .or_else(|| {
            media
                .original_language
                .clone()
        });
        let user_configuration = auth
            .user
            .configuration
            .as_ref()
            .map(|configuration| &configuration.0);
        let preferred_audio_language = audio_language_for_index(
            resolved_media
                .probe_data
                .as_ref(),
            requested_audio_stream_index,
        )
        .or_else(|| {
            if user_configuration
                .map_or(true, |configuration| configuration.play_default_audio_track)
            {
                parent_original_language
                    .as_deref()
                    .and_then(lang_to_two_letter)
            } else {
                user_configuration
                    .and_then(|configuration| {
                        configuration
                            .audio_language_preference
                            .as_deref()
                    })
                    .and_then(lang_to_two_letter)
            }
        });
        let pre_hedge_source_id = resolved_media.id;

        // PlaybackInfo normally pins the ranked Auto choice to the
        // PlaySessionId. HLS must also reconstruct that pin from the
        // deterministic Auto MediaSourceId: clients can retry the URL after
        // clearing their PlaybackInfo state or request the returned URL from a
        // separate playback context. Without this, the throughput hedge silently
        // degrades to the metadata-ranked source.
        if let Some(auto_id) = q.media_source_id {
            if StreamService::auto_resolution(id, auto_id).is_some() {
                StreamService::pin_auto_session_winner(
                    &state
                        .ctx
                        .store,
                    &play_session_id,
                    id,
                    auto_id,
                    resolved_media.id,
                    None,
                );
            }
        }

        // Auto startup hedging is intentionally server-side and runs whenever
        // a new transcode is created, including initial playback resumed from
        // a saved position. Its bounded probes target that requested offset.
        // A true in-session seek restarts the already-measured source directly.
        if !is_seek_restart {
            resolved_media = maybe_hedge_auto_startup(
                state,
                &play_session_id,
                resolved_media,
                q.start_time_ticks,
            )
            .await?;
        }
        if let Some(advertised_source_id) = q.media_source_id {
            crate::api::subtitles::remap_torrent_subtitle_routes(
                &state.ctx,
                &auth
                    .device
                    .id,
                id,
                advertised_source_id,
                &resolved_media,
            );
        }
        let source_changed = resolved_media.id != pre_hedge_source_id;
        state
            .ctx
            .sessions
            .mark_source_selected(&play_session_id, resolved_media.id);

        let input_url = resolved_media
            .stream_info
            .as_ref()
            .map(|si| {
                si.descriptor
                    .server_input(
                        resolved_media.id,
                        state
                            .ctx
                            .config
                            .port,
                    )
            })
            .context_not_found("media source has no URL")?;

        let output_dir = state
            .ctx
            .sessions
            .base_dir()
            .join(&play_session_id);
        // Keep the API stable (no RunId in URLs) by reusing one on-disk path per
        // PlaySessionId and clearing stale segments when a transcode restarts.
        let _ = std::fs::remove_dir_all(&output_dir);
        // Use the URL-path item (`id`) to determine liveness so that a specific
        // stream source (MediaSourceId = stream child, kind = Stream) doesn't
        // incorrectly produce is_live = false for live-TV sessions.
        let parent_is_tv_channel = if id != media_source_id {
            let db = &state
                .ctx
                .db;
            match db::Media::get_by_id(db, &id).await {
                Ok(opt) => opt.map_or(false, |m| m.kind == db::MediaKind::TvChannel),
                Err(e) => {
                    warn!(err = %e, item_id = %id, "failed to look up parent media for is_live; treating as not-live");
                    false
                }
            }
        } else {
            false
        };
        let is_live =
            resolved_media.kind == db::MediaKind::TvChannel || parent_is_tv_channel;

        // --- Why we force audio transcoding for live channels ---
        //
        // IPTV/broadcast streams frequently carry AAC encoded in LATM format
        // (Low-overhead MPEG-4 Audio Transport Multiplex). ffprobe identifies
        // LATM streams as codec "aac" but reports sample_rate=0 because the
        // sample rate is stored implicitly inside the AudioSpecificConfig
        // bitstream rather than in an ADTS header. When ffmpeg copies these
        // bits into an HLS/TS segment without re-encoding, the resulting
        // segment still contains LATM-framed audio.
        //
        // Native clients (Swiftfin, Streamyfin, VLC) decode via OS-level
        // hardware decoders that handle LATM transparently. Safari running
        // hls.js goes through the browser's Media Source Extensions (MSE)
        // API, which only accepts standard ADTS-framed AAC. Feeding it LATM
        // produces a silent or immediately-failed decode — the segment HTTP
        // response is 200 and the bytes arrive, but the player can't present
        // any video.
        //
        // The Jellyfin Web device profile declares MaxAudioChannels=6 and
        // lists "aac" as a supported codec without any channel-count or
        // sample-rate codec-profile conditions, so our PlaybackInfo decision
        // layer has no basis on which to reject copy — it looks like a
        // perfectly legal copy of a supported codec. Jellyfin server has a
        // SampleRate<=0 guard in CanStreamCopyAudio, but that guard is gated
        // on the client explicitly requesting an AudioSampleRate, which
        // Jellyfin Web does not do; so Jellyfin server has the same gap.
        //
        // Workaround: always re-encode audio to standard ADTS AAC stereo for
        // live channels, regardless of what the client negotiated. The
        // existing audio_channels logic (None for copy, Some(2) for transcode)
        // then kicks in automatically and produces the correct stereo downmix.
        let audio_codec = resolve_live_audio_codec(is_live, &audio_codec);

        // Live streams have no fixed duration — skip all runtime lookups.
        let runtime_ticks = if is_live {
            0
        } else {
            // Take the maximum of stored runtime and probe data so a stale/short
            // metadata value can't truncate the playlist for a longer file.
            let stored_ticks = resolved_media
                .runtime
                .or(media.runtime)
                .filter(|&r| r > 0)
                .and_then(|r| r.to_ticks(TickUnit::Seconds));
            let probe_ticks = resolved_media
                .probe_data
                .as_ref()
                .and_then(|p| p.run_time_ticks)
                .filter(|&t| t > 0);
            let rt = match (stored_ticks, probe_ticks) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (a, b) => a.or(b),
            };
            match rt {
                Some(t) if t > 0 => t,
                _ => db::Media::get_by_id(
                    &state
                        .ctx
                        .db,
                    &id,
                )
                .await
                .ok()
                .flatten()
                .and_then(|m| m.runtime)
                .filter(|&r| r > 0)
                .and_then(|r| r.to_ticks(TickUnit::Seconds))
                .unwrap_or(0),
            }
        };
        debug!(runtime_ticks, is_live, segment_length, "transcode session");
        let source_video_stream = resolved_media
            .probe_data
            .as_ref()
            .and_then(|p| p.video_stream());
        let source_video_codec = source_video_stream
            .as_ref()
            .and_then(|s| {
                s.codec
                    .clone()
            });
        let source_video_profile = source_video_stream
            .as_ref()
            .and_then(|s| {
                s.profile
                    .clone()
            });
        let source_video_level = source_video_stream
            .as_ref()
            .and_then(|s| s.level);
        let source_video_range_type = source_video_stream
            .as_ref()
            .and_then(|s| s.video_range_type);
        let source_video_width = source_video_stream
            .as_ref()
            .and_then(|s| s.width);
        let source_video_height = source_video_stream
            .as_ref()
            .and_then(|s| s.height);
        let source_frame_rate = source_video_stream
            .as_ref()
            .and_then(|s| s.real_frame_rate);
        debug!(
            ?source_video_codec,
            ?source_video_profile,
            ?source_video_level,
            ?source_video_range_type,
            source_video_width,
            source_video_height,
            source_frame_rate,
            "source video codec for HLS session"
        );
        let source_audio_stream = resolved_media
            .probe_data
            .as_ref()
            .and_then(|probe| {
                select_source_audio_stream(
                    probe,
                    requested_audio_stream_index,
                    preferred_audio_language.as_deref(),
                    source_changed,
                )
            });
        let source_audio_codec = source_audio_stream.and_then(|stream| {
            stream
                .codec
                .clone()
        });
        // PlaybackInfo describes the source selected before the startup hedge.
        // If the hedge switches to a release with a different audio codec, the
        // client's `AudioCodec=copy` decision is no longer trustworthy. MPEG-TS
        // browser HLS can safely copy the codecs below; transcode everything
        // else (notably Opus, DTS and TrueHD) to stereo AAC.
        let audio_codec =
            resolve_source_audio_codec(&audio_codec, source_audio_codec.as_deref());
        let has_stream = |index: i32, stream_type: api::MediaStreamType| {
            resolved_media
                .probe_data
                .as_ref()
                .map_or(true, |probe| {
                    probe
                        .media_streams
                        .iter()
                        .any(|stream| {
                            stream.index == i64::from(index)
                                && stream.type_ == Some(stream_type)
                        })
                })
        };
        let audio_stream_index = source_audio_stream
            .and_then(|stream| i32::try_from(stream.index).ok())
            .or_else(|| {
                (!source_changed
                    && resolved_media
                        .probe_data
                        .is_none())
                .then_some(requested_audio_stream_index)
                .flatten()
            });
        if source_changed && requested_audio_stream_index != audio_stream_index {
            info!(
                play_session_id,
                from_source_id = %pre_hedge_source_id,
                to_source_id = %resolved_media.id,
                requested_audio_stream_index = ?requested_audio_stream_index,
                selected_audio_stream_index = ?audio_stream_index,
                preferred_audio_language = ?preferred_audio_language,
                "remapped audio track after Auto source changed"
            );
        }
        let subtitle_stream_index = q
            .subtitle_stream_index
            .map(|value| value as i32)
            .filter(|index| has_stream(*index, api::MediaStreamType::Subtitle));
        let burn_subtitle =
            q.subtitle_method == Some(api::SubtitleDeliveryMethod::Encode);
        let session_video_bitrate = if video_codec == "copy" {
            None
        } else {
            resolved_media
                .probe_data
                .as_ref()
                .and_then(|p| p.video_bitrate())
                .map(|b| {
                    let source = b as u32;
                    let target = q
                        .video_bit_rate
                        .map_or(source, |v| source.min(v as u32));
                    q.max_streaming_bitrate
                        .map_or(target, |c| target.min(c as u32))
                })
        };
        let session_hw_accel = if video_codec == "copy" {
            None
        } else {
            match encoding_opts_hls
                .hardware_acceleration_type
                .unwrap_or_default()
            {
                HardwareAccelerationType::None => None,
                hw => Some(hw.to_string()),
            }
        };
        let session = TranscodeSession::new(
            play_session_id.clone(),
            id,
            resolved_media.id,
            input_url.clone(),
            output_dir,
            video_codec.clone(),
            audio_codec.clone(),
            audio_stream_index,
            subtitle_stream_index,
            burn_subtitle,
            segment_length,
            // Parse reasons from query param (set by playbackinfo on the transcoding URL)
            q.transcode_reasons
                .as_deref()
                .map(api::TranscodeReasons::from_query_value)
                .unwrap_or_default(),
            runtime_ticks,
            is_live,
            source_video_codec,
            source_audio_codec,
            source_video_profile,
            source_video_level,
            source_video_range_type,
            source_video_width,
            source_video_height,
            source_frame_rate,
            session_video_bitrate,
            session_hw_accel,
        );

        // Record the requested origin before publishing the session. This lets
        // a duplicate master request distinguish itself from a real seek even
        // if FFmpeg has not started yet.
        session
            .write()
            .await
            .start_time_secs = requested_start_secs.min(u32::MAX as u64) as u32;

        state
            .ctx
            .sessions
            .attach_transcode(&play_session_id, session.clone());

        // Start transcoding in background
        let session_clone = session.clone();
        let encoding_opts = encoding_opts_hls.clone();
        let params = crate::playback::engine::TranscodeParams {
            input_url,
            output_dir: session
                .read()
                .await
                .output_dir
                .clone(),
            video_codec: video_codec.clone(),
            audio_codec: audio_codec.clone(),
            segment_length,
            start_time_ticks: q.start_time_ticks,
            max_width: q
                .max_width
                .map(|v| v as u32),
            max_height: q
                .max_height
                .map(|v| v as u32),
            video_bitrate: resolved_media
                .probe_data
                .as_ref()
                .and_then(|p| p.video_bitrate())
                .map(|b| {
                    let source = b as u32;
                    let target = q
                        .video_bit_rate
                        .map_or(source, |v| source.min(v as u32));
                    q.max_streaming_bitrate
                        .map_or(target, |c| target.min(c as u32))
                }),
            audio_bitrate: q
                .audio_bit_rate
                .map(|v| v as u32),
            // Force stereo downmix when transcoding audio — multi-channel AAC
            // (e.g. 6.1 from DTS-HD) causes MEDIA_ERR_SRC_NOT_SUPPORTED on most
            // browsers and iOS Safari.
            audio_channels: if audio_codec == "copy" { None } else { Some(2) },
            audio_stream_index,
            subtitle_stream_index,
            burn_subtitle,
            subtitle_width: None,
            subtitle_height: None,
            encoding_preset: encoding_opts.encoding_preset,
            source_video_codec: session
                .read()
                .await
                .source_video_codec
                .clone(),
            source_audio_codec: session
                .read()
                .await
                .source_audio_codec
                .clone(),
            accelerator: hw_accel::from_encoding_opts(&encoding_opts),
            source_video_range_type,
            enable_tonemapping: encoding_opts
                .enable_tonemapping
                .unwrap_or(false),
            enable_vpp_tonemapping: encoding_opts
                .enable_vpp_tonemapping
                .unwrap_or(false),
            tonemapping_algorithm: encoding_opts
                .tonemapping_algorithm
                .unwrap_or_else(|| "hable".to_string()),
            tonemapping_desat: encoding_opts
                .tonemapping_desat
                .unwrap_or(0.0),
            tonemapping_peak: encoding_opts
                .tonemapping_peak
                .unwrap_or(0.0),
            allow_hevc_encoding: encoding_opts
                .allow_hevc_encoding
                .unwrap_or(false),
            allow_av1_encoding: encoding_opts
                .allow_av1_encoding
                .unwrap_or(false),
            h264_crf: encoding_opts
                .h264_crf
                .unwrap_or(23),
            h265_crf: encoding_opts
                .h265_crf
                .unwrap_or(28),
            is_live,
            normalize_audio_loudness: encoding_opts
                .normalize_audio_loudness
                .unwrap_or(false),
        };

        state
            .ctx
            .sessions
            .mark_transcode_started(&play_session_id);
        // Spawn the transcode task with proper error handling
        let media_title_for_log = resolved_media
            .title
            .clone();
        let transcode_reasons_for_log = q
            .transcode_reasons
            .clone();
        let log_user = auth
            .user
            .username
            .clone();
        let log_client = auth
            .device
            .app_name
            .clone();
        let play_session_id_for_log = play_session_id.clone();
        let video_transcoding_enabled = encoding_opts
            .enable_video_transcoding
            .unwrap_or(true);
        let session_clone = session.clone();
        tokio::spawn(async move {
            let play_session_id = play_session_id_for_log;
            let start_secs = params
                .start_time_ticks
                .unwrap_or(0)
                / 10_000_000;
            let resolution = match (params.max_width, params.max_height) {
                (Some(w), Some(h)) => format!("{}x{}", w, h),
                (Some(w), None) => format!("{}w", w),
                (None, Some(h)) => format!("{}h", h),
                _ => "native".to_string(),
            };
            info!(
                play_session_id = %play_session_id,
                title = %media_title_for_log,
                user = %log_user,
                client = %log_client,
                video_codec = %params.video_codec,
                audio_codec = %params.audio_codec,
                resolution,
                video_bitrate = ?params.video_bitrate,
                hw_accel = ?params.accelerator.as_type(),
                transcode_reasons = ?transcode_reasons_for_log,
                start_secs,
                "Transcode started"
            );
            if let Err(e) =
                crate::playback::engine::start_transcode(session_clone, params).await
            {
                error!("Transcode failed: {:#}", e);
            }
        });

        session
    };

    Ok((session, play_session_id))
}

#[get("/videos/{id}/master.m3u8")]
pub async fn master_hls_video(
    State(state): State<AppState>,
    auth: auth::AuthSession,
    Path(id): Path<Uuid>,
    Query(q): Query<api::HlsVideoQuery>,
) -> Result<impl IntoResponse> {
    debug!("master_hls_video: item_id={}, q={:?}", id, q);
    let (session, _) = match create_hls_session(&state, &auth, id, &q).await {
        Ok(s) => s,
        Err(error) => {
            let error_text = format!("{error:?}");
            if let Some(play_session_id) = q
                .play_session_id
                .as_deref()
            {
                record_hls_startup_failure(&state, play_session_id, id, &error_text)
                    .await;
            }
            return Ok(axum::response::Redirect::temporary("/videos/no-streams")
                .into_response());
        }
    };
    let session_read = session
        .read()
        .await;
    let master_playlist =
        crate::playback::engine::generate_master_playlist(&session_read);
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/vnd.apple.mpegurl")
        .header("Cache-Control", "no-cache, no-store")
        .body(Body::from(master_playlist))
        .unwrap())
}

/// Safari/iOS live TV endpoint: creates the transcode session and returns the
/// variant playlist directly so the player gets segment URLs without a
/// master→variant redirect.
#[get("/videos/{id}/live.m3u8")]
pub async fn live_hls_video(
    State(state): State<AppState>,
    auth: auth::AuthSession,
    Path(id): Path<Uuid>,
    Query(mut q): Query<api::HlsVideoQuery>,
) -> Result<impl IntoResponse> {
    debug!("live_hls_video: item_id={}, q={:?}", id, q);
    let (_, play_session_id) = create_hls_session(&state, &auth, id, &q).await?;
    q.play_session_id = Some(play_session_id);
    variant_hls_video_inner(state, q).await
}

/// Variant HLS playlist - alternate URL used by some clients.
#[get("/videos/{id}/main.m3u8")]
pub async fn variant_hls_video_alt(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Query(q): Query<api::HlsVideoQuery>,
) -> Result<impl IntoResponse> {
    variant_hls_video_inner(state, q).await
}

/// Serves the variant (child) HLS playlist generated by the transcoding engine.
#[get("/videos/{id}/main/stream.m3u8")]
pub async fn variant_hls_video(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Query(q): Query<api::HlsVideoQuery>,
) -> Result<impl IntoResponse> {
    variant_hls_video_inner(state, q).await
}

/// Returns the audio codec to use for HLS transcoding.
///
/// Live IPTV sources often carry LATM-encoded AAC (see the large comment block
/// inside `create_hls_session`). When the client has negotiated `copy` for a
/// live channel we override it to `aac` so ffmpeg re-encodes to standard ADTS
/// stereo, which MSE-based players (Safari + hls.js) can actually decode.
fn resolve_live_audio_codec(is_live: bool, requested: &str) -> String {
    if is_live && requested == "copy" {
        "aac".to_string()
    } else {
        requested.to_string()
    }
}

fn resolve_source_audio_codec(requested: &str, source_codec: Option<&str>) -> String {
    if requested != "copy" {
        return requested.to_string();
    }
    match source_codec
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("aac" | "mp2" | "mp3") => "copy".to_string(),
        Some(_) => "aac".to_string(),
        // A newly selected torrent may not have probe metadata yet. Copying an
        // unknown codec into MPEG-TS is unsafe for WebOS/browser decoders; AAC
        // stereo is the compatibility-safe default and is cheap to encode.
        None => "aac".to_string(),
    }
}

fn audio_language_for_index(
    source: Option<&api::MediaSourceInfo>,
    index: Option<i32>,
) -> Option<String> {
    let index = i64::from(index?);
    source?
        .media_streams
        .iter()
        .find(|stream| {
            stream.index == index
                && matches!(stream.type_, Some(api::MediaStreamType::Audio))
        })
        .and_then(|stream| {
            stream
                .language
                .as_deref()
        })
        .and_then(lang_to_two_letter)
}

fn select_source_audio_stream<'a>(
    source: &'a api::MediaSourceInfo,
    requested_index: Option<i32>,
    preferred_language: Option<&str>,
    source_changed: bool,
) -> Option<&'a api::MediaStream> {
    if !source_changed {
        if let Some(index) = requested_index {
            if let Some(stream) = source
                .media_streams
                .iter()
                .find(|stream| {
                    stream.index == i64::from(index)
                        && matches!(stream.type_, Some(api::MediaStreamType::Audio))
                })
            {
                return Some(stream);
            }
        }
    }

    if let Some(target) = preferred_language.and_then(lang_to_two_letter) {
        if let Some(stream) = source
            .media_streams
            .iter()
            .find(|stream| {
                matches!(stream.type_, Some(api::MediaStreamType::Audio))
                    && stream
                        .language
                        .as_deref()
                        .and_then(lang_to_two_letter)
                        .as_deref()
                        == Some(target.as_str())
            })
        {
            return Some(stream);
        }
    }

    source.audio_stream()
}

fn should_serve_ffmpeg_variant_playlist(
    is_live: bool,
    _use_fmp4: bool,
    _start_time_secs: u32,
) -> bool {
    is_live
}

async fn variant_hls_video_inner(
    state: AppState,
    q: api::HlsVideoQuery,
) -> Result<impl IntoResponse> {
    let play_session_id = q
        .play_session_id
        .context_not_found("PlaySessionId is required")?;

    let session = state
        .ctx
        .sessions
        .get_transcode(&play_session_id)
        .context_not_found("transcode session not found")?;

    // Keep the session alive.
    state
        .ctx
        .sessions
        .ping(&play_session_id);

    let session_read = session
        .read()
        .await;
    let is_live = session_read.is_live;
    let use_fmp4 = session_read.use_fmp4();
    let playlist_path = session_read.variant_playlist_path();
    let psid = session_read
        .id
        .clone();

    // For live streams, fMP4 sessions, and resumed TS-HLS sessions we must
    // serve the ffmpeg-written playlist:
    // - live streams need the rolling EVENT playlist
    // - fMP4 segments snap to keyframe boundaries, so actual durations differ
    //   from the target and the playlist must reflect the real segment timing
    // - resumed TS-HLS sessions start ffmpeg at a non-zero segment number; a
    //   synthetic zero-based playlist would point clients at segment_00000 even
    //   though ffmpeg is writing segment_{start_number}.ts
    if should_serve_ffmpeg_variant_playlist(
        is_live,
        use_fmp4,
        session_read.start_time_secs,
    ) {
        drop(session_read);
        // For live streams, serve the ffmpeg-written EVENT playlist directly.
        // For fMP4 VOD, also use ffmpeg's playlist because fMP4 segments snap to
        // keyframe boundaries so actual durations differ from our target.
        // For resumed TS-HLS sessions, ffmpeg's playlist carries the correct
        // non-zero MEDIA-SEQUENCE and segment filenames after -start_number.
        // Poll until ffmpeg has written at least the first segment entry.
        let content = tokio::time::timeout(std::time::Duration::from_secs(15), async {
            loop {
                if let Ok(text) = tokio::fs::read_to_string(&playlist_path).await {
                    if text.contains("#EXTINF") {
                        return text;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
        })
        .await
        .unwrap_or_default();

        // For non-live VOD sessions: once ffmpeg finishes it appends
        // #EXT-X-ENDLIST and the playlist type stays as EVENT. Upgrade
        // EVENT→VOD so hls.js treats the stream as a completed VOD rather than
        // a live feed; leave live streams untouched.
        let is_complete = !is_live && content.contains("#EXT-X-ENDLIST");

        // Inject ?PlaySessionId=... into segment/map lines so hls_segment_inner can find the session.
        let content = content
            .lines()
            .map(|line| {
                if !line.starts_with('#')
                    && (line.ends_with(".ts") || line.ends_with(".m4s"))
                {
                    format!("{}?PlaySessionId={}", line, psid)
                } else if line.starts_with("#EXT-X-MAP:")
                    && !line.contains("PlaySessionId")
                {
                    // Inject PlaySessionId into the fMP4 init segment URI.
                    // e.g. #EXT-X-MAP:URI="init.mp4" → #EXT-X-MAP:URI="init.mp4?PlaySessionId=…"
                    line.replace(
                        "\"init.mp4\"",
                        &format!("\"init.mp4?PlaySessionId={}\"", psid),
                    )
                } else if is_complete && line == "#EXT-X-PLAYLIST-TYPE:EVENT" {
                    "#EXT-X-PLAYLIST-TYPE:VOD".to_string()
                } else {
                    line.to_string()
                }
            })
            .collect::<Vec<_>>()
            .join("\n");

        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/vnd.apple.mpegurl")
            .header("Cache-Control", "no-cache, no-store")
            .body(Body::from(content))
            .unwrap());
    }

    debug!(
        runtime_ticks = session_read.runtime_ticks,
        segment_length = session_read.segment_length,
        play_session_id = %play_session_id,
        "Generating VOD variant playlist"
    );
    let content = crate::playback::engine::generate_variant_playlist(
        &session_read,
        "", // no extra query string needed
    );

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/vnd.apple.mpegurl")
        .header("Cache-Control", "no-cache, no-store")
        .body(Body::from(content))
        .unwrap())
}

/// Serves individual HLS segment files.
/// Captures the full segment filename (e.g. "segment_00001.ts") and strips the extension.
#[get("/videos/{id}/main/{segment_file}")]
pub async fn hls_segment(
    State(state): State<AppState>,
    Path((id, segment_file)): Path<(Uuid, String)>,
    Query(q): Query<api::HlsVideoQuery>,
) -> Result<impl IntoResponse> {
    let segment_id = strip_segment_extension(&segment_file);
    hls_segment_inner(state, segment_id, q).await
}

/// Segment route at the same level as main.m3u8 — browsers resolve bare
/// segment filenames relative to the variant playlist URL.
#[get("/videos/{id}/{segment_file}")]
pub async fn hls_segment_flat(
    State(state): State<AppState>,
    Path((id, segment_file)): Path<(Uuid, String)>,
    Query(q): Query<api::HlsVideoQuery>,
) -> Result<impl IntoResponse> {
    let segment_id = strip_segment_extension(&segment_file);
    hls_segment_inner(state, segment_id, q).await
}

/// Jellyfin-compatible HLS segment route: /Videos/{id}/hls1/{playlistId}/{segmentFile}
#[get("/videos/{id}/hls1/{playlist_id}/{segment_file}")]
pub async fn hls1_segment(
    State(state): State<AppState>,
    Path((id, _playlist_id, segment_file)): Path<(Uuid, String, String)>,
    Query(q): Query<api::HlsVideoQuery>,
) -> Result<impl IntoResponse> {
    let segment_id = strip_segment_extension(&segment_file);
    hls_segment_inner(state, segment_id, q).await
}

fn strip_segment_extension(filename: &str) -> String {
    filename
        .rsplit_once('.')
        .map(|(name, _ext)| name.to_string())
        .unwrap_or_else(|| filename.to_string())
}

/// Find the highest segment index currently on disk in `dir`.
fn get_current_transcoding_index(dir: &std::path::Path) -> Option<u32> {
    let entries = std::fs::read_dir(dir).ok()?;
    let mut max_idx: Option<u32> = None;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        // Accept both MPEG-TS (.ts) and fMP4 (.m4s) segment files.
        if let Some(idx_str) = name
            .strip_suffix(".ts")
            .or_else(|| name.strip_suffix(".m4s"))
            .and_then(|s| {
                s.rsplit('_')
                    .next()
            })
        {
            if let Ok(idx) = idx_str.parse::<u32>() {
                max_idx = Some(max_idx.map_or(idx, |m: u32| m.max(idx)));
            }
        }
    }
    max_idx
}

/// Serve a complete on-disk file with a `Content-Length` header.
///
/// HLS init/fragments are always fetched whole by players, so byte-range
/// handling isn't needed — but the length header lets the browser size the
/// response up front instead of reading a chunked stream.
async fn serve_file_with_length(
    path: &std::path::Path,
    content_type: &str,
) -> Result<Response<Body>> {
    let file = tokio::fs::File::open(path).await?;
    let file_size = file
        .metadata()
        .await?
        .len();
    let body = Body::from_stream(ReaderStream::new(file));
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", content_type)
        .header("Content-Length", file_size)
        .header("Cache-Control", "public, max-age=86400")
        .body(body)
        .unwrap())
}

/// Poll until `path` exists with a non-zero size and stays unchanged across
/// consecutive polls.
///
/// ffmpeg creates HLS init/segment files *before* flushing their content, so
/// checking existence alone can serve a 0-byte init segment to the player.
/// MSE and AVFoundation cannot initialize from an empty fragment, so require a
/// stable non-zero size before serving it. Returns true once the file is ready
/// (or already was).
async fn wait_for_file_ready(
    path: &std::path::Path,
    timeout: std::time::Duration,
) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut last_size: Option<u64> = None;
    let mut stable = 0u32;
    while tokio::time::Instant::now() < deadline {
        match tokio::fs::metadata(path).await {
            Ok(meta) if meta.len() > 0 => {
                if Some(meta.len()) == last_size {
                    stable += 1;
                    if stable >= 2 {
                        return true;
                    }
                } else {
                    stable = 0;
                }
                last_size = Some(meta.len());
            }
            _ => {
                last_size = None;
                stable = 0;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    // Last chance: file exists and is non-empty right now.
    matches!(tokio::fs::metadata(path).await, Ok(m) if m.len() > 0)
}

fn slow_auto_observation_window(segment_length: u32) -> std::time::Duration {
    std::time::Duration::from_secs(
        u64::from(segment_length)
            .saturating_mul(3)
            .clamp(12, 24),
    )
}

fn slow_auto_startup_ratio(
    elapsed: std::time::Duration,
    segment_length: u32,
    current_idx: Option<u32>,
    requested_idx: u32,
) -> Option<f64> {
    if segment_length == 0
        || current_idx.is_some_and(|current| requested_idx <= current)
        || elapsed > std::time::Duration::from_secs(2 * 60)
    {
        return None;
    }

    if elapsed < slow_auto_observation_window(segment_length) {
        return None;
    }

    let produced_secs = current_idx
        .map(|current| u64::from(current.saturating_add(1)) * u64::from(segment_length))
        .unwrap_or(0);
    let production_ratio = produced_secs as f64
        / elapsed
            .as_secs_f64()
            .max(1.0);
    (production_ratio < 0.75).then_some(production_ratio)
}

fn transcoded_segment_age(
    output_dir: &std::path::Path,
    segment_index: u32,
) -> Option<std::time::Duration> {
    ["ts", "m4s"]
        .into_iter()
        .find_map(|extension| {
            std::fs::metadata(
                output_dir.join(format!("segment_{segment_index:05}.{extension}")),
            )
            .ok()
        })
        .and_then(|metadata| {
            metadata
                .modified()
                .ok()
        })
        .and_then(|modified| {
            modified
                .elapsed()
                .ok()
        })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AutoSourceSwitch {
    NotNeeded,
    Switched,
    Exhausted,
}

async fn maybe_switch_slow_auto_source(
    state: &AppState,
    session: &Arc<tokio::sync::RwLock<TranscodeSession>>,
    play_session_id: &str,
    requested_idx: u32,
) -> AutoSourceSwitch {
    let (
        item_id,
        old_source_id,
        output_dir,
        segment_length,
        elapsed,
        selected_audio_index,
    ) = {
        let session = session
            .read()
            .await;
        (
            session.item_id,
            session.media_source_id,
            session
                .output_dir
                .clone(),
            session.segment_length,
            session
                .created_at
                .elapsed(),
            session.audio_stream_index,
        )
    };
    let current_idx = get_current_transcoding_index(&output_dir);
    if current_idx
        .and_then(|current| transcoded_segment_age(&output_dir, current))
        .is_some_and(|age| {
            age < std::time::Duration::from_secs(u64::from(segment_length).max(1))
        })
    {
        return AutoSourceSwitch::NotNeeded;
    }
    let Some(production_ratio) =
        slow_auto_startup_ratio(elapsed, segment_length, current_idx, requested_idx)
    else {
        return AutoSourceSwitch::NotNeeded;
    };

    let Some(choice) = StreamService::auto_session_choice(
        &state
            .ctx
            .store,
        play_session_id,
    ) else {
        return AutoSourceSwitch::NotNeeded;
    };
    if choice.item_id != item_id || choice.winner_id != old_source_id {
        return AutoSourceSwitch::NotNeeded;
    }
    if !StreamService::claim_auto_failover_attempt(
        &state
            .ctx
            .store,
        play_session_id,
        old_source_id,
    ) {
        return AutoSourceSwitch::NotNeeded;
    }
    if !StreamService::claim_auto_failover_slot(
        &state
            .ctx
            .store,
        play_session_id,
    ) {
        warn!(
            %play_session_id,
            %old_source_id,
            "Auto startup fallback budget exhausted"
        );
        StreamService::clear_auto_session_winner(
            &state
                .ctx
                .store,
            play_session_id,
        );
        if let Some(report) = state
            .ctx
            .sessions
            .fail_startup(play_session_id, "Auto startup fallback budget exhausted")
        {
            if let Err(error) = db::record_playback_startup(
                &state
                    .ctx
                    .db,
                &report,
            )
            .await
            {
                warn!(
                    %error,
                    %play_session_id,
                    "failed to persist exhausted playback startup metric"
                );
            }
        }
        state
            .ctx
            .sessions
            .stop_transcode(play_session_id)
            .await;
        state
            .ctx
            .torrent
            .release_playback(play_session_id)
            .await;
        return AutoSourceSwitch::Exhausted;
    }

    let old_source = match db::Media::get_by_id(
        &state
            .ctx
            .db,
        &old_source_id,
    )
    .await
    {
        Ok(Some(source)) => source,
        Ok(None) => return AutoSourceSwitch::NotNeeded,
        Err(error) => {
            warn!(
                %play_session_id,
                %old_source_id,
                %error,
                "failed to inspect slow Auto source"
            );
            return AutoSourceSwitch::NotNeeded;
        }
    };
    if !old_source
        .stream_info
        .as_ref()
        .is_some_and(|info| info.is_p2p())
    {
        return AutoSourceSwitch::NotNeeded;
    }
    let parent_original_language = db::Media::get_by_id(
        &state
            .ctx
            .db,
        &item_id,
    )
    .await
    .ok()
    .flatten()
    .and_then(|item| item.original_language);
    let preferred_audio_language = audio_language_for_index(
        old_source
            .probe_data
            .as_ref(),
        selected_audio_index,
    )
    .or_else(|| {
        old_source
            .probe_data
            .as_ref()
            .and_then(|probe| probe.audio_stream())
            .and_then(|stream| {
                stream
                    .language
                    .as_deref()
            })
            .and_then(lang_to_two_letter)
    })
    // If the old source was never probed, the title's original language is
    // still a better semantic key than carrying a numeric stream index to an
    // unrelated release.
    .or_else(|| {
        parent_original_language
            .as_deref()
            .and_then(lang_to_two_letter)
    });

    warn!(
        %play_session_id,
        %old_source_id,
        title = %old_source.title,
        requested_idx,
        ?current_idx,
        elapsed_secs = elapsed.as_secs_f64(),
        production_ratio,
        "Auto source cannot sustain startup; selecting a fallback"
    );
    StreamService::mark_auto_candidate_failed(
        &state
            .ctx
            .store,
        choice.item_id,
        &choice.resolution,
        &old_source,
    );

    let mut warm_backup = None;
    for backup_id in choice
        .fallback_ids
        .iter()
        .copied()
        .filter(|backup_id| *backup_id != old_source_id)
    {
        match db::Media::get_by_id(
            &state
                .ctx
                .db,
            &backup_id,
        )
        .await
        {
            Ok(Some(backup))
                if backup
                    .stream_info
                    .as_ref()
                    .is_some_and(|info| info.is_p2p())
                    && !StreamService::is_auto_candidate_failed(
                        &state
                            .ctx
                            .store,
                        choice.item_id,
                        &choice.resolution,
                        &backup,
                    ) =>
            {
                warm_backup = Some(backup);
                break;
            }
            Ok(_) => {}
            Err(error) => {
                warn!(
                    %play_session_id,
                    %backup_id,
                    %error,
                    "failed to load Auto warm fallback"
                );
            }
        }
    }
    let (replacement, used_warm_backup) = if let Some(replacement) = warm_backup {
        info!(
            %play_session_id,
            replacement_id = %replacement.id,
            replacement_title = %replacement.title,
            "using measured Auto warm fallback"
        );
        (replacement, true)
    } else {
        match StreamService::resolve_auto(
            &state.ctx,
            choice.item_id,
            &choice.resolution,
            choice.user_id,
        )
        .await
        {
            Ok(replacement) => (replacement, false),
            Err(error) => {
                warn!(
                    %play_session_id,
                    %old_source_id,
                    %error,
                    "Auto fallback selection failed"
                );
                return AutoSourceSwitch::NotNeeded;
            }
        }
    };
    let session_still_attached = state
        .ctx
        .sessions
        .get_transcode(play_session_id)
        .is_some_and(|attached| Arc::ptr_eq(&attached, session));
    if !session_still_attached {
        return AutoSourceSwitch::NotNeeded;
    }
    let (source_unchanged, latest_elapsed) = {
        let transcode = session
            .read()
            .await;
        (
            transcode.media_source_id == old_source_id,
            transcode
                .created_at
                .elapsed(),
        )
    };
    if !source_unchanged {
        return AutoSourceSwitch::NotNeeded;
    }
    let latest_idx = get_current_transcoding_index(&output_dir);
    if slow_auto_startup_ratio(
        latest_elapsed,
        segment_length,
        latest_idx,
        requested_idx,
    )
    .is_none()
    {
        StreamService::clear_auto_candidate_failed(
            &state
                .ctx
                .store,
            choice.item_id,
            &choice.resolution,
            &old_source,
        );
        info!(
            %play_session_id,
            %old_source_id,
            ?latest_idx,
            elapsed_secs = latest_elapsed.as_secs_f64(),
            "Auto source recovered while fallback was being evaluated"
        );
        return AutoSourceSwitch::NotNeeded;
    }
    if replacement.id == old_source_id {
        warn!(
            %play_session_id,
            %old_source_id,
            "Auto fallback had no alternate candidate"
        );
        return AutoSourceSwitch::NotNeeded;
    }

    let Some(stream_info) = replacement
        .stream_info
        .as_ref()
    else {
        warn!(
            %play_session_id,
            replacement_id = %replacement.id,
            "Auto fallback has no stream URL"
        );
        return AutoSourceSwitch::NotNeeded;
    };
    let input_url = stream_info
        .descriptor
        .server_input(
            replacement.id,
            state
                .ctx
                .config
                .port,
        );
    let source_video_stream = replacement
        .probe_data
        .as_ref()
        .and_then(|probe| probe.video_stream());
    let source_audio_stream = replacement
        .probe_data
        .as_ref()
        .and_then(|probe| {
            select_source_audio_stream(
                probe,
                selected_audio_index,
                preferred_audio_language.as_deref(),
                true,
            )
        });
    let replacement_audio_codec = source_audio_stream.and_then(|stream| {
        stream
            .codec
            .clone()
    });

    let mut transcode = session
        .write()
        .await;
    if transcode.media_source_id != old_source_id {
        return AutoSourceSwitch::NotNeeded;
    }
    let requested_audio_index = transcode.audio_stream_index;
    let requested_subtitle_index = transcode.subtitle_stream_index;
    let replacement_has_subtitle_index = |index: i32| {
        replacement
            .probe_data
            .as_ref()
            .is_some_and(|probe| {
                probe
                    .media_streams
                    .iter()
                    .any(|stream| {
                        stream.index == i64::from(index)
                            && matches!(
                                stream.type_,
                                Some(api::MediaStreamType::Subtitle)
                            )
                    })
            })
    };

    transcode.media_source_id = replacement.id;
    transcode.input_url = input_url;
    transcode.created_at = std::time::Instant::now();
    transcode.source_video_codec = source_video_stream.and_then(|stream| {
        stream
            .codec
            .clone()
    });
    transcode.source_video_profile = source_video_stream.and_then(|stream| {
        stream
            .profile
            .clone()
    });
    transcode.source_video_level = source_video_stream.and_then(|stream| stream.level);
    transcode.source_video_range_type =
        source_video_stream.and_then(|stream| stream.video_range_type);
    transcode.source_video_width = source_video_stream.and_then(|stream| stream.width);
    transcode.source_video_height =
        source_video_stream.and_then(|stream| stream.height);
    transcode.source_frame_rate =
        source_video_stream.and_then(|stream| stream.real_frame_rate);
    transcode.audio_codec = resolve_source_audio_codec(
        &transcode.audio_codec,
        replacement_audio_codec.as_deref(),
    );
    transcode.source_audio_codec = replacement_audio_codec;
    transcode.audio_stream_index =
        source_audio_stream.and_then(|stream| i32::try_from(stream.index).ok());
    if requested_audio_index != transcode.audio_stream_index {
        info!(
            %play_session_id,
            from_source_id = %old_source_id,
            to_source_id = %replacement.id,
            requested_audio_stream_index = ?requested_audio_index,
            selected_audio_stream_index = ?transcode.audio_stream_index,
            preferred_audio_language = ?preferred_audio_language,
            "remapped audio track after Auto fallback source changed"
        );
    }
    transcode.subtitle_stream_index =
        requested_subtitle_index.filter(|index| replacement_has_subtitle_index(*index));
    if requested_subtitle_index.is_some()
        && transcode
            .subtitle_stream_index
            .is_none()
    {
        transcode.burn_subtitle = false;
    }
    drop(transcode);

    if used_warm_backup {
        StreamService::update_auto_session_choice(
            &state
                .ctx
                .store,
            play_session_id,
            replacement.id,
            choice
                .fallback_ids
                .iter()
                .copied()
                .filter(|candidate_id| {
                    *candidate_id != replacement.id && *candidate_id != old_source_id
                })
                .collect(),
        );
    } else {
        let auto_id = StreamService::auto_source_id(choice.item_id, &choice.resolution);
        StreamService::pin_auto_session_winner(
            &state
                .ctx
                .store,
            play_session_id,
            choice.item_id,
            auto_id,
            replacement.id,
            choice.user_id,
        );
    }
    state
        .ctx
        .sessions
        .mark_source_selected(play_session_id, replacement.id);
    warn!(
        %play_session_id,
        %old_source_id,
        replacement_id = %replacement.id,
        replacement_title = %replacement.title,
        "Auto source fallback selected"
    );
    AutoSourceSwitch::Switched
}

async fn hls_segment_inner(
    state: AppState,
    segment_id: String,
    q: api::HlsVideoQuery,
) -> Result<impl IntoResponse> {
    let play_session_id = q
        .play_session_id
        .context_not_found("PlaySessionId is required")?;

    trace!(
        segment_id = %segment_id,
        play_session_id = %play_session_id,
        runtime_ticks = ?q.runtime_ticks,
        "HLS segment request"
    );

    let session = state
        .ctx
        .sessions
        .get_transcode(&play_session_id);

    // The fMP4 init segment is served at "init.mp4" — strip_segment_extension
    // reduces that to "init", so we detect it here and serve it directly.
    if segment_id == "init" {
        let init_path = match &session {
            Some(s) => s
                .read()
                .await
                .init_segment_path(),
            None => state
                .ctx
                .sessions
                .segment_path(&play_session_id, "init.mp4")
                .with_extension("mp4"),
        };
        // Wait for ffmpeg to write the init segment. Merely checking existence
        // is not enough: ffmpeg creates init.mp4 before buffering its content
        // out, so serving it in that window yields an empty init segment that
        // breaks the browser's decoder. Wait until the file is non-empty and
        // stable.
        if session.is_some() {
            if !wait_for_file_ready(&init_path, std::time::Duration::from_secs(10))
                .await
            {
                None::<()>.context_not_found("fMP4 init segment not ready")?;
            }
        } else if !init_path.exists() {
            None::<()>.context_not_found("fMP4 init segment not ready")?;
        }
        state
            .ctx
            .sessions
            .ping(&play_session_id);
        return serve_file_with_length(&init_path, "video/mp4").await;
    }

    // Derive the segment path — either from the live session or from the base
    // dir directly (handles server restart where session is gone but files remain).
    let segment_path = match &session {
        Some(s) => s
            .read()
            .await
            .segment_path(&segment_id),
        None => state
            .ctx
            .sessions
            .segment_path(&play_session_id, &segment_id),
    };

    // Parse the requested segment index from the filename.
    let requested_idx: Option<u32> = segment_id
        .rsplit('_')
        .next()
        .and_then(|n| {
            n.parse::<u32>()
                .ok()
        });

    if let Some(ref session) = session {
        // Update playback position for the buffer monitor.
        if let Some(idx) = requested_idx {
            use std::sync::atomic::Ordering;
            let s = session
                .read()
                .await;
            let prev = s
                .last_segment_index
                .load(Ordering::Relaxed);
            if idx > prev {
                s.last_segment_index
                    .store(idx, Ordering::Relaxed);
            }
        }
    }

    // The first segment request normally arrives immediately after FFmpeg starts.
    // Waiting for the full 60-second segment timeout means the slow-source check
    // never runs again unless the client gives up and retries. For Auto playback,
    // stage the wait at the observation boundary so this same request can switch
    // sources server-side as soon as startup is demonstrably too slow.
    if !segment_path.exists() {
        if let (Some(session), Some(requested_idx)) = (&session, requested_idx) {
            let observation_wait = {
                let transcode = session
                    .read()
                    .await;
                let choice = StreamService::auto_session_choice(
                    &state
                        .ctx
                        .store,
                    &play_session_id,
                );
                let current_idx = get_current_transcoding_index(&transcode.output_dir);
                choice
                    .filter(|choice| {
                        choice.item_id == transcode.item_id
                            && choice.winner_id == transcode.media_source_id
                            && current_idx
                                .map_or(true, |current| requested_idx > current)
                    })
                    .and_then(|_| {
                        slow_auto_observation_window(transcode.segment_length)
                            .checked_sub(
                                transcode
                                    .created_at
                                    .elapsed(),
                            )
                    })
                    .filter(|remaining| !remaining.is_zero())
            };
            if let Some(observation_wait) = observation_wait {
                let _ = wait_for_file_ready(&segment_path, observation_wait).await;
            }
        }
    }

    // If the segment doesn't exist and we have a live session, check whether
    // FFmpeg needs to be restarted at a different position (like Jellyfin does).
    if !segment_path.exists() {
        if let (Some(session), Some(requested_idx)) = (&session, requested_idx) {
            let failed_source_id = session
                .read()
                .await
                .media_source_id;
            let source_switch = maybe_switch_slow_auto_source(
                &state,
                session,
                &play_session_id,
                requested_idx,
            )
            .await;
            if source_switch == AutoSourceSwitch::Exhausted {
                None::<()>.context_not_found(
                    "Auto startup exhausted its measured fallback",
                )?;
            }
            let force_restart = source_switch == AutoSourceSwitch::Switched;
            let s = session
                .read()
                .await;
            let output_dir = s
                .output_dir
                .clone();
            let segment_length = s.segment_length;
            let current_idx = get_current_transcoding_index(&output_dir);
            let segment_gap_threshold = 24 / segment_length;

            let needs_restart = force_restart
                || match current_idx {
                    None => {
                        // No segments on disk yet. If FFmpeg is still running
                        // (Starting/Running), just fall through to the wait loop —
                        // killing it here causes an infinite restart cycle.
                        matches!(
                            s.state,
                            TranscodeState::Error(_) | TranscodeState::Complete
                        )
                    }
                    Some(cur) if requested_idx < cur => true, // seeking backward
                    Some(cur)
                        if requested_idx.saturating_sub(cur)
                            > segment_gap_threshold =>
                    {
                        true
                    } // too far ahead
                    _ => false, // within range — just wait for FFmpeg
                };

            if needs_restart {
                // Guard against concurrent restart: only proceed if FFmpeg
                // is actually running (kill_tx is Some). If another request
                // already killed it and started a new one, just wait.
                let has_running_ffmpeg = s
                    .kill_tx
                    .is_some();
                if !has_running_ffmpeg && !force_restart {
                    drop(s);
                    // Another request already restarted — fall through to wait loop.
                } else {
                    debug!(
                        requested_idx,
                        ?current_idx,
                        segment_gap_threshold,
                        force_restart,
                        "Segment-driven transcode restart"
                    );

                    // Gather params we need before dropping the read lock.
                    let input_url = s
                        .input_url
                        .clone();
                    let video_codec = s
                        .video_codec
                        .clone();
                    let audio_codec = s
                        .audio_codec
                        .clone();
                    let audio_stream_index = s.audio_stream_index;
                    let subtitle_stream_index = s.subtitle_stream_index;
                    let burn_subtitle = s.burn_subtitle;
                    drop(s);

                    // Stop the rejected torrent before waiting for FFmpeg to
                    // exit. The old HTTP input guard is still alive until that
                    // process dies, so the normal idle-release path would let a
                    // runaway source keep consuming bandwidth and disk here.
                    let failed_torrent_id = if force_restart {
                        state
                            .ctx
                            .torrent
                            .pause_playback_for_switch(&play_session_id)
                            .await
                    } else {
                        None
                    };

                    // Kill running FFmpeg and clean up stale segments (params
                    // like bitrate/codec may change, so old segments are invalid).
                    {
                        let (kill_tx, wait_done) = {
                            let mut s = session
                                .write()
                                .await;
                            (
                                s.kill_tx
                                    .take(),
                                s.wait_done
                                    .clone(),
                            )
                        };
                        if let Some(kill_tx) = kill_tx {
                            let notification = wait_done.notified();
                            let _ = kill_tx.send(());
                            notification.await;
                        }
                    }
                    if let Some(failed_torrent_id) = failed_torrent_id {
                        match state
                            .ctx
                            .torrent
                            .discard_failed_torrent_if_unwatched(
                                &state
                                    .ctx
                                    .db,
                                failed_torrent_id,
                                failed_source_id,
                            )
                            .await
                        {
                            Ok(true) => {}
                            Ok(false) => debug!(
                                %play_session_id,
                                %failed_source_id,
                                "kept failed Auto torrent because it was watched or still active"
                            ),
                            Err(error) => warn!(
                                %play_session_id,
                                %failed_source_id,
                                %error,
                                "failed to discard unwatched Auto torrent"
                            ),
                        }
                    }
                    let _ = std::fs::remove_dir_all(&output_dir);
                    let _ = std::fs::create_dir_all(&output_dir);

                    // Calculate the seek position from the runtimeTicks query param
                    // (cumulative ticks to start of this segment) provided by our
                    // server-generated VOD playlist. Fall back to segment_index * segment_length.
                    let start_time_ticks = q
                        .runtime_ticks
                        .unwrap_or_else(|| {
                            (requested_idx as i64 * segment_length as i64)
                                .to_ticks(TickUnit::Seconds)
                                .unwrap_or(0)
                        });

                    let encoding_opts = crate::db::Settings::get_encoding_config(
                        &state
                            .ctx
                            .db,
                    )
                    .await
                    .unwrap_or_default();
                    let params = crate::playback::engine::TranscodeParams {
                        input_url,
                        output_dir: output_dir.clone(),
                        video_codec,
                        audio_codec: audio_codec.clone(),
                        segment_length,
                        start_time_ticks: Some(start_time_ticks),
                        max_width: q
                            .max_width
                            .map(|v| v as u32),
                        max_height: q
                            .max_height
                            .map(|v| v as u32),
                        video_bitrate: q
                            .video_bit_rate
                            .map(|v| v as u32),
                        audio_bitrate: q
                            .audio_bit_rate
                            .map(|v| v as u32),
                        audio_channels: if audio_codec == "copy" {
                            None
                        } else {
                            Some(2)
                        },
                        audio_stream_index,
                        subtitle_stream_index,
                        burn_subtitle,
                        subtitle_width: None,
                        subtitle_height: None,
                        encoding_preset: encoding_opts.encoding_preset,
                        source_video_codec: session
                            .read()
                            .await
                            .source_video_codec
                            .clone(),
                        source_audio_codec: session
                            .read()
                            .await
                            .source_audio_codec
                            .clone(),
                        accelerator: hw_accel::from_encoding_opts(&encoding_opts),
                        source_video_range_type: session
                            .read()
                            .await
                            .source_video_range_type,
                        enable_tonemapping: encoding_opts
                            .enable_tonemapping
                            .unwrap_or(false),
                        enable_vpp_tonemapping: encoding_opts
                            .enable_vpp_tonemapping
                            .unwrap_or(false),
                        tonemapping_algorithm: encoding_opts
                            .tonemapping_algorithm
                            .unwrap_or_else(|| "hable".to_string()),
                        tonemapping_desat: encoding_opts
                            .tonemapping_desat
                            .unwrap_or(0.0),
                        tonemapping_peak: encoding_opts
                            .tonemapping_peak
                            .unwrap_or(0.0),
                        allow_hevc_encoding: encoding_opts
                            .allow_hevc_encoding
                            .unwrap_or(false),
                        allow_av1_encoding: encoding_opts
                            .allow_av1_encoding
                            .unwrap_or(false),
                        h264_crf: encoding_opts
                            .h264_crf
                            .unwrap_or(23),
                        h265_crf: encoding_opts
                            .h265_crf
                            .unwrap_or(28),
                        is_live: false,
                        normalize_audio_loudness: encoding_opts
                            .normalize_audio_loudness
                            .unwrap_or(false),
                    };

                    // Reinitialise the session's state for the new transcode run.
                    {
                        let mut s = session
                            .write()
                            .await;
                        s.state = TranscodeState::Starting;
                        let _ = s
                            .state_tx
                            .send(TranscodeState::Starting);
                        s.start_time_secs = (start_time_ticks / 10_000_000) as u32;
                        s.playback_offset_secs
                            .store(
                                s.start_time_secs,
                                std::sync::atomic::Ordering::Relaxed,
                            );
                    }

                    let session_clone = session.clone();
                    tokio::spawn(async move {
                        if let Err(e) = crate::playback::engine::start_transcode(
                            session_clone,
                            params,
                        )
                        .await
                        {
                            error!("Transcode restart failed: {:#}", e);
                        }
                    });
                } // else: has_running_ffmpeg
            } // needs_restart
        }
    }

    // Wait up to 60s for ffmpeg to produce the segment (also waiting until it
    // is non-empty — same create-before-write race as the init segment).
    // If there's no live session (e.g. after server restart), only serve from disk.
    if session.is_some()
        && !wait_for_file_ready(&segment_path, std::time::Duration::from_secs(60)).await
    {
        // fall through to the not-found error below
    }

    if !segment_path.exists() {
        if session.is_none() {
            None::<()>.context_not_found(&format!(
                "transcode session {} gone and segment {} not on disk",
                play_session_id, segment_id
            ))?;
        }
        None::<()>.context_not_found(&format!(
            "segment {} not ready after timeout",
            segment_id
        ))?;
    }

    // Keep the session alive — the segment request counts as activity.
    state
        .ctx
        .sessions
        .ping(&play_session_id);
    state
        .ctx
        .sessions
        .mark_first_segment_served(&play_session_id);
    // fMP4 segments (.m4s) use video/mp4; MPEG-TS segments use video/mp2t.
    let content_type = if segment_path
        .extension()
        .and_then(|e| e.to_str())
        == Some("m4s")
    {
        "video/mp4"
    } else {
        "video/mp2t"
    };

    serve_file_with_length(&segment_path, content_type).await
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    fn startup_probe(
        goodput_mbps: f64,
        required_mbps: Option<f64>,
    ) -> super::StartupProbeResult {
        let bytes = super::HEDGE_PROBE_BYTES;
        let elapsed =
            Duration::from_secs_f64(bytes as f64 * 8.0 / (goodput_mbps * 1_000_000.0));
        super::StartupProbeResult {
            source_id: uuid::Uuid::nil(),
            title: "test".to_string(),
            range_start: 0,
            source_size_bytes: None,
            bytes,
            elapsed,
            warmup_elapsed: None,
            verified_live_sample: false,
            required_bitrate_bps: required_mbps.map(|value| value * 1_000_000.0),
            error: None,
        }
    }

    #[test]
    fn startup_status_phase_follows_server_milestones() {
        let mut progress = crate::playback_session::PlaybackStartupProgress {
            play_session_id: "test".to_string(),
            user_id: uuid::Uuid::nil(),
            elapsed: Duration::ZERO,
            playback_info_ready: false,
            hls_requested: false,
            source_selected: false,
            transcode_started: false,
            first_segment_served: false,
        };

        assert_eq!(super::startup_phase(&progress, false), "preparing_playback");
        progress.playback_info_ready = true;
        assert_eq!(super::startup_phase(&progress, false), "starting_player");
        progress.hls_requested = true;
        assert_eq!(super::startup_phase(&progress, false), "selecting_source");
        progress.source_selected = true;
        assert_eq!(super::startup_phase(&progress, false), "opening_stream");
        assert_eq!(super::startup_phase(&progress, true), "prebuffering");
        progress.transcode_started = true;
        assert_eq!(super::startup_phase(&progress, true), "preparing_video");
        progress.first_segment_served = true;
        assert_eq!(super::startup_phase(&progress, true), "starting_playback");
    }

    #[test]
    fn prebuffer_status_waits_for_a_trusted_rate_before_reporting_eta() {
        use std::sync::atomic::{AtomicBool, AtomicU64};

        let plan = super::AutoPrebufferPlan {
            source_id: uuid::Uuid::nil(),
            range_start: 0,
            source_size_bytes: 1_000_000,
            required_bitrate_bps: 8_000_000.0,
            remaining_duration_seconds: 1.0,
            next_offset: AtomicU64::new(100_000),
            estimated_goodput_bps: AtomicU64::new(4_000_000.0f64.to_bits()),
            rate_trusted: AtomicBool::new(false),
        };

        let measuring = super::PrebufferStatusResponse::from_plan(&plan);
        assert_eq!(measuring.downloaded_bytes, 100_000);
        assert!(
            measuring
                .estimated_wait_seconds
                .is_none()
        );

        plan.rate_trusted
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let trusted = super::PrebufferStatusResponse::from_plan(&plan);
        assert_eq!(trusted.target_bytes, 1_000_000);
        assert_eq!(trusted.estimated_wait_seconds, Some(2));
    }

    #[test]
    fn auto_prebuffer_stream_is_owned_by_the_playback_session() {
        let media_id =
            uuid::Uuid::parse_str("723fb86d-f8df-548b-b4b1-169f8ec3f475").unwrap();
        let url = url::Url::parse(&super::auto_prebuffer_url(
            3000,
            media_id,
            "play/session 1",
        ))
        .unwrap();

        assert_eq!(url.path(), "/stream/723fb86d-f8df-548b-b4b1-169f8ec3f475");
        assert!(
            url.query_pairs()
                .any(|(key, value)| {
                    key == "PlaySessionId" && value == "play/session 1"
                })
        );
    }

    #[test]
    fn startup_hedge_uses_release_bitrate_not_raw_speed_alone() {
        let primary = startup_probe(12.0, Some(12.0));
        let backup = startup_probe(8.0, Some(4.0));
        assert!(super::prefer_backup_probe(&primary, &backup));
    }

    #[test]
    fn startup_hedge_can_choose_a_staggered_third_probe() {
        let primary = startup_probe(2.0, Some(4.0));
        let backup = startup_probe(4.0, Some(4.0));
        let tertiary = startup_probe(12.0, Some(4.0));

        assert_eq!(
            super::preferred_probe_index(&[&primary, &backup, &tertiary]),
            2
        );
    }

    #[test]
    fn patient_probe_prefers_warm_throughput_over_peer_setup_time() {
        let mut slow_setup = startup_probe(1.0, Some(30.0));
        slow_setup.bytes = super::PATIENT_PROBE_BYTES;
        slow_setup.elapsed = Duration::from_secs(34);
        slow_setup.warmup_elapsed = Some(Duration::from_millis(33_370));

        let quick_but_slow = startup_probe(18.5, Some(39.2));

        assert!(slow_setup.goodput_bps() > 26_000_000.0);
        assert!(slow_setup.cold_goodput_bps() < 1_000_000.0);
        assert!(!super::prefer_backup_probe(&quick_but_slow, &slow_setup));
        assert!(super::prefer_patient_probe(&quick_but_slow, &slow_setup));
    }

    #[test]
    fn patient_probe_prebuffers_a_fast_burst_after_a_slow_cold_start() {
        let mut probe = startup_probe(1.0, Some(2.229));
        probe.bytes = super::PATIENT_PROBE_BYTES;
        probe.elapsed = Duration::from_millis(18_577);
        probe.warmup_elapsed = Some(Duration::from_millis(17_646));
        probe.source_size_bytes = Some(1_839_000_000);

        assert!(probe.goodput_bps() > 17_000_000.0);
        assert!(probe.is_decisive());
        assert!(
            probe
                .cold_headroom()
                .is_some_and(|headroom| headroom < 1.0)
        );
        assert!(!probe.can_start_without_prebuffer());
        assert!(super::AutoPrebufferPlan::from_probe(&probe).is_some());
    }

    #[test]
    fn startup_hedge_uses_item_duration_when_release_duration_is_missing() {
        let media = crate::db::Media {
            stream_info: Some(crate::stream::StreamInfo {
                size: Some(1_000_000_000),
                ..Default::default()
            }),
            ..Default::default()
        };

        let bitrate =
            super::required_source_bitrate_bps(&media, Some(1_000.0), None).unwrap();

        assert_eq!(bitrate, 8_000_000.0);
    }

    #[test]
    fn startup_hedge_uses_observed_selected_file_size() {
        let media = crate::db::Media::default();

        let bitrate = super::required_source_bitrate_bps(
            &media,
            Some(1_000.0),
            Some(1_000_000_000),
        )
        .unwrap();

        assert_eq!(bitrate, 8_000_000.0);
    }

    #[test]
    fn startup_hedge_does_not_mistake_a_piece_burst_for_throughput() {
        let mut burst = startup_probe(24.0, Some(24.0));
        burst.bytes = super::HEDGE_PROBE_BYTES;
        burst.elapsed = Duration::from_millis(5_050);
        burst.warmup_elapsed = Some(Duration::from_millis(5_045));

        assert_eq!(burst.goodput_bps(), burst.cold_goodput_bps());
        assert!(
            burst
                .cold_headroom()
                .is_some_and(|headroom| headroom < super::HEDGE_MIN_COLD_HEADROOM)
        );
        assert!(!burst.is_decisive());
        assert!(burst.is_unsustainable());
    }

    #[test]
    fn startup_hedge_rejects_a_short_burst_despite_cold_recovery_allowance() {
        let mut burst = startup_probe(18.0, Some(18.0));
        burst.bytes = super::HEDGE_PROBE_BYTES;
        burst.elapsed = Duration::from_millis(2_300);
        burst.warmup_elapsed = Some(Duration::from_millis(2_298));

        assert!(
            burst
                .cold_headroom()
                .is_some_and(|headroom| headroom >= super::HEDGE_MIN_COLD_HEADROOM)
        );
        assert_eq!(burst.goodput_bps(), burst.cold_goodput_bps());
        assert!(!burst.is_decisive());
        assert!(burst.is_unsustainable());
    }

    #[test]
    fn startup_hedge_requires_data_beyond_a_cached_prefix() {
        let mut cached_prefix = startup_probe(2_000.0, Some(10.0));
        cached_prefix.bytes = super::HEDGE_PREFIX_BYTES;
        cached_prefix.elapsed = Duration::from_millis(10);
        cached_prefix.warmup_elapsed = Some(Duration::from_millis(1));

        assert!(
            cached_prefix
                .headroom()
                .is_some_and(|headroom| headroom > 100.0)
        );
        assert!(!cached_prefix.is_decisive());
    }

    #[test]
    fn startup_hedge_cache_busts_an_instant_complete_probe() {
        let mut cached_probe = startup_probe(2_000.0, Some(10.0));
        cached_probe.elapsed = Duration::from_millis(10);
        cached_probe.warmup_elapsed = Some(Duration::from_millis(1));

        assert!(cached_probe.sample_is_inconclusive());
        assert!(cached_probe.is_unsustainable());
        assert!(!cached_probe.is_decisive());

        cached_probe.verified_live_sample = true;
        assert!(!cached_probe.sample_is_inconclusive());
        assert!(cached_probe.is_decisive());
    }

    #[test]
    fn cache_bust_probe_does_not_overlap_the_startup_range() {
        let source_size = 18_000_000_000;
        let startup_start = 0;
        let sample_start =
            super::cache_bust_probe_range_start(source_size, startup_start).unwrap();
        let sample_end = sample_start + super::HEDGE_STEADY_SAMPLE_BYTES;

        assert!(sample_end <= source_size);
        assert!(sample_start >= super::HEDGE_PROBE_BYTES);
    }

    #[test]
    fn resumed_startup_probe_targets_the_requested_time() {
        let media = crate::db::Media {
            stream_info: Some(crate::stream::StreamInfo {
                size: Some(10_000_000_000),
                ..Default::default()
            }),
            ..Default::default()
        };

        let range_start = super::startup_probe_range_start(
            &media,
            Some(1_000.0),
            Some(500 * 10_000_000),
        );

        assert_eq!(range_start, 5_000_000_000);
    }

    #[test]
    fn startup_probe_falls_back_to_the_prefix_without_timing_metadata() {
        let media = crate::db::Media {
            stream_info: Some(crate::stream::StreamInfo {
                size: Some(10_000_000_000),
                ..Default::default()
            }),
            ..Default::default()
        };

        assert_eq!(
            super::startup_probe_range_start(&media, None, Some(500 * 10_000_000),),
            0
        );
    }

    #[test]
    fn startup_hedge_requires_sustainable_headroom() {
        assert!(!startup_probe(13.4, Some(10.0)).is_strong());
        assert!(startup_probe(13.6, Some(10.0)).is_strong());
    }

    #[test]
    fn startup_hedge_separates_cold_peer_setup_from_warm_throughput() {
        let mut probe = startup_probe(1.0, Some(10.0));
        probe.bytes = super::HEDGE_PROBE_BYTES;
        probe.elapsed = Duration::from_millis(4_500);
        probe.warmup_elapsed = Some(Duration::from_millis(4_200));

        assert!(probe.cold_goodput_bps() < 5_000_000.0);
        assert!(probe.goodput_bps() > 13_500_000.0);
        assert!(probe.is_strong());
    }

    #[test]
    fn completed_probe_with_modest_headroom_is_viable() {
        let mut probe = startup_probe(11.6, Some(10.0));
        probe.bytes = super::HEDGE_PROBE_BYTES;
        probe.elapsed =
            Duration::from_secs_f64(probe.bytes as f64 * 8.0 / 11_600_000.0);
        assert!(probe.is_viable());
        assert!(probe.is_decisive());
    }

    #[test]
    fn startup_hedge_uses_buffer_runway_for_near_realtime_sources() {
        let mut slow = startup_probe(8.0, Some(10.0));
        slow.bytes = super::HEDGE_PROBE_BYTES;
        slow.elapsed = Duration::from_secs_f64(slow.bytes as f64 * 8.0 / 8_000_000.0);
        assert!(slow.is_unsustainable());

        let mut near_realtime = startup_probe(9.9, Some(10.0));
        near_realtime.bytes = super::HEDGE_PROBE_BYTES;
        near_realtime.elapsed =
            Duration::from_secs_f64(near_realtime.bytes as f64 * 8.0 / 9_900_000.0);
        assert!(
            near_realtime
                .deficit_runway()
                .is_some_and(|runway| runway > Duration::from_secs(60))
        );
        assert!(!near_realtime.is_unsustainable());
    }

    #[test]
    fn auto_prebuffer_covers_the_remaining_throughput_deficit() {
        let mut probe = startup_probe(11.0, Some(18.0));
        probe.source_size_bytes = Some(18_000_000_000);
        let plan = super::AutoPrebufferPlan::from_probe(&probe).unwrap();

        assert_eq!(plan.target_bytes(), 18_000_000_000);

        plan.estimated_goodput_bps
            .store(
                16_800_000.0f64.to_bits(),
                std::sync::atomic::Ordering::Relaxed,
            );
        plan.rate_trusted
            .store(true, std::sync::atomic::Ordering::Relaxed);

        assert!((2_879_000_000..=2_881_000_000).contains(&plan.target_bytes()));
    }

    #[test]
    fn auto_prebuffer_keeps_a_minimum_runway_for_fast_sources() {
        let mut probe = startup_probe(30.0, Some(18.0));
        probe.source_size_bytes = Some(18_000_000_000);
        let plan = super::AutoPrebufferPlan::from_probe(&probe).unwrap();
        plan.estimated_goodput_bps
            .store(
                30_000_000.0f64.to_bits(),
                std::sync::atomic::Ordering::Relaxed,
            );
        plan.rate_trusted
            .store(true, std::sync::atomic::Ordering::Relaxed);

        assert_eq!(plan.target_bytes(), 135_000_000);
    }

    #[test]
    fn auto_prebuffer_requires_a_complete_live_probe() {
        let media = crate::db::Media::default();
        let mut cached_only = startup_probe(1.0, Some(18.0));
        cached_only.source_size_bytes = Some(18_000_000_000);
        cached_only.bytes = super::HEDGE_PREFIX_BYTES;
        let mut candidates = Vec::new();

        assert!(!super::retain_prebuffer_candidate(
            &mut candidates,
            &media,
            &cached_only,
        ));

        cached_only.bytes = super::HEDGE_PROBE_BYTES;
        assert!(super::retain_prebuffer_candidate(
            &mut candidates,
            &media,
            &cached_only,
        ));
    }

    #[test]
    fn average_bitrate_fallback_uses_size_and_runtime() {
        let bitrate = super::average_bitrate_bps(9_000_000_000, 7_200.0)
            .expect("valid size and runtime");
        assert!((bitrate - 10_000_000.0).abs() < f64::EPSILON);
    }

    #[test]
    fn slow_auto_startup_requires_enough_observation_time() {
        assert_eq!(
            super::slow_auto_startup_ratio(Duration::from_secs(11), 6, Some(0), 1,),
            None
        );
    }

    #[test]
    fn slow_auto_startup_detects_unsustainable_segment_production() {
        let ratio =
            super::slow_auto_startup_ratio(Duration::from_secs(36), 6, Some(0), 1)
                .expect("one segment in 36 seconds is too slow");
        assert!((ratio - (1.0 / 6.0)).abs() < f64::EPSILON);
    }

    #[test]
    fn slow_auto_startup_detects_no_initial_progress() {
        assert_eq!(
            super::slow_auto_startup_ratio(Duration::from_secs(18), 6, None, 0,),
            Some(0.0)
        );
    }

    #[test]
    fn slow_auto_observation_scales_with_segment_length() {
        assert_eq!(
            super::slow_auto_observation_window(6),
            Duration::from_secs(18)
        );
        assert_eq!(
            super::slow_auto_observation_window(10),
            Duration::from_secs(24)
        );
    }

    #[test]
    fn slow_auto_startup_accepts_real_time_production() {
        assert_eq!(
            super::slow_auto_startup_ratio(Duration::from_secs(24), 6, Some(3), 4,),
            None
        );
    }

    #[test]
    fn slow_auto_startup_only_applies_to_early_playback() {
        assert_eq!(
            super::slow_auto_startup_ratio(Duration::from_secs(121), 6, Some(4), 5,),
            None
        );
    }

    #[test]
    fn live_channel_forces_aac_over_copy() {
        assert_eq!(super::resolve_live_audio_codec(true, "copy"), "aac");
        assert_eq!(super::resolve_live_audio_codec(false, "copy"), "copy");
        assert_eq!(super::resolve_live_audio_codec(true, "aac"), "aac");
        assert_eq!(super::resolve_live_audio_codec(true, "ac3"), "ac3");
    }

    #[test]
    fn hedged_source_rechecks_audio_copy_compatibility() {
        assert_eq!(
            super::resolve_source_audio_codec("copy", Some("aac")),
            "copy"
        );
        assert_eq!(
            super::resolve_source_audio_codec("copy", Some("opus")),
            "aac"
        );
        assert_eq!(
            super::resolve_source_audio_codec("copy", Some("dts")),
            "aac"
        );
        assert_eq!(
            super::resolve_source_audio_codec("aac", Some("opus")),
            "aac"
        );
        assert_eq!(super::resolve_source_audio_codec("copy", None), "aac");
    }

    #[test]
    fn changed_source_remaps_audio_by_language_instead_of_index() {
        let source = crate::api::MediaSourceInfo {
            media_streams: vec![
                crate::api::MediaStream {
                    type_: Some(crate::api::MediaStreamType::Audio),
                    index: 1,
                    language: Some("fra".to_string()),
                    codec: Some("eac3".to_string()),
                    is_default: Some(true),
                    ..Default::default()
                },
                crate::api::MediaStream {
                    type_: Some(crate::api::MediaStreamType::Audio),
                    index: 3,
                    language: Some("eng".to_string()),
                    codec: Some("aac".to_string()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let selected =
            super::select_source_audio_stream(&source, Some(1), Some("en"), true)
                .expect("English audio track");
        assert_eq!(selected.index, 3);
    }

    #[test]
    fn unchanged_source_preserves_explicit_audio_index() {
        let source = crate::api::MediaSourceInfo {
            media_streams: vec![
                crate::api::MediaStream {
                    type_: Some(crate::api::MediaStreamType::Audio),
                    index: 1,
                    language: Some("fra".to_string()),
                    ..Default::default()
                },
                crate::api::MediaStream {
                    type_: Some(crate::api::MediaStreamType::Audio),
                    index: 3,
                    language: Some("eng".to_string()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let selected =
            super::select_source_audio_stream(&source, Some(1), Some("en"), false)
                .expect("explicit audio track");
        assert_eq!(selected.index, 1);
    }

    #[test]
    fn resumed_ts_hls_uses_ffmpeg_variant_playlist() {
        assert!(!super::should_serve_ffmpeg_variant_playlist(
            false, false, 0
        ));
        assert!(!super::should_serve_ffmpeg_variant_playlist(
            false, false, 1
        ));
        // fMP4 now also uses synthetic VOD playlist — full seek bar from the start.
        assert!(!super::should_serve_ffmpeg_variant_playlist(false, true, 0));
        assert!(super::should_serve_ffmpeg_variant_playlist(true, false, 0));
    }
}
