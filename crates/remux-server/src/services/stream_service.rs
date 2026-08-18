use crate::{
    AppContext, api, db,
    playback::probe::{probe_stream, resolve_stream_root},
    stream::StreamDescriptor,
};
use futures_util::{StreamExt, stream};
use remux_sdks::{
    remux::{MediaStreamType, StreamFilter, VideoRangeType, lang_to_two_letter},
    remuxdb,
};
use std::{
    cmp::Reverse,
    collections::{HashMap, HashSet},
    time::Duration,
};
use tracing::debug;
use uuid::Uuid;

/// Result of probing a single stream candidate.
pub(crate) struct ProbeResult {
    /// Probed source info with id/name/path/remux already stamped.
    pub source: api::MediaSourceInfo,
    /// Original candidate stream (needed for RTSP check, subtitle extraction).
    pub stream: db::Media,
    /// Effective stream post-fallback (may differ from `stream` if probe failed over).
    pub effective_stream: db::Media,
}

/// Result of `StreamService::probe_candidates`.
pub(crate) struct ProbedStreams {
    pub results: Vec<ProbeResult>,
    /// True when the client named a specific stream — keep its UUID, don't override to item_id.
    pub specific_requested: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct AutoSessionChoice {
    pub item_id: Uuid,
    pub resolution: String,
    pub winner_id: Uuid,
    pub fallback_ids: Vec<Uuid>,
    pub user_id: Option<Uuid>,
}

#[derive(Clone, Debug)]
struct AutoRankedChoice {
    winner_id: Uuid,
    fallback_ids: Vec<Uuid>,
}

fn startup_compatibility_score(candidate: &db::Media) -> u64 {
    let Some(probe) = candidate
        .probe_data
        .as_ref()
    else {
        return 700;
    };
    let container = probe
        .container
        .as_deref()
        .unwrap_or_default()
        .to_ascii_lowercase();
    let video_codec = probe
        .video_stream()
        .and_then(|stream| {
            stream
                .codec
                .as_deref()
        })
        .unwrap_or_default()
        .to_ascii_lowercase();
    let audio_codec = probe
        .audio_stream()
        .and_then(|stream| {
            stream
                .codec
                .as_deref()
        })
        .unwrap_or_default()
        .to_ascii_lowercase();

    let video = match video_codec.as_str() {
        "h264" | "avc" | "avc1" => 1000u64,
        "hevc" | "h265" | "hev1" | "hvc1" => 650,
        "av1" => 600,
        "vp9" => 700,
        _ => 750,
    };
    let container = match container.as_str() {
        "mp4" | "m4v" | "mov" => 1000u64,
        "mkv" | "matroska" | "webm" => 800,
        _ => 850,
    };
    let audio = match audio_codec.as_str() {
        "aac" | "mp3" => 1000u64,
        "ac3" | "eac3" | "dts" | "truehd" => 800,
        "opus" | "vorbis" => 850,
        _ if audio_codec.is_empty() => 900,
        _ => 850,
    };

    video
        .saturating_mul(container)
        .saturating_div(1000)
        .saturating_mul(audio)
        .saturating_div(1000)
}

fn relative_size_fit_score(
    file_bytes: Option<u64>,
    target_file_bytes: Option<u64>,
) -> u64 {
    match (file_bytes, target_file_bytes) {
        (Some(size), Some(target)) if size > 0 && target > 0 => size
            .min(target)
            .saturating_mul(1000)
            .saturating_div(size.max(target)),
        (Some(_), None) => 1000,
        _ => 500,
    }
}

fn lower_median(mut values: Vec<u64>) -> Option<u64> {
    values.sort_unstable();
    values
        .get(
            values
                .len()
                .saturating_sub(1)
                / 2,
        )
        .copied()
}

fn known_candidate_file_bytes(
    candidate: &db::Media,
    preflight: Option<&crate::torrent::TorrentPreflight>,
) -> Option<u64> {
    preflight
        .and_then(|result| result.selected_file_bytes)
        .or_else(|| {
            candidate
                .probe_data
                .as_ref()
                .and_then(|probe| probe.size)
                .and_then(|size| u64::try_from(size).ok())
                .filter(|size| *size > 0)
        })
        .or_else(|| {
            candidate
                .stream_info
                .as_ref()
                .and_then(|info| info.size)
                .and_then(|size| u64::try_from(size).ok())
                .filter(|size| *size > 0)
        })
}

fn has_stereoscopic_release_tag(value: &str) -> bool {
    let tokens: Vec<_> = value
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|token| !token.is_empty())
        .collect();
    tokens
        .iter()
        .any(|token| {
            ["3d", "hsbs", "hou", "mvc"]
                .iter()
                .any(|tag| token.eq_ignore_ascii_case(tag))
        })
        || tokens
            .windows(2)
            .any(|pair| {
                (pair[0].eq_ignore_ascii_case("h")
                    || pair[0].eq_ignore_ascii_case("half"))
                    && (pair[1].eq_ignore_ascii_case("sbs")
                        || pair[1].eq_ignore_ascii_case("ou"))
            })
}

fn is_stereoscopic_auto_candidate(candidate: &db::Media) -> bool {
    has_stereoscopic_release_tag(&candidate.title)
        || candidate
            .stream_info
            .as_ref()
            .is_some_and(|info| {
                info.filename
                    .as_deref()
                    .is_some_and(has_stereoscopic_release_tag)
                    || info
                        .name
                        .as_deref()
                        .is_some_and(has_stereoscopic_release_tag)
                    || info
                        .description
                        .as_deref()
                        .is_some_and(has_stereoscopic_release_tag)
            })
}

fn auto_release_tokens(candidate: &db::Media) -> Vec<String> {
    let mut values = vec![
        candidate
            .title
            .as_str(),
    ];
    if let Some(info) = candidate
        .stream_info
        .as_ref()
    {
        values.extend(
            [
                info.filename
                    .as_deref(),
                info.name
                    .as_deref(),
                info.description
                    .as_deref(),
            ]
            .into_iter()
            .flatten(),
        );
    }
    values
        .into_iter()
        .flat_map(|value| {
            value
                .split(|character: char| !character.is_ascii_alphanumeric())
                .filter(|token| !token.is_empty())
                .map(str::to_ascii_lowercase)
        })
        .collect()
}

/// Releases that may contain the requested movie but are not the normal
/// feature presentation, plus capture sources that should never win Auto.
/// Manual selection remains available for users who intentionally want one.
fn is_blocked_auto_candidate(candidate: &db::Media) -> bool {
    let tokens = auto_release_tokens(candidate);
    tokens
        .iter()
        .any(|token| {
            matches!(
                token.as_str(),
                "rifftrax"
                    | "parody"
                    | "fanedit"
                    | "fanedited"
                    | "mst3k"
                    | "commentary"
                    | "camrip"
                    | "hdcam"
                    | "telesync"
                    | "hdts"
                    | "telecine"
                    | "workprint"
            )
        })
        || tokens
            .windows(2)
            .any(|pair| pair[0] == "fan" && pair[1] == "edit")
}

fn release_language_tag(token: &str) -> Option<&'static str> {
    match token {
        "eng" | "english" => Some("en"),
        "vf2" | "vff" | "vfi" | "truefrench" | "fra" | "fre" | "french" => Some("fr"),
        "dublado" | "portuguese" | "brazilian" => Some("pt"),
        "latino" | "castellano" | "spanish" | "espanol" => Some("es"),
        "pldub" | "lektor" | "polish" => Some("pl"),
        "rus" | "russian" => Some("ru"),
        "german" | "deutsch" => Some("de"),
        "italian" => Some("it"),
        "hindi" => Some("hi"),
        "jpn" | "japanese" => Some("ja"),
        "korean" => Some("ko"),
        "mandarin" | "cantonese" | "chinese" => Some("zh"),
        _ => None,
    }
}

fn is_strong_release_language_tag(token: &str) -> bool {
    matches!(
        token,
        "vf2"
            | "vff"
            | "vfi"
            | "truefrench"
            | "dublado"
            | "latino"
            | "castellano"
            | "pldub"
            | "lektor"
    )
}

/// Return true only when we have positive evidence that a candidate lacks the
/// title's desired audio language. Probe metadata is authoritative. Before a
/// torrent has been probed, common release-language conventions (for example
/// VF2 or DUBLADO) are used conservatively. Tokens that also appear in the
/// media title are ignored because they may be title text rather than language
/// metadata.
fn is_language_mismatched_auto_candidate(
    candidate: &db::Media,
    desired_language: Option<&str>,
    parent_title: &str,
) -> bool {
    let Some(desired) = desired_language.and_then(lang_to_two_letter) else {
        return false;
    };

    if let Some(probe) = candidate
        .probe_data
        .as_ref()
    {
        let audio_stream_count = probe
            .media_streams
            .iter()
            .filter(|stream| matches!(stream.type_, Some(MediaStreamType::Audio)))
            .count();
        let known_audio_languages: Vec<_> = probe
            .media_streams
            .iter()
            .filter(|stream| matches!(stream.type_, Some(MediaStreamType::Audio)))
            .filter_map(|stream| {
                stream
                    .language
                    .as_deref()
                    .and_then(lang_to_two_letter)
            })
            .collect();
        if known_audio_languages
            .iter()
            .any(|language| language == &desired)
        {
            return false;
        }
        if audio_stream_count > 0 && known_audio_languages.len() == audio_stream_count {
            return true;
        }
    }

    let parent_tokens: HashSet<_> = parent_title
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|token| !token.is_empty())
        .map(str::to_ascii_lowercase)
        .collect();
    let tagged_languages: Vec<_> = auto_release_tokens(candidate)
        .into_iter()
        .filter(|token| {
            is_strong_release_language_tag(token) || !parent_tokens.contains(token)
        })
        .filter_map(|token| release_language_tag(&token))
        .collect();
    !tagged_languages.is_empty()
        && !tagged_languages
            .iter()
            .any(|language| *language == desired)
}

fn auto_candidate_score(
    candidate: &db::Media,
    preflight: Option<&crate::torrent::TorrentPreflight>,
    tracker: Option<&crate::torrent_availability::TrackerAvailability>,
    peer: Option<&crate::torrent_availability::PeerAvailability>,
    target_file_bytes: Option<u64>,
) -> (
    u8,
    u64,
    u64,
    u64,
    usize,
    usize,
    u64,
    u32,
    u64,
    Reverse<u64>,
    Reverse<u64>,
) {
    let managed = preflight.and_then(|result| {
        result
            .managed
            .as_ref()
    });
    let finished = managed
        .map(|state| state.finished)
        .unwrap_or(false);
    let live_peers = managed
        .map(|state| state.live_peers)
        .unwrap_or(0);
    let seen_peers = preflight
        .map(|result| result.seen_peers)
        .unwrap_or(0);
    let tracker_seeders = tracker
        .map(|result| result.seeders)
        .unwrap_or(0);
    let tracker_peers = tracker
        .map(|result| {
            result
                .peers
                .len()
        })
        .unwrap_or(0);
    let active_seeders = peer
        .map(|availability| availability.seeders)
        .unwrap_or(0);
    let responsive_peers = peer
        .map(|availability| availability.responsive)
        .unwrap_or(0);
    let provider_seeders = candidate
        .stream_info
        .as_ref()
        .and_then(|info| info.seeders)
        .unwrap_or(0)
        .max(0) as u64;
    let compatibility_score = startup_compatibility_score(candidate);
    let progress_per_thousand = managed
        .filter(|state| state.total_bytes > 0)
        .map(|state| {
            state
                .progress_bytes
                .saturating_mul(1000)
                / state.total_bytes
        })
        .unwrap_or(0);
    let tier = if finished {
        5
    } else if live_peers > 0 {
        4
    } else if active_seeders > 0 || responsive_peers > 0 || seen_peers > 0 {
        3
    } else if candidate
        .probe_data
        .is_some()
    {
        2
    } else if tracker_seeders > 0 {
        2
    } else if provider_seeders > 0 {
        1
    } else {
        0
    };
    let elapsed_ms = preflight
        .map(|result| result.elapsed_ms)
        .unwrap_or(u64::MAX);
    let file_bytes = known_candidate_file_bytes(candidate, preflight);

    // Same-resolution releases have diminishing quality returns as bitrate
    // grows. Use the live candidates' median size as a relative quality target:
    // undersized outliers are penalized, while oversized releases gain no
    // quality bonus and must justify their slower startup with much stronger
    // availability. No absolute GB threshold is encoded here.
    let quality_score = match (file_bytes, target_file_bytes) {
        (Some(size), Some(target)) if target > 0 => {
            size.min(target)
                .saturating_mul(1000)
                / target
        }
        (Some(_), None) => 1000,
        _ => 500,
    };
    // `seen_peers` is every address discovered during metadata lookup, not a
    // count of connected seeders. Keep those broad counts logarithmic so a
    // stale swarm cannot overwhelm the peers that answered a wire handshake
    // and advertised a complete bitfield right now.
    let seed_signal = (active_seeders as u64)
        .saturating_mul(5000)
        .saturating_add((live_peers as u64).saturating_mul(1500))
        .saturating_add((responsive_peers as u64).saturating_mul(500))
        .saturating_add(((seen_peers as u64 + 1).ilog2() as u64).saturating_mul(100))
        .saturating_add(((tracker_peers as u64 + 1).ilog2() as u64).saturating_mul(75))
        .saturating_add(
            (tracker_seeders as u64 + provider_seeders + 1).ilog2() as u64 * 50,
        )
        .max(1);
    // Penalize both sides of the same-resolution target symmetrically. A file
    // one fifth the target (likely over-compressed) or five times the target
    // (higher required streaming bitrate) must each have roughly five times
    // stronger live availability to win. This remains a soft prior derived
    // from the current candidates; no absolute size is hard-coded.
    let size_fit_score = relative_size_fit_score(file_bytes, target_file_bytes);
    let startup_score = seed_signal
        .saturating_mul(size_fit_score.max(1))
        .saturating_div(1000);

    (
        tier,
        startup_score,
        compatibility_score,
        quality_score,
        active_seeders,
        responsive_peers,
        progress_per_thousand,
        tracker_seeders,
        provider_seeders,
        Reverse(file_bytes.unwrap_or(u64::MAX)),
        Reverse(elapsed_ms),
    )
}

#[cfg(test)]
mod auto_score_tests {
    use super::{
        AutoRankedChoice, StreamService, is_blocked_auto_candidate,
        is_language_mismatched_auto_candidate, is_stereoscopic_auto_candidate,
        known_candidate_file_bytes, lower_median, relative_size_fit_score,
    };
    use remux_sdks::remux::{MediaSourceInfo, MediaStream, MediaStreamType};
    use std::time::Duration;
    use uuid::Uuid;

    #[test]
    fn auto_resolution_round_trips_synthetic_id() {
        let item_id = Uuid::new_v4();
        let auto_id = Uuid::new_v5(
            &Uuid::NAMESPACE_URL,
            format!("{item_id}-auto-1080p").as_bytes(),
        );

        assert_eq!(
            StreamService::auto_resolution(item_id, auto_id),
            Some("1080p")
        );
        assert_eq!(
            StreamService::auto_resolution(item_id, Uuid::new_v4()),
            None
        );
    }

    #[test]
    fn size_fit_penalizes_equal_relative_outliers_symmetrically() {
        assert_eq!(
            relative_size_fit_score(Some(600), Some(3_000)),
            relative_size_fit_score(Some(15_000), Some(3_000)),
        );
        assert_eq!(relative_size_fit_score(Some(600), Some(3_000)), 200);
    }

    #[test]
    fn size_fit_favors_the_relative_target() {
        assert_eq!(relative_size_fit_score(Some(3_000), Some(3_000)), 1_000);
        assert_eq!(relative_size_fit_score(Some(1_500), Some(3_000)), 500);
        assert_eq!(relative_size_fit_score(Some(6_000), Some(3_000)), 500);
    }

    #[test]
    fn lower_median_is_not_anchored_by_one_large_outlier() {
        assert_eq!(lower_median(vec![600, 2_000, 3_000, 11_000]), Some(2_000));
    }

    #[test]
    fn candidate_size_uses_existing_probe_metadata() {
        let candidate = crate::db::Media {
            probe_data: Some(crate::api::MediaSourceInfo {
                size: Some(42_000),
                ..Default::default()
            }),
            ..Default::default()
        };

        assert_eq!(known_candidate_file_bytes(&candidate, None), Some(42_000));
    }

    #[test]
    fn auto_excludes_stereoscopic_releases() {
        let stereoscopic = crate::db::Media {
            title: "Torrentio\n1080p 3D HSBS".to_string(),
            ..Default::default()
        };
        let ordinary = crate::db::Media {
            title: "Torrentio\n1080p BluRay H.264".to_string(),
            ..Default::default()
        };

        assert!(is_stereoscopic_auto_candidate(&stereoscopic));
        assert!(!is_stereoscopic_auto_candidate(&ordinary));
    }

    #[test]
    fn auto_excludes_alternate_cuts_and_bad_captures_from_filenames() {
        for filename in [
            "Example.Movie.2020.1080p.Rifftrax.6ch.x265.mkv",
            "Movie.2026.1080p.TELESYNC.x264.mkv",
            "Movie.2026.1080p.Fan.Edit.mkv",
        ] {
            let candidate = crate::db::Media {
                title: "Torrentio\n1080p".to_string(),
                stream_info: Some(crate::stream::StreamInfo {
                    filename: Some(filename.to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            };
            assert!(is_blocked_auto_candidate(&candidate), "{filename}");
        }
    }

    #[test]
    fn auto_flags_unprobed_foreign_default_release_for_english_title() {
        let candidate = crate::db::Media {
            title: "Torrentio\n1080p".to_string(),
            stream_info: Some(crate::stream::StreamInfo {
                filename: Some(
                    "Example.Feature.2026.MULTi.VF2.1080p.WEB.H264.mkv".to_string(),
                ),
                ..Default::default()
            }),
            ..Default::default()
        };

        assert!(is_language_mismatched_auto_candidate(
            &candidate,
            Some("en"),
            "Example Feature",
        ));
    }

    #[test]
    fn probed_matching_audio_overrides_release_language_hint() {
        let candidate = crate::db::Media {
            title: "Torrentio\n1080p".to_string(),
            stream_info: Some(crate::stream::StreamInfo {
                filename: Some("Movie.MULTi.VF2.1080p.mkv".to_string()),
                ..Default::default()
            }),
            probe_data: Some(MediaSourceInfo {
                media_streams: vec![
                    MediaStream {
                        type_: Some(MediaStreamType::Audio),
                        index: 1,
                        language: Some("fra".to_string()),
                        ..Default::default()
                    },
                    MediaStream {
                        type_: Some(MediaStreamType::Audio),
                        index: 3,
                        language: Some("eng".to_string()),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }),
            ..Default::default()
        };

        assert!(!is_language_mismatched_auto_candidate(
            &candidate,
            Some("en"),
            "Movie",
        ));
    }

    #[test]
    fn partially_unlabeled_audio_is_not_assumed_to_lack_desired_language() {
        let candidate = crate::db::Media {
            title: "Torrentio\n1080p".to_string(),
            stream_info: Some(crate::stream::StreamInfo {
                filename: Some("Movie.2026.1080p.WEB.mkv".to_string()),
                ..Default::default()
            }),
            probe_data: Some(MediaSourceInfo {
                media_streams: vec![
                    MediaStream {
                        type_: Some(MediaStreamType::Audio),
                        index: 1,
                        language: Some("fra".to_string()),
                        ..Default::default()
                    },
                    MediaStream {
                        type_: Some(MediaStreamType::Audio),
                        index: 2,
                        language: None,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }),
            ..Default::default()
        };

        assert!(!is_language_mismatched_auto_candidate(
            &candidate,
            Some("en"),
            "Movie",
        ));
    }

    #[test]
    fn language_word_in_movie_title_is_not_treated_as_a_release_tag() {
        let candidate = crate::db::Media {
            title: "Torrentio\n1080p".to_string(),
            stream_info: Some(crate::stream::StreamInfo {
                filename: Some("A.French.Journey.2021.1080p.BluRay.mkv".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };

        assert!(!is_language_mismatched_auto_candidate(
            &candidate,
            Some("en"),
            "A French Journey",
        ));
    }

    #[test]
    fn auto_startup_allows_only_one_post_selection_failover() {
        let store = remux_utils::Store::new_weighted(1024);

        assert!(StreamService::claim_auto_failover_slot(
            &store,
            "play-session"
        ));
        assert!(!StreamService::claim_auto_failover_slot(
            &store,
            "play-session"
        ));
        assert!(StreamService::claim_auto_failover_slot(
            &store,
            "other-session"
        ));
    }

    #[test]
    fn empirical_auto_winner_keeps_the_ranked_candidates_as_fallbacks() {
        let store = remux_utils::Store::new_weighted(1024);
        let item_id = Uuid::new_v4();
        let ranked_winner = Uuid::new_v4();
        let empirical_winner = Uuid::new_v4();
        let other = Uuid::new_v4();
        let auto_id = Uuid::new_v5(
            &Uuid::NAMESPACE_URL,
            format!("{item_id}-auto-1080p").as_bytes(),
        );
        store.save(
            StreamService::auto_ranked_choice_key(item_id, "1080p"),
            AutoRankedChoice {
                winner_id: ranked_winner,
                fallback_ids: vec![empirical_winner, other],
            },
            Duration::from_secs(60),
        );

        StreamService::pin_auto_session_winner(
            &store,
            "play-session",
            item_id,
            auto_id,
            empirical_winner,
            None,
        );

        let choice =
            StreamService::auto_session_choice(&store, "play-session").unwrap();
        assert_eq!(choice.winner_id, empirical_winner);
        assert_eq!(choice.fallback_ids, vec![ranked_winner, other]);
    }
}

pub(crate) struct StreamServiceConfig {
    pub ctx: AppContext,
    pub item_id: Uuid,
    pub requested_id: Option<Uuid>,
    pub show_ungrouped: bool,
    pub stream_filter: Option<StreamFilter>,
    pub user_id: Option<Uuid>,
}

/// Central service for stream selection on a single playback request.
///
/// Construct with `new()`, then call `resolve()` to do all async work (group detection,
/// stream loading, policy filtering). After that the selection and ID-mapping methods
/// are available with no further parameters.
pub(crate) struct StreamService {
    ctx: AppContext,
    pub item_id: Uuid,
    pub requested_id: Option<Uuid>,
    show_ungrouped: bool,
    stream_filter: Option<StreamFilter>,
    user_id: Option<Uuid>,
    // Populated by resolve()
    group: Option<(Uuid, String, Vec<db::Media>)>,
    stream: Option<db::Media>,
    pub streams: Vec<db::Media>,
}

impl StreamService {
    const AUTO_SESSION_CANDIDATE_LIMIT: usize = 12;
    // A metadata-ranked winner is only a hint; the HLS startup hedge still
    // verifies real piece throughput on every playback. A short cache avoids
    // repeating tracker and peer discovery across closely spaced requests.
    const AUTO_CHOICE_TTL: Duration = Duration::from_secs(5 * 60);
    pub(crate) const AUTO_RESOLUTIONS: [&'static str; 5] =
        ["4K", "1080p", "720p", "480p", "1440p"];

    pub(crate) fn auto_source_id(item_id: Uuid, resolution: &str) -> Uuid {
        Uuid::new_v5(
            &Uuid::NAMESPACE_URL,
            format!("{item_id}-auto-{resolution}").as_bytes(),
        )
    }

    fn auto_session_winner_key(play_session_id: &str) -> String {
        format!("auto-session-winner:{play_session_id}")
    }

    fn auto_choice_key(item_id: Uuid, resolution: &str) -> String {
        format!("auto-choice:{item_id}:{}", resolution.to_ascii_lowercase())
    }

    pub(crate) fn remember_auto_choice(
        store: &remux_utils::Store,
        item_id: Uuid,
        resolution: &str,
        winner_id: Uuid,
    ) {
        store.save(
            Self::auto_choice_key(item_id, resolution),
            winner_id,
            Self::AUTO_CHOICE_TTL,
        );
    }

    fn auto_ranked_choice_key(item_id: Uuid, resolution: &str) -> String {
        format!(
            "auto-ranked-choice:{item_id}:{}",
            resolution.to_ascii_lowercase()
        )
    }

    fn auto_failed_candidate_key(
        item_id: Uuid,
        resolution: &str,
        failure_token: &str,
    ) -> String {
        format!(
            "auto-failed:{item_id}:{}:{}",
            resolution.to_ascii_lowercase(),
            failure_token.to_ascii_lowercase()
        )
    }

    fn auto_candidate_failure_token(candidate: &db::Media) -> String {
        candidate
            .stream_info
            .as_ref()
            .and_then(|info| {
                info.descriptor
                    .torrent_info_hash()
            })
            .map(str::to_ascii_lowercase)
            .unwrap_or_else(|| {
                candidate
                    .id
                    .to_string()
            })
    }

    fn auto_failover_attempt_key(play_session_id: &str, candidate_id: Uuid) -> String {
        format!("auto-failover-attempt:{play_session_id}:{candidate_id}")
    }

    fn auto_failover_slot_key(play_session_id: &str) -> String {
        format!("auto-failover-slot:{play_session_id}")
    }

    pub(crate) fn auto_resolution(
        item_id: Uuid,
        auto_id: Uuid,
    ) -> Option<&'static str> {
        Self::AUTO_RESOLUTIONS
            .iter()
            .copied()
            .find(|resolution| Self::auto_source_id(item_id, resolution) == auto_id)
    }

    pub(crate) fn pin_auto_session_winner(
        store: &remux_utils::Store,
        play_session_id: &str,
        item_id: Uuid,
        auto_id: Uuid,
        winner_id: Uuid,
        user_id: Option<Uuid>,
    ) {
        let Some(resolution) = Self::auto_resolution(item_id, auto_id) else {
            tracing::warn!(
                %play_session_id,
                %item_id,
                %auto_id,
                "refusing to pin unknown auto source"
            );
            return;
        };
        let fallback_ids = store
            .get::<AutoRankedChoice>(Self::auto_ranked_choice_key(item_id, resolution))
            .map(|choice| {
                std::iter::once(choice.winner_id)
                    .chain(
                        choice
                            .fallback_ids
                            .iter()
                            .copied(),
                    )
                    .filter(|fallback_id| *fallback_id != winner_id)
                    .take(Self::AUTO_SESSION_CANDIDATE_LIMIT.saturating_sub(1))
                    .collect()
            })
            .unwrap_or_default();
        store.save(
            Self::auto_session_winner_key(play_session_id),
            AutoSessionChoice {
                item_id,
                resolution: resolution.to_string(),
                winner_id,
                fallback_ids,
                user_id,
            },
            Duration::from_secs(6 * 60 * 60),
        );
    }

    pub(crate) fn update_auto_session_choice(
        store: &remux_utils::Store,
        play_session_id: &str,
        winner_id: Uuid,
        fallback_ids: Vec<Uuid>,
    ) -> bool {
        let Some(mut choice) = Self::auto_session_choice(store, play_session_id) else {
            return false;
        };
        choice.winner_id = winner_id;
        choice.fallback_ids = fallback_ids
            .into_iter()
            .filter(|fallback_id| *fallback_id != winner_id)
            .take(Self::AUTO_SESSION_CANDIDATE_LIMIT.saturating_sub(1))
            .collect();
        store.save(
            Self::auto_session_winner_key(play_session_id),
            choice,
            Duration::from_secs(6 * 60 * 60),
        );
        true
    }

    pub(crate) fn auto_session_choice(
        store: &remux_utils::Store,
        play_session_id: &str,
    ) -> Option<AutoSessionChoice> {
        store
            .get::<AutoSessionChoice>(Self::auto_session_winner_key(play_session_id))
            .map(|choice| (*choice).clone())
    }

    pub(crate) fn auto_session_winner(
        store: &remux_utils::Store,
        play_session_id: &str,
    ) -> Option<Uuid> {
        Self::auto_session_choice(store, play_session_id).map(|choice| choice.winner_id)
    }

    pub(crate) fn clear_auto_session_winner(
        store: &remux_utils::Store,
        play_session_id: &str,
    ) {
        store.delete(Self::auto_session_winner_key(play_session_id));
    }

    pub(crate) fn claim_auto_failover_attempt(
        store: &remux_utils::Store,
        play_session_id: &str,
        candidate_id: Uuid,
    ) -> bool {
        store.insert(
            Self::auto_failover_attempt_key(play_session_id, candidate_id),
            (),
            Duration::from_secs(60),
        )
    }

    /// Permit one post-selection source switch. The startup hedge has already
    /// measured several swarms, so serially walking the whole fallback list
    /// only turns a canceled player request into minutes of background I/O.
    pub(crate) fn claim_auto_failover_slot(
        store: &remux_utils::Store,
        play_session_id: &str,
    ) -> bool {
        store.insert(
            Self::auto_failover_slot_key(play_session_id),
            (),
            Duration::from_secs(10 * 60),
        )
    }

    pub(crate) fn mark_auto_candidate_failed(
        store: &remux_utils::Store,
        item_id: Uuid,
        resolution: &str,
        candidate: &db::Media,
    ) {
        store.save(
            Self::auto_failed_candidate_key(
                item_id,
                resolution,
                &Self::auto_candidate_failure_token(candidate),
            ),
            (),
            Duration::from_secs(10 * 60),
        );
        store.delete(Self::auto_choice_key(item_id, resolution));
        store.delete(Self::auto_ranked_choice_key(item_id, resolution));
    }

    pub(crate) fn clear_auto_candidate_failed(
        store: &remux_utils::Store,
        item_id: Uuid,
        resolution: &str,
        candidate: &db::Media,
    ) {
        store.delete(Self::auto_failed_candidate_key(
            item_id,
            resolution,
            &Self::auto_candidate_failure_token(candidate),
        ));
    }

    pub(crate) fn is_auto_candidate_failed(
        store: &remux_utils::Store,
        item_id: Uuid,
        resolution: &str,
        candidate: &db::Media,
    ) -> bool {
        store
            .get::<()>(Self::auto_failed_candidate_key(
                item_id,
                resolution,
                &Self::auto_candidate_failure_token(candidate),
            ))
            .is_some()
    }

    pub fn new(cfg: StreamServiceConfig) -> Self {
        Self {
            ctx: cfg.ctx,
            item_id: cfg.item_id,
            requested_id: cfg.requested_id,
            show_ungrouped: cfg.show_ungrouped,
            stream_filter: cfg.stream_filter,
            user_id: cfg.user_id,
            group: None,
            stream: None,
            streams: vec![],
        }
    }

    /// Load the service from a pre-fetched media item (playbackinfo path).
    ///
    /// Populates `self.group`, `self.stream`, and `self.streams`. Must be called
    /// before any of the selection or ID-mapping methods.
    pub async fn load(&mut self, media: db::Media) -> anyhow::Result<()> {
        // A synthetic Auto id represents a playable source even when its parent
        // has no cached torrent rows yet. Preserve it so PlaybackInfo can return
        // inferred streams while live source resolution completes.
        if media.kind == db::MediaKind::Stream
            && media
                .title
                .ends_with("(auto)")
        {
            self.stream = Some(media.clone());
            self.streams = vec![media];
            return Ok(());
        }
        if media.kind == db::MediaKind::StreamGroup {
            if let Ok(Some(mut parent)) = db::Media::get_by_id(
                &self
                    .ctx
                    .db,
                &self.item_id,
            )
            .await
            {
                self.ctx
                    .addons
                    .refresh_streams(&mut parent, &self.ctx, self.user_id)
                    .await
                    .inspect_err(|e| tracing::error!("refresh_streams failed: {e:#}"));
            }
            self.resolve_stream_group(media)
                .await?;
            return Ok(());
        }

        let mut root = resolve_stream_root(
            &media,
            self.item_id,
            &self
                .ctx
                .db,
        )
        .await;

        self.ctx
            .addons
            .refresh_streams(&mut root, &self.ctx, self.user_id)
            .await
            .inspect_err(|e| tracing::error!("refresh_streams failed: {e:#}"));

        let root_kind = root
            .kind
            .clone();
        let db_streams = root
            .streams(
                &self
                    .ctx
                    .db,
            )
            .await?;
        let raw = if db_streams.is_empty() {
            // Root item can be the stream itself (e.g. locally-imported files)
            // but only when it carries a URL. Addon content uses the root as a
            // container — falling back to it when the addon returned no streams
            // would queue a probe against an item with no stream_info.
            if root
                .stream_info
                .is_some()
            {
                vec![root]
            } else {
                vec![]
            }
        } else {
            db_streams
        };

        let streams = db::StreamGroup::filter_sources(
            &self
                .ctx
                .db,
            raw,
            self.show_ungrouped,
        )
        .await;
        let streams = if let Some(sf) = self
            .stream_filter
            .as_ref()
            .filter(|sf| {
                !sf.rules
                    .is_empty()
            })
            .filter(|_| {
                matches!(root_kind, db::MediaKind::Movie | db::MediaKind::Episode)
            }) {
            let before = streams.len();
            let filtered = db::apply_stream_filter(sf, streams);
            debug!(
                streams_before = before,
                streams_after = filtered.len(),
                rules = sf
                    .rules
                    .len(),
                "stream filter applied"
            );
            filtered
        } else {
            debug!(
                has_filter = self
                    .stream_filter
                    .is_some(),
                "stream filter skipped"
            );
            streams
        };

        if streams.is_empty() {
            return Ok(());
        }
        self.stream = streams
            .first()
            .cloned();
        self.streams = streams;
        Ok(())
    }

    /// One-shot lookup for handlers that only need a single resolved stream (subtitles, video).
    ///
    /// Handles StreamGroup → best candidate, device preference, and explicit stream UUID.
    /// Returns the concrete `db::Media` to stream.
    pub async fn lookup(
        ctx: &AppContext,
        item_id: Uuid,
        requested_id: Option<Uuid>,
        device_key: Option<&str>,
        user_id: Option<Uuid>,
    ) -> anyhow::Result<db::Media> {
        let lookup_id = requested_id.unwrap_or(item_id);
        let media = db::Media::get_by_id(&ctx.db, &lookup_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("stream not found: {}", lookup_id))?;
        Self::dispatch_lookup(ctx, item_id, requested_id, device_key, user_id, media)
            .await
    }

    async fn dispatch_lookup(
        ctx: &AppContext,
        item_id: Uuid,
        requested_id: Option<Uuid>,
        device_key: Option<&str>,
        user_id: Option<Uuid>,
        media: db::Media,
    ) -> anyhow::Result<db::Media> {
        match media.kind {
            db::MediaKind::StreamGroup => {
                let gid = media.id;
                let mut candidates =
                    db::StreamGroup::streams_for(&ctx.db, &gid, &item_id).await?;
                if candidates.is_empty() {
                    return Err(anyhow::anyhow!(
                        "no streams available for group {}",
                        gid
                    ));
                }
                let cascade =
                    db::StreamGroup::streams_for_groups_after(&ctx.db, &gid, &item_id)
                        .await
                        .unwrap_or_default();
                candidates.extend(cascade);
                Ok(candidates.remove(0))
            }
            db::MediaKind::Movie | db::MediaKind::Episode | db::MediaKind::Track => {
                let mut media = media;
                let _ = ctx
                    .addons
                    .refresh_streams(&mut media, ctx, user_id)
                    .await
                    .inspect_err(|e| tracing::error!("refresh_streams failed: {e:#}"));
                let sources = media
                    .streams(&ctx.db)
                    .await?;
                if let Some(sid) = requested_id.filter(|&sid| sid != item_id) {
                    sources
                        .into_iter()
                        .find(|s| s.id == sid)
                        .ok_or_else(|| anyhow::anyhow!("stream not found: {}", sid))
                } else if let Some(key) = device_key {
                    let saved = ctx
                        .store
                        .get::<Uuid>(&format!("pstream:{}:{}", item_id, key));
                    let by_pref = saved.and_then(|sid| {
                        sources
                            .iter()
                            .find(|s| s.id == *sid)
                            .cloned()
                    });
                    by_pref
                        .or_else(|| {
                            sources
                                .into_iter()
                                .next()
                        })
                        .ok_or_else(|| {
                            anyhow::anyhow!("no playable sources for {}", item_id)
                        })
                } else {
                    sources
                        .into_iter()
                        .next()
                        .ok_or_else(|| {
                            anyhow::anyhow!("no playable sources for {}", item_id)
                        })
                }
            }
            _ => Ok(media),
        }
    }

    async fn resolve_stream_group(&mut self, media: db::Media) -> anyhow::Result<()> {
        let gid = media.id;
        let gtitle = media
            .title
            .clone();
        let mut candidates = db::StreamGroup::streams_for(
            &self
                .ctx
                .db,
            &gid,
            &self.item_id,
        )
        .await?;
        if candidates.is_empty() {
            return Err(anyhow::anyhow!("no streams available for group {}", gid));
        }
        let cascade = db::StreamGroup::streams_for_groups_after(
            &self
                .ctx
                .db,
            &gid,
            &self.item_id,
        )
        .await
        .unwrap_or_default();
        candidates.extend(cascade);
        self.stream = Some(candidates[0].clone());
        self.group = Some((gid, gtitle, candidates));
        Ok(())
    }

    /// The concrete resolved stream. Panics if called before `resolve()`.
    pub fn candidate(&self) -> &db::Media {
        self.stream
            .as_ref()
            .expect("StreamService::load() must be called first")
    }

    /// The StreamGroup context, if the request was for a group.
    pub fn group(&self) -> Option<&(Uuid, String, Vec<db::Media>)> {
        self.group
            .as_ref()
    }

    /// UUID the client should see in `MediaSources[0].Id` and `TranscodingUrl MediaSourceId`.
    pub fn client_facing_id(&self) -> Uuid {
        self.group
            .as_ref()
            .map(|(gid, _, _)| *gid)
            .unwrap_or_else(|| {
                self.candidate()
                    .id
            })
    }

    /// UUID for `MediaSources[idx].Id`, using the probe-fallback effective stream.
    pub fn source_id_for(&self, effective: &db::Media) -> Uuid {
        self.group
            .as_ref()
            .map(|(gid, _, _)| *gid)
            .unwrap_or(effective.id)
    }

    /// Display name for `MediaSources[idx].Name`.
    pub fn source_name_for(&self, effective: &db::Media) -> String {
        self.group
            .as_ref()
            .map(|(_, t, _)| t.clone())
            .unwrap_or_else(|| crate::conversions::media_source_display_name(effective))
    }

    fn candidates(&self) -> &[db::Media] {
        self.group
            .as_ref()
            .map(|(_, _, c)| c.as_slice())
            .unwrap_or(&[])
    }

    /// Partition `self.streams` into candidate/probe lists and compute selection flags.
    pub(crate) fn select_streams(&self) -> StreamSelection {
        let all_streams = self
            .streams
            .clone();
        let item_id = self.item_id;
        let requested_id = self.requested_id;

        let specific_requested = self
            .group
            .is_some()
            || requested_id
                .map(|sid| {
                    sid != item_id
                        && all_streams
                            .iter()
                            .any(|s| s.id == sid)
                })
                .unwrap_or(false);

        if self
            .group
            .is_some()
        {
            return StreamSelection {
                candidates: vec![
                    self.candidate()
                        .clone(),
                ],
                probe_pool: self
                    .candidates()
                    .to_vec(),
                restrict_resolution: false,
                probe_only_first: false,
                specific_requested: true,
            };
        }

        let probe_pool = all_streams.clone();

        let (candidates, probe_only_first) = if specific_requested {
            let sid = requested_id.unwrap();
            (
                all_streams
                    .into_iter()
                    .filter(|s| s.id == sid)
                    .collect(),
                false,
            )
        } else if requested_id.is_some() {
            // media_source_id == item_id (Android TV auto-play) or stream not found:
            // return only the first stream; specific_requested stays false so
            // source[0].id is overridden to item_id below (required for Android TV routing).
            let mut v = all_streams;
            v.truncate(1);
            (v, false)
        } else {
            // No stream ID: return all versions for the selection UI,
            // probe only the first to avoid spawning N FFmpeg processes.
            (all_streams, true)
        };

        StreamSelection {
            candidates,
            probe_pool,
            restrict_resolution: true,
            probe_only_first,
            specific_requested,
        }
    }

    /// Probe all stream candidates and return stamped results.
    ///
    /// Internally calls `select_streams()`, loads probe config, then invokes `probe_stream`
    /// for each candidate. Source ID/name/path/remux are stamped before returning so the
    /// handler only deals with playback-decision work.
    pub async fn probe_candidates(&self) -> anyhow::Result<ProbedStreams> {
        let sel = self.select_streams();
        let probe_cfg = db::Settings::get_config_or_default(
            &self
                .ctx
                .db,
        )
        .await;
        let timeout = probe_cfg
            .probe_timeout_secs
            .unwrap_or(20) as u64;
        let timeout_p2p = probe_cfg
            .probe_timeout_p2p_secs
            .unwrap_or(60) as u64;
        let auto_next = probe_cfg
            .auto_next_stream_on_probe_fail
            .unwrap_or(true);
        let max_retries = probe_cfg
            .max_probe_fallback_streams
            .unwrap_or(3) as usize;
        let port = self
            .ctx
            .config
            .port;
        let mut item = db::Media::get_by_id(
            &self
                .ctx
                .db,
            &self.item_id,
        )
        .await
        .ok()
        .flatten();
        if let Some(ref mut it) = item {
            it.grandparent(
                &self
                    .ctx
                    .db,
            )
            .await
            .ok();
        }

        let mut results = Vec::with_capacity(
            sel.candidates
                .len(),
        );
        for (idx, stream) in sel
            .candidates
            .into_iter()
            .enumerate()
        {
            let url_opt = stream
                .stream_info
                .as_ref()
                .map(|si| {
                    si.descriptor
                        .server_input(stream.id, port)
                });
            let skip_probe = sel.probe_only_first && idx > 0;
            let was_cached = stream
                .probe_data
                .as_ref()
                .and_then(|pd| pd.video_stream())
                .is_some();
            let timeout_secs = if stream
                .stream_info
                .as_ref()
                .map_or(false, |si| si.is_p2p())
            {
                timeout_p2p
            } else {
                timeout
            };
            let (mut source, effective_stream) = probe_stream(
                &stream,
                url_opt,
                skip_probe,
                timeout_secs,
                auto_next,
                max_retries,
                &sel.probe_pool,
                sel.restrict_resolution,
                port,
                &self
                    .ctx
                    .db,
            )
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?;

            // Use the StreamGroup UUID when this candidate is a group representative
            // (group_id is set by filter_sources). This ensures the client sends back
            // the stable group UUID, not a stream UUID that can change after a refresh.
            let (cid, name) = if let Some(gid) = stream.group_id {
                (
                    gid,
                    stream
                        .title
                        .clone(),
                )
            } else {
                (
                    self.source_id_for(&effective_stream),
                    self.source_name_for(&effective_stream),
                )
            };
            source.id = cid;
            source.e_tag = cid;
            source.name = Some(name);
            source.has_segments = true;
            source.path = Some(format!("/remux/{}", effective_stream.id));
            source.is_remote = false;
            // Re-apply binge-group headers — ffmpeg probing produces a fresh
            // MediaSourceInfo and would otherwise drop provider hints.
            source.remux = Some(api::MediaSourceRemuxInfo {
                provider_info: stream
                    .stream_info
                    .as_ref()
                    .and_then(|si| serde_json::to_value(si).ok()),
            });

            let remuxdb_enabled = probe_cfg
                .remuxdb_enabled
                .unwrap_or(true);
            let is_remuxdb_kind = item
                .as_ref()
                .map_or(false, |it| {
                    matches!(it.kind, db::MediaKind::Movie | db::MediaKind::Episode)
                });
            if was_cached {
                debug!(id = %effective_stream.id, "remuxdb: skipping (probe cache hit)");
            } else if !remuxdb_enabled {
                debug!(id = %effective_stream.id, "remuxdb: skipping (disabled)");
            } else if !is_remuxdb_kind {
                debug!(id = %effective_stream.id, kind = ?item.as_ref().map(|it| &it.kind), "remuxdb: skipping (not movie/episode)");
            } else if let Some(url) = self
                .ctx
                .config
                .remuxdb_url
                .clone()
            {
                match media_info_from_probe(&source, &effective_stream, item.as_ref()) {
                    Some(mi) => {
                        debug!(id = %effective_stream.id, url, "remuxdb: submitting mediainfo");
                        let token = probe_cfg
                            .remuxdb_token
                            .clone();
                        tokio::spawn(mi.submit(url, token));
                    }
                    None => {
                        debug!(id = %effective_stream.id, "remuxdb: skipping (no stream_info or missing required fields)");
                    }
                }
            }

            results.push(ProbeResult {
                source,
                stream,
                effective_stream,
            });
        }

        Ok(ProbedStreams {
            results,
            specific_requested: sel.specific_requested,
        })
    }

    /// Persist the resolved stream UUID in the device-preference store (24 h TTL).
    /// Also records the group→item association so /Items/{group_uuid} can redirect
    /// to the correct content item without a DB scan.
    /// No-op when this was not a group request.
    pub fn save_preference(&self, device_key: &str) {
        let Some((gid, _, _)) = &self.group else {
            return;
        };
        self.ctx
            .store
            .save(
                format!("pstream:{}:{}", self.item_id, device_key),
                self.candidate()
                    .id,
                std::time::Duration::from_secs(24 * 3600),
            );
        if let Some(uid) = self.user_id {
            Self::save_group_item(
                &self
                    .ctx
                    .store,
                uid,
                *gid,
                self.item_id,
            );
        }
    }

    /// Record that `group_id` (a stream group UUID) belongs to `item_id` for the given user.
    ///
    /// Keyed per-user to avoid collisions when the same global group appears across multiple
    /// media items. Used by `/Items/{group_uuid}` to redirect back to the owning content item.
    /// TTL is 7 days — long enough to survive normal browsing sessions.

    /// Resolve an "(auto)" entry to the live winner for a given item and resolution.
    pub async fn resolve_auto(
        ctx: &AppContext,
        item_id: uuid::Uuid,
        resolution: &str,
        user_id: Option<uuid::Uuid>,
    ) -> anyhow::Result<crate::db::Media> {
        let choice_key = Self::auto_choice_key(item_id, resolution);
        let mut parent = crate::db::Media::get_by_id(&ctx.db, &item_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("item not found: {}", item_id))?;
        if let Some(winner_id) = ctx
            .store
            .get::<Uuid>(&choice_key)
        {
            if let Some(winner) =
                crate::db::Media::get_by_id(&ctx.db, &winner_id).await?
            {
                if !Self::is_auto_candidate_failed(
                    &ctx.store, item_id, resolution, &winner,
                ) && winner
                    .stream_info
                    .as_ref()
                    .is_some_and(|info| info.is_p2p())
                    && !is_stereoscopic_auto_candidate(&winner)
                    && !is_blocked_auto_candidate(&winner)
                    && !is_language_mismatched_auto_candidate(
                        &winner,
                        parent
                            .original_language
                            .as_deref(),
                        &parent.title,
                    )
                {
                    tracing::info!(
                        item_id = %item_id,
                        resolution = %resolution,
                        winner = %winner.id,
                        "resolve_auto cached winner"
                    );
                    return Ok(winner);
                }
            }
            ctx.store
                .delete(&choice_key);
        }

        let _ = ctx
            .addons
            .refresh_streams(&mut parent, ctx, user_id)
            .await;
        let mut all = parent
            .streams(&ctx.db)
            .await?;
        let mut total_all = all.len();
        // Transient empty due to DB lock / refresh race — retry once after short delay
        if total_all == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(800)).await;
            let _ = ctx
                .addons
                .refresh_streams(&mut parent, ctx, user_id)
                .await;
            all = parent
                .streams(&ctx.db)
                .await?;
            total_all = all.len();
        }
        fn canonical(title: &str) -> String {
            let l = title.to_ascii_lowercase();
            if l.contains("2160p") || l.contains("4k") {
                "4K".to_string()
            } else if l.contains("1080p") {
                "1080p".to_string()
            } else if l.contains("720p") {
                "720p".to_string()
            } else if l.contains("480p") {
                "480p".to_string()
            } else if l.contains("1440p") {
                "1440p".to_string()
            } else {
                "".to_string()
            }
        }
        fn canonical_for_media(m: &crate::db::Media) -> String {
            let c = canonical(&m.title);
            if !c.is_empty() {
                return c;
            }
            if let Some(si) = &m.stream_info {
                if let Some(fname) = &si.filename {
                    let cf = canonical(fname);
                    if !cf.is_empty() {
                        return cf;
                    }
                }
            }
            "".to_string()
        }
        let mut candidates: Vec<crate::db::Media> = all
            .into_iter()
            .filter(|m| {
                m.stream_info
                    .as_ref()
                    .is_some_and(|info| info.is_p2p())
                    && canonical_for_media(m) == resolution
            })
            .collect();
        let blocked_candidates = candidates
            .iter()
            .filter(|candidate| is_blocked_auto_candidate(candidate))
            .count();
        if blocked_candidates > 0 {
            candidates.retain(|candidate| !is_blocked_auto_candidate(candidate));
            tracing::info!(
                item_id = %item_id,
                resolution = %resolution,
                excluded = blocked_candidates,
                remaining = candidates.len(),
                "resolve_auto excluded alternate-cut or capture candidates"
            );
        }
        let stereoscopic_candidates = candidates
            .iter()
            .filter(|candidate| is_stereoscopic_auto_candidate(candidate))
            .count();
        if stereoscopic_candidates > 0 {
            candidates.retain(|candidate| !is_stereoscopic_auto_candidate(candidate));
            tracing::info!(
                item_id = %item_id,
                resolution = %resolution,
                excluded = stereoscopic_candidates,
                remaining = candidates.len(),
                "resolve_auto excluded stereoscopic candidates"
            );
        }
        let language_mismatches = candidates
            .iter()
            .filter(|candidate| {
                is_language_mismatched_auto_candidate(
                    candidate,
                    parent
                        .original_language
                        .as_deref(),
                    &parent.title,
                )
            })
            .count();
        if language_mismatches > 0 && language_mismatches < candidates.len() {
            candidates.retain(|candidate| {
                !is_language_mismatched_auto_candidate(
                    candidate,
                    parent
                        .original_language
                        .as_deref(),
                    &parent.title,
                )
            });
            tracing::info!(
                item_id = %item_id,
                resolution = %resolution,
                desired_language = ?parent.original_language,
                excluded = language_mismatches,
                remaining = candidates.len(),
                "resolve_auto excluded candidates without desired audio language"
            );
        } else if language_mismatches == candidates.len() && language_mismatches > 0 {
            tracing::warn!(
                item_id = %item_id,
                resolution = %resolution,
                desired_language = ?parent.original_language,
                candidates = language_mismatches,
                "all Auto candidates appear language-mismatched; retaining them as fallback"
            );
        }
        if candidates.is_empty() {
            tracing::warn!(item_id=%item_id, resolution=%resolution, total=total_all, "resolve_auto no candidates");
            anyhow::bail!("no candidates for resolution {}", resolution);
        }
        let unfailed_candidates = candidates
            .iter()
            .filter(|candidate| {
                !Self::is_auto_candidate_failed(
                    &ctx.store, item_id, resolution, candidate,
                )
            })
            .count();
        if unfailed_candidates == 0 {
            tracing::warn!(
                item_id = %item_id,
                resolution = %resolution,
                failed = candidates.len(),
                "resolve_auto has no unfailed candidates"
            );
            anyhow::bail!(
                "all {} candidates for {} recently failed",
                candidates.len(),
                resolution
            );
        }
        if unfailed_candidates > 0 && unfailed_candidates < candidates.len() {
            let excluded = candidates.len() - unfailed_candidates;
            candidates.retain(|candidate| {
                !Self::is_auto_candidate_failed(
                    &ctx.store, item_id, resolution, candidate,
                )
            });
            tracing::info!(
                item_id = %item_id,
                resolution = %resolution,
                excluded,
                remaining = candidates.len(),
                "resolve_auto excluded recently failed candidates"
            );
        }
        candidates.sort_by_key(|candidate| {
            Reverse(
                candidate
                    .stream_info
                    .as_ref()
                    .and_then(|info| info.seeders)
                    .unwrap_or(0),
            )
        });
        tracing::info!(item_id=%item_id, resolution=%resolution, candidates=%candidates.len(), "resolve_auto candidates");

        // UDP tracker queries are the broad, cheap pass. They check dozens of
        // hashes with small datagrams and never add a torrent or allocate storage.
        let mut scrape_hashes = Vec::new();
        let mut seen_hashes = HashSet::new();
        for candidate in &candidates {
            let Some(hash) = candidate
                .stream_info
                .as_ref()
                .and_then(|info| {
                    info.descriptor
                        .torrent_info_hash()
                })
                .map(str::to_ascii_lowercase)
            else {
                continue;
            };
            if seen_hashes.insert(hash.clone()) {
                scrape_hashes.push(hash);
            }
            if scrape_hashes.len() >= 50 {
                break;
            }
        }

        let mut tracker_urls: Vec<String> = crate::stream::DEFAULT_TRACKERS
            .iter()
            .map(|tracker| (*tracker).to_string())
            .collect();
        let mut seen_trackers: HashSet<String> = tracker_urls
            .iter()
            .cloned()
            .collect();
        for candidate in &candidates {
            if let Some(trackers) = candidate
                .stream_info
                .as_ref()
                .and_then(|info| {
                    info.descriptor
                        .torrent_trackers()
                })
            {
                for tracker in trackers {
                    let tracker = tracker
                        .as_ref()
                        .to_string();
                    if seen_trackers.insert(tracker.clone()) {
                        tracker_urls.push(tracker);
                    }
                }
            }
        }

        // Metadata-only preflight is the narrower, stronger pass. It proves a
        // peer can actually answer and caches the metadata/peer addresses for
        // the eventual winner. `list_only` creates no files and downloads no pieces.
        let mut torrent_candidates: Vec<(Uuid, String, String, u64, bool, u8)> =
            candidates
                .iter()
                .filter_map(|candidate| {
                    let info = candidate
                        .stream_info
                        .as_ref()?;
                    let hash = info
                        .descriptor
                        .torrent_info_hash()?
                        .to_ascii_lowercase();
                    let magnet = info.torrent_magnet()?;
                    let provider_seeders = info
                        .seeders
                        .unwrap_or(0)
                        .max(0) as u64;
                    let managed_rank = ctx
                        .torrent
                        .managed_availability(&hash)
                        .map(|state| {
                            if state.finished {
                                2
                            } else if state.live_peers > 0 {
                                1
                            } else {
                                0
                            }
                        })
                        .unwrap_or(0);
                    Some((
                        candidate.id,
                        hash,
                        magnet,
                        provider_seeders,
                        candidate
                            .probe_data
                            .is_some(),
                        managed_rank,
                    ))
                })
                .collect();
        torrent_candidates
            .sort_by_key(|candidate| Reverse((candidate.5, candidate.4, candidate.3)));

        // Keep the cheap metadata wave diverse. Cached probe data is useful,
        // but sorting only by that signal can exclude a highly seeded small
        // release from the latency-sensitive path. Reserve one of four slots
        // for the strongest provider-seed signal and fill the rest normally.
        let mut first_wave: Vec<_> = torrent_candidates
            .iter()
            .take(3)
            .cloned()
            .collect();
        if let Some(provider_pick) = torrent_candidates
            .iter()
            .max_by_key(|candidate| candidate.3)
            .filter(|candidate| {
                !first_wave
                    .iter()
                    .any(|selected| selected.0 == candidate.0)
            })
        {
            first_wave.push(provider_pick.clone());
        }
        for candidate in &torrent_candidates {
            if first_wave.len() >= 4 {
                break;
            }
            if !first_wave
                .iter()
                .any(|selected| selected.0 == candidate.0)
            {
                first_wave.push(candidate.clone());
            }
        }
        let scrape_future = tokio::time::timeout(
            Duration::from_millis(2700),
            crate::torrent_availability::scrape_torrents(
                &ctx.store,
                &scrape_hashes,
                &tracker_urls,
            ),
        );
        let first_wave_future = stream::iter(first_wave)
            .map(|(candidate_id, _, magnet, _, _, _)| async move {
                (
                    candidate_id,
                    ctx.torrent
                        .preflight(&magnet, &[], Duration::from_millis(2800))
                        .await,
                )
            })
            .buffer_unordered(4)
            .collect::<Vec<_>>();
        let (scrape_result, first_wave_results) =
            tokio::join!(scrape_future, first_wave_future);
        let tracker_results = scrape_result.unwrap_or_else(|_| {
            debug!(item_id=%item_id, resolution=%resolution, "resolve_auto tracker scrape timed out");
            HashMap::new()
        });

        let mut preflight_results = HashMap::new();
        let mut checked = HashSet::new();
        for (candidate_id, result) in first_wave_results {
            checked.insert(candidate_id);
            match result {
                Ok(result) => {
                    preflight_results.insert(candidate_id, result);
                }
                Err(error) => {
                    debug!(candidate_id=%candidate_id, error=%error, "torrent auto preflight failed");
                }
            }
        }

        let confirmed_live = preflight_results
            .values()
            .any(|result| {
                result.seen_peers > 0
                    || result
                        .managed
                        .as_ref()
                        .map(|state| state.finished || state.live_peers > 0)
                        .unwrap_or(false)
            });
        if !confirmed_live {
            let has_announced_peers = torrent_candidates
                .iter()
                .any(|candidate| {
                    tracker_results
                        .get(&candidate.1)
                        .map(|availability| {
                            !availability
                                .peers
                                .is_empty()
                        })
                        .unwrap_or(false)
                });
            let mut second_wave: Vec<_> = torrent_candidates
                .iter()
                .cloned()
                .filter(|candidate| {
                    if has_announced_peers {
                        tracker_results
                            .get(&candidate.1)
                            .map(|availability| {
                                !availability
                                    .peers
                                    .is_empty()
                            })
                            .unwrap_or(false)
                    } else {
                        !checked.contains(&candidate.0)
                    }
                })
                .map(|candidate| {
                    let availability = tracker_results.get(&candidate.1);
                    let peers = availability
                        .map(|availability| {
                            availability
                                .peers
                                .clone()
                        })
                        .unwrap_or_default();
                    let tracker_seeders = availability
                        .map(|availability| availability.seeders)
                        .unwrap_or(0);
                    let peer_count = peers.len();
                    (candidate, peers, peer_count, tracker_seeders)
                })
                .collect();
            second_wave.sort_by_key(|(candidate, _, peer_count, tracker_seeders)| {
                Reverse((*peer_count, *tracker_seeders, candidate.3))
            });
            let second_wave_limit = if has_announced_peers { 8 } else { 4 };
            let second_wave_budget = if has_announced_peers { 3800 } else { 2200 };
            let second_wave_future = stream::iter(
                second_wave
                    .into_iter()
                    .take(second_wave_limit),
            )
            .map(
                |((candidate_id, _, magnet, _, _, _), peers, _, _)| async move {
                    (
                        candidate_id,
                        ctx.torrent
                            .preflight(
                                &magnet,
                                &peers,
                                Duration::from_millis(second_wave_budget),
                            )
                            .await,
                    )
                },
            )
            .buffer_unordered(second_wave_limit)
            .collect::<Vec<_>>();
            for (candidate_id, result) in second_wave_future.await {
                match result {
                    Ok(result) => {
                        preflight_results.insert(candidate_id, result);
                    }
                    Err(error) => {
                        debug!(candidate_id=%candidate_id, error=%error, "torrent auto second-wave preflight failed");
                    }
                }
            }
        }

        // Tracker counts can be stale and metadata preflight counts every
        // discovered address. For only the strongest few candidates, perform
        // a tiny wire-protocol check against tracker-returned peers. This reads
        // handshakes/bitfields only: no torrent is added and no piece is fetched.
        let mut peer_probe_candidates: Vec<_> = torrent_candidates
            .iter()
            .take(5)
            .collect();
        if let Some(provider_pick) = torrent_candidates
            .iter()
            .max_by_key(|candidate| candidate.3)
            .filter(|candidate| {
                !peer_probe_candidates
                    .iter()
                    .any(|selected| selected.0 == candidate.0)
            })
        {
            peer_probe_candidates.push(provider_pick);
        }
        let peer_probe_jobs: Vec<_> = peer_probe_candidates
            .into_iter()
            .filter_map(|candidate| {
                let mut peers = preflight_results
                    .get(&candidate.0)
                    .map(|result| {
                        result
                            .peers
                            .clone()
                    })
                    .unwrap_or_default();
                if let Some(availability) = tracker_results.get(&candidate.1) {
                    let mut seen: HashSet<_> = peers
                        .iter()
                        .copied()
                        .collect();
                    for peer in &availability.peers {
                        if seen.insert(*peer) {
                            peers.push(*peer);
                        }
                    }
                }
                (!peers.is_empty()).then(|| {
                    (
                        candidate.0,
                        candidate
                            .1
                            .clone(),
                        peers,
                    )
                })
            })
            .take(6)
            .collect();
        let peer_probe_limit = peer_probe_jobs
            .len()
            .max(1);
        let peer_probe_results: HashMap<_, _> = stream::iter(peer_probe_jobs)
            .map(|(candidate_id, hash, peers)| async move {
                (
                    candidate_id,
                    crate::torrent_availability::probe_active_peers(
                        &ctx.store, &hash, &peers,
                    )
                    .await,
                )
            })
            .buffer_unordered(peer_probe_limit)
            .collect()
            .await;

        // Reuse the exact peers that just answered our live handshake. Without
        // this handoff the real torrent add starts from a broad stale peer list,
        // so a good availability decision can still spend several seconds
        // rediscovering the same working seeders.
        for candidate in &torrent_candidates {
            if let Some(availability) = peer_probe_results.get(&candidate.0) {
                ctx.torrent
                    .prioritize_preflight_peers(&candidate.1, &availability.peers);
            }
        }

        let candidate_file_bytes = |candidate: &db::Media| {
            known_candidate_file_bytes(candidate, preflight_results.get(&candidate.id))
        };
        // Release size is a quality/required-bitrate prior, independent of
        // which few swarms happened to answer metadata preflight. Basing the
        // target only on confirmed candidates lets one large outlier become
        // its own benchmark. Use the full same-resolution population and
        // deduplicate repeated addon entries for the same torrent file.
        let mut seen_release_sizes: HashSet<(String, u64)> = HashSet::new();
        let population_sizes: Vec<u64> = candidates
            .iter()
            .filter_map(|candidate| {
                let size = candidate_file_bytes(candidate)?;
                let release = candidate
                    .stream_info
                    .as_ref()
                    .and_then(|info| {
                        info.descriptor
                            .torrent_info_hash()
                    })
                    .map(str::to_ascii_lowercase)
                    .unwrap_or_else(|| {
                        candidate
                            .id
                            .to_string()
                    });
                seen_release_sizes
                    .insert((release, size))
                    .then_some(size)
            })
            .collect();
        let target_sample_size = population_sizes.len();
        let target_file_bytes = lower_median(population_sizes);

        let mut ranked_candidates: Vec<_> = candidates
            .into_iter()
            .map(|candidate| {
                let tracker = candidate
                    .stream_info
                    .as_ref()
                    .and_then(|info| {
                        info.descriptor
                            .torrent_info_hash()
                    })
                    .and_then(|hash| tracker_results.get(&hash.to_ascii_lowercase()));
                let score = auto_candidate_score(
                    &candidate,
                    preflight_results.get(&candidate.id),
                    tracker,
                    peer_probe_results.get(&candidate.id),
                    target_file_bytes,
                );
                (candidate, score)
            })
            .collect();
        ranked_candidates.sort_by(|left, right| {
            right
                .1
                .cmp(&left.1)
        });
        let winner = ranked_candidates
            .first()
            .map(|(candidate, _)| candidate.clone())
            .expect("Auto candidates were checked as non-empty");
        let winner_hash = winner
            .stream_info
            .as_ref()
            .and_then(|info| {
                info.descriptor
                    .torrent_info_hash()
            })
            .map(str::to_ascii_lowercase);
        // Addon results can contain duplicate rows for the same info hash. Keep
        // a short ranked list of genuinely independent swarms so startup can
        // advance in bounded two-candidate waves without rerunning selection.
        let mut seen_ranked =
            HashSet::from([Self::auto_candidate_failure_token(&winner)]);
        let ranked_fallbacks: Vec<_> = ranked_candidates
            .iter()
            .skip(1)
            .filter_map(|(candidate, _)| {
                seen_ranked
                    .insert(Self::auto_candidate_failure_token(candidate))
                    .then(|| candidate.clone())
            })
            .take(Self::AUTO_SESSION_CANDIDATE_LIMIT.saturating_sub(1))
            .collect();
        let tracker_seeders = winner_hash
            .as_ref()
            .and_then(|hash| tracker_results.get(hash))
            .map(|availability| availability.seeders)
            .unwrap_or(0);
        let announced_peers = winner_hash
            .as_ref()
            .and_then(|hash| tracker_results.get(hash))
            .map(|availability| {
                availability
                    .peers
                    .len()
            })
            .unwrap_or(0);
        let preflight = preflight_results.get(&winner.id);
        let provider_seeders = winner
            .stream_info
            .as_ref()
            .and_then(|info| info.seeders)
            .unwrap_or(0);
        let live_peers = preflight
            .and_then(|result| {
                result
                    .managed
                    .as_ref()
            })
            .map(|state| state.live_peers)
            .unwrap_or(0);
        let active_peer_probe = peer_probe_results.get(&winner.id);
        Self::remember_auto_choice(&ctx.store, item_id, resolution, winner.id);
        ctx.store
            .save(
                Self::auto_ranked_choice_key(item_id, resolution),
                AutoRankedChoice {
                    winner_id: winner.id,
                    fallback_ids: ranked_fallbacks
                        .iter()
                        .map(|candidate| candidate.id)
                        .collect(),
                },
                Duration::from_secs(5 * 60),
            );
        tracing::info!(
            item_id = %item_id,
            resolution = %resolution,
            winner = %winner.id,
            title = %winner.title,
            provider_seeders,
            tracker_seeders,
            announced_peers,
            preflight_peers = preflight.map(|result| result.seen_peers).unwrap_or(0),
            preflight_ms = preflight.map(|result| result.elapsed_ms).unwrap_or(0),
            preflight_cached = preflight.map(|result| result.from_cache).unwrap_or(false),
            live_peers,
            responsive_peers = active_peer_probe.map(|result| result.responsive).unwrap_or(0),
            active_seeders = active_peer_probe.map(|result| result.seeders).unwrap_or(0),
            startup_compatibility = startup_compatibility_score(&winner),
            selected_file_bytes = candidate_file_bytes(&winner),
            target_file_bytes,
            target_sample_size,
            fallback_ids = ?ranked_fallbacks.iter().map(|candidate| candidate.id).collect::<Vec<_>>(),
            "resolve_auto winner"
        );
        Ok(winner)
    }

    pub fn save_group_item(
        store: &remux_utils::Store,
        user_id: Uuid,
        group_id: Uuid,
        item_id: Uuid,
    ) {
        store.save(
            format!("gitem:{}:{}", user_id, group_id),
            item_id,
            std::time::Duration::from_secs(7 * 24 * 3600),
        );
    }

    /// Look up the content item that owns `group_id` for `user_id`.
    ///
    /// Returns `None` when the user has not yet browsed an item that carries this stream group,
    /// or the mapping has expired. Callers should surface a 404 in that case.
    pub fn get_group_item(
        store: &remux_utils::Store,
        user_id: Uuid,
        group_id: Uuid,
    ) -> Option<Uuid> {
        store
            .get::<Uuid>(format!("gitem:{}:{}", user_id, group_id))
            .map(|id| *id)
    }
}

/// Result of `StreamService::select_streams` — partitioned candidate/probe lists and flags.
pub(crate) struct StreamSelection {
    /// Streams to present to the client and probe.
    pub candidates: Vec<db::Media>,
    /// Full pool used for probe-fallback across sibling streams.
    pub probe_pool: Vec<db::Media>,
    /// When false (group requests), cross-resolution fallback is intentional.
    pub restrict_resolution: bool,
    /// Probe only the first candidate to avoid N parallel FFmpeg processes.
    pub probe_only_first: bool,
    /// True when the client named a specific stream — keep its UUID, don't override to item_id.
    pub specific_requested: bool,
}

fn media_info_from_probe(
    probe: &api::MediaSourceInfo,
    stream: &db::Media,
    item: Option<&db::Media>,
) -> Option<remuxdb::MediaInfoPayload> {
    let (info_hash, file_idx, nzb, filename) = match stream
        .stream_info
        .as_ref()
    {
        Some(si) => {
            let (hash, idx) = match &si.descriptor {
                StreamDescriptor::Torrent {
                    info_hash,
                    file_idx,
                    ..
                } => (Some(info_hash.clone()), file_idx.map(|i| i as i32)),
                _ => (None, None),
            };
            let nzb = si
                .usenet_guid
                .as_ref()
                .zip(
                    si.usenet_indexer
                        .as_ref(),
                )
                .map(|(guid, indexer)| remuxdb::NzbSubmission {
                    indexer: indexer.clone(),
                    indexer_guid: guid.clone(),
                    title: si
                        .filename
                        .clone(),
                });
            (
                hash,
                idx,
                nzb,
                si.filename
                    .clone()
                    .unwrap_or_else(|| {
                        stream
                            .title
                            .clone()
                    }),
            )
        }
        None => (
            None,
            None,
            None,
            stream
                .title
                .clone(),
        ),
    };

    if info_hash.is_none() && nzb.is_none() {
        return None;
    }

    let (kind, external_ids, season, episode) = if let Some(item) = item {
        let kind = match item.kind {
            db::MediaKind::Episode => "episode",
            _ => "movie",
        }
        .to_string();
        let imdb_id = item
            .external_ids
            .imdb
            .as_ref()
            .map(|v| v.to_string())
            .or_else(|| {
                item.grandparent
                    .as_deref()
                    .and_then(|gp| {
                        gp.external_ids
                            .imdb
                            .as_ref()
                    })
                    .map(|v| v.to_string())
            });
        let ids = (imdb_id.is_some()
            || item
                .external_ids
                .tmdb
                .is_some()
            || item
                .external_ids
                .tvdb
                .is_some()
            || item
                .external_ids
                .kitsu
                .is_some())
        .then(|| remuxdb::ExternalIds {
            imdb_id,
            tmdb_id: item
                .external_ids
                .tmdb,
            tvdb_id: item
                .external_ids
                .tvdb,
            kitsu_id: item
                .external_ids
                .kitsu,
        });
        let season = if item.kind == db::MediaKind::Episode {
            item.parent_idx
                .map(|v| v as i32)
        } else {
            None
        };
        let episode = if item.kind == db::MediaKind::Episode {
            item.idx
                .map(|v| v as i32)
        } else {
            None
        };
        (kind, ids, season, episode)
    } else {
        ("movie".to_string(), None, None, None)
    };

    let tracks = probe
        .media_streams
        .iter()
        .filter_map(|ms| remuxdb::TrackPayload::try_from(ms).ok())
        .collect();

    Some(remuxdb::MediaInfoPayload {
        client_id: Some(crate::common::server_id()),
        kind,
        filename,
        torrent_info_hash: info_hash,
        torrent_file_idx: file_idx,
        nzb,
        container: probe
            .container
            .clone()
            .unwrap_or_default(),
        size: probe
            .size
            .or_else(|| {
                stream
                    .stream_info
                    .as_ref()
                    .and_then(|si| si.size)
            })
            .filter(|&s| s > 0)?,
        duration: crate::common::ticks_to_seconds(
            probe
                .run_time_ticks
                .unwrap_or(0),
        ),
        bitrate: probe.bitrate,
        season,
        episode,
        external_ids,
        tracks,
    })
}
