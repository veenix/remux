use axum::{
    extract::{Path, Query, State},
    response::IntoResponse,
};
use remux_macros::get;
use uuid::Uuid;

use crate::{OptionExt, ResultExt};
use axum_anyhow::ApiResult as Result;

use crate::{
    AppState, db,
    stream::{StreamDescriptor, TorrentSource},
};

/// Proxy any stream stored in `db::Media.stream_info` to the caller.
///
/// Handles all URL schemes transparently via [`crate::stream::StreamSource`].
/// Addon-owned streams (`Opendal`) are dispatched to the addon's `serve_stream`.
/// Auth is not required — stream UUIDs are stable but not guessable.
#[get("/stream/{id}")]
pub async fn stream_proxy(
    headers: axum::http::HeaderMap,
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Query(query): Query<crate::api::VideoStreamQuery>,
) -> Result<impl IntoResponse> {
    let media = db::Media::get_by_id(
        &state
            .ctx
            .db,
        &id,
    )
    .await?
    .context_not_found("not found")?;

    let stream_info = media
        .stream_info
        .context_not_found("media has no URL")?;
    let fallback_file_hint = stream_info
        .filename
        .clone();
    let descriptor = stream_info.descriptor;

    if let Some(addon_id) = descriptor.addon_id() {
        let addon = state
            .ctx
            .addons
            .get(addon_id)
            .context_not_found("addon not found")?;
        let stream = addon
            .stream
            .as_ref()
            .context_not_found("addon does not support streams")?;
        return stream
            .serve_stream(&descriptor, &headers)
            .await;
    }

    if let StreamDescriptor::Torrent {
        info_hash,
        file_hint,
        file_idx,
        trackers,
    } = &descriptor
    {
        let cfg = db::Settings::get_config_or_default(
            &state
                .ctx
                .db,
        )
        .await;
        if !cfg
            .p2p_enabled
            .unwrap_or(true)
        {
            return Err(anyhow::anyhow!("P2P disabled")).context_bad_request(
                "P2P streams are disabled by the server administrator",
            );
        }
        let playback_ids = state
            .ctx
            .sessions
            .playback_ids_for_stream(
                id,
                query
                    .play_session_id
                    .as_deref(),
            )
            .await;
        return TorrentSource {
            info_hash: info_hash.clone(),
            file_hint: file_hint
                .clone()
                .or(fallback_file_hint),
            file_idx: *file_idx,
            trackers: trackers.clone(),
        }
        .serve_for_playback(&state, &headers, &playback_ids)
        .await;
    }

    descriptor
        .into_source()
        .serve(&state, &headers)
        .await
}
