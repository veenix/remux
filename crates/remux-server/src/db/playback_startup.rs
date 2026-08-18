use anyhow::Result;
use sqlx::SqlitePool;

use crate::playback_session::PlaybackStartupReport;

pub async fn record_playback_startup(
    db: &SqlitePool,
    report: &PlaybackStartupReport,
) -> Result<()> {
    sqlx::query(
        "INSERT OR REPLACE INTO playback_startup_metrics (\
             play_session_id, item_id, media_source_id, user_id, device_id, client_name, \
             outcome, started_at, playback_info_ms, hls_requested_ms, source_selected_ms, \
             transcode_started_ms, first_segment_served_ms, first_progress_ms, \
             estimated_actual_playback_ms, confirmation_delay_ms, failure_reason\
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&report.play_session_id)
    .bind(report.item_id.to_string())
    .bind(report.media_source_id.map(|id| id.to_string()))
    .bind(report.user_id.to_string())
    .bind(&report.device_id)
    .bind(&report.client_name)
    .bind(&report.outcome)
    .bind(report.started_at)
    .bind(report.playback_info_ms)
    .bind(report.hls_requested_ms)
    .bind(report.source_selected_ms)
    .bind(report.transcode_started_ms)
    .bind(report.first_segment_served_ms)
    .bind(report.first_progress_ms)
    .bind(report.estimated_actual_playback_ms)
    .bind(report.confirmation_delay_ms)
    .bind(&report.failure_reason)
    .execute(db)
    .await?;
    Ok(())
}
