CREATE TABLE IF NOT EXISTS playback_startup_metrics (
    play_session_id             TEXT PRIMARY KEY,
    item_id                     TEXT NOT NULL,
    media_source_id             TEXT,
    user_id                     TEXT NOT NULL,
    device_id                   TEXT NOT NULL,
    client_name                 TEXT NOT NULL,
    outcome                     TEXT NOT NULL,
    started_at                  TIMESTAMP NOT NULL,
    playback_info_ms            INTEGER,
    hls_requested_ms            INTEGER,
    source_selected_ms          INTEGER,
    transcode_started_ms        INTEGER,
    first_segment_served_ms     INTEGER,
    first_progress_ms           INTEGER,
    estimated_actual_playback_ms INTEGER,
    confirmation_delay_ms       INTEGER,
    failure_reason              TEXT,
    created_at                  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_playback_startup_metrics_started
    ON playback_startup_metrics(started_at DESC);

CREATE INDEX IF NOT EXISTS idx_playback_startup_metrics_item
    ON playback_startup_metrics(item_id, started_at DESC);
