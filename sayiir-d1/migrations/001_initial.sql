-- Workflow snapshots: current state of each workflow instance (1:1 with instance).
CREATE TABLE IF NOT EXISTS sayiir_workflow_snapshots (
    instance_id          TEXT    PRIMARY KEY,
    status               TEXT    NOT NULL,
    definition_hash      TEXT,
    current_task_id      TEXT,
    completed_task_count INTEGER NOT NULL DEFAULT 0,
    data                 BLOB    NOT NULL,
    error                TEXT,
    position_kind        TEXT,
    delay_wake_at        TEXT,
    trace_parent         TEXT,
    awaited_signal_name  TEXT,
    started_at           TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    completed_at         TEXT,
    updated_at           TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE INDEX IF NOT EXISTS idx_snapshots_status   ON sayiir_workflow_snapshots (status);
CREATE INDEX IF NOT EXISTS idx_snapshots_task     ON sayiir_workflow_snapshots (current_task_id);
CREATE INDEX IF NOT EXISTS idx_snapshots_updated  ON sayiir_workflow_snapshots (updated_at);
CREATE INDEX IF NOT EXISTS idx_snapshots_position ON sayiir_workflow_snapshots (position_kind);
CREATE INDEX IF NOT EXISTS idx_snapshots_awaited_signal
    ON sayiir_workflow_snapshots (awaited_signal_name)
    WHERE awaited_signal_name IS NOT NULL;

-- Snapshot history: append-only log of every checkpoint (1:N per instance).
CREATE TABLE IF NOT EXISTS sayiir_workflow_snapshot_history (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id     TEXT    NOT NULL,
    version         INTEGER NOT NULL,
    status          TEXT    NOT NULL,
    current_task_id TEXT,
    data            BLOB    NOT NULL,
    created_at      TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_history_instance ON sayiir_workflow_snapshot_history (instance_id, version);

-- Cancel/pause signals (at most one per kind per instance).
CREATE TABLE IF NOT EXISTS sayiir_workflow_signals (
    instance_id  TEXT NOT NULL,
    kind         TEXT NOT NULL,
    reason       TEXT,
    requested_by TEXT,
    created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (instance_id, kind)
);

-- External events (signals) buffered per (instance_id, signal_name) in FIFO order.
CREATE TABLE IF NOT EXISTS sayiir_workflow_events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id  TEXT    NOT NULL,
    signal_name  TEXT    NOT NULL,
    payload      BLOB    NOT NULL,
    created_at   TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE INDEX IF NOT EXISTS idx_events_instance_signal
    ON sayiir_workflow_events (instance_id, signal_name, id);
