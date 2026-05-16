-- Tables owned by the agent example. Sayiir manages its own snapshot tables.

CREATE TABLE IF NOT EXISTS agent_drafts (
  instance_id TEXT PRIMARY KEY,
  topic       TEXT NOT NULL,
  body        TEXT NOT NULL,
  created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS agent_reports (
  instance_id  TEXT PRIMARY KEY,
  topic        TEXT NOT NULL,
  body         TEXT NOT NULL,
  approved_by  TEXT,
  published_at TEXT NOT NULL DEFAULT (datetime('now'))
);
