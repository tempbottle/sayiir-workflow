-- Application schema. Sayiir's own snapshot/signal tables are auto-created
-- on first `Engine.create()` and live in the same D1 database.

-- Source documents (one row per ingested URL).
CREATE TABLE IF NOT EXISTS docs (
  id           TEXT PRIMARY KEY,
  url          TEXT NOT NULL UNIQUE,
  title        TEXT,
  fetched_at   TEXT NOT NULL DEFAULT (datetime('now')),
  content_type TEXT,
  raw_r2_key   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS docs_fetched_at ON docs(fetched_at);

-- Chunks of each doc. Vectorize stores embeddings keyed by `id`.
CREATE TABLE IF NOT EXISTS chunks (
  id          TEXT PRIMARY KEY,
  doc_id      TEXT NOT NULL REFERENCES docs(id) ON DELETE CASCADE,
  ordinal     INTEGER NOT NULL,
  text        TEXT NOT NULL,
  byte_start  INTEGER NOT NULL,
  byte_end    INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS chunks_doc_id ON chunks(doc_id);

-- FTS5 index for the keyword branch of hybrid retrieval.
CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(
  text, content='chunks', content_rowid='rowid'
);
CREATE TRIGGER IF NOT EXISTS chunks_ai AFTER INSERT ON chunks BEGIN
  INSERT INTO chunks_fts(rowid, text) VALUES (new.rowid, new.text);
END;
CREATE TRIGGER IF NOT EXISTS chunks_ad AFTER DELETE ON chunks BEGIN
  INSERT INTO chunks_fts(chunks_fts, rowid, text) VALUES ('delete', old.rowid, old.text);
END;

-- Conversation history.
CREATE TABLE IF NOT EXISTS conversations (
  id         TEXT PRIMARY KEY,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS messages (
  id              TEXT PRIMARY KEY,
  conversation_id TEXT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
  role            TEXT NOT NULL CHECK (role IN ('user','assistant')),
  content         TEXT NOT NULL,
  citations_json  TEXT,
  confidence      REAL,
  low_confidence  INTEGER NOT NULL DEFAULT 0,
  created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS messages_conv ON messages(conversation_id, created_at);
