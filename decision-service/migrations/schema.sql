CREATE TABLE IF NOT EXISTS decisions (
    event_id   TEXT PRIMARY KEY,
    user_id    TEXT NOT NULL,
    amount     NUMERIC(12, 2) NOT NULL,
    currency   TEXT NOT NULL,
    decision   TEXT NOT NULL,
    reason     TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
