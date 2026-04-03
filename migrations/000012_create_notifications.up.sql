CREATE TABLE IF NOT EXISTS notifications (
    id           BIGSERIAL PRIMARY KEY,
    user_address VARCHAR(42),
    type         VARCHAR(30) NOT NULL,
    channel      VARCHAR(20) NOT NULL,
    title        VARCHAR(200) NOT NULL,
    content      TEXT NOT NULL,
    data         JSONB,
    status       VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sent_at      TIMESTAMPTZ
);

CREATE INDEX idx_notifications_user ON notifications(user_address);
CREATE INDEX idx_notifications_status ON notifications(status);
CREATE INDEX idx_notifications_time ON notifications(created_at DESC);

CREATE TABLE IF NOT EXISTS notification_preferences (
    user_address              VARCHAR(42) PRIMARY KEY,
    enabled_channels          TEXT[] NOT NULL DEFAULT ARRAY['websocket'],
    health_factor_threshold   NUMERIC(10,4) NOT NULL DEFAULT 1.3,
    created_at                TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
