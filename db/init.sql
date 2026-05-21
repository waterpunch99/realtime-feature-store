CREATE TABLE IF NOT EXISTS event_quality_log (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT,
    event_type TEXT,
    user_id TEXT,
    reason_code TEXT NOT NULL,
    reason_detail TEXT,
    raw_event JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT event_quality_log_event_type_chk
        CHECK (
            event_type IS NULL
            OR event_type IN ('view', 'click', 'add_to_cart', 'purchase', 'search')
        )
);

CREATE INDEX IF NOT EXISTS idx_event_quality_log_event_id
    ON event_quality_log (event_id);

CREATE INDEX IF NOT EXISTS idx_event_quality_log_created_at
    ON event_quality_log (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_event_quality_log_reason_code
    ON event_quality_log (reason_code);

CREATE TABLE IF NOT EXISTS late_event_log (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL,
    event_type TEXT,
    user_id TEXT,
    event_time TIMESTAMPTZ NOT NULL,
    ingest_time TIMESTAMPTZ NOT NULL,
    lateness_seconds INTEGER NOT NULL,
    reason_code TEXT NOT NULL,
    raw_event JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT late_event_log_event_type_chk
        CHECK (
            event_type IS NULL
            OR event_type IN ('view', 'click', 'add_to_cart', 'purchase', 'search')
        ),
    CONSTRAINT late_event_log_lateness_seconds_chk
        CHECK (lateness_seconds >= 0)
);

CREATE INDEX IF NOT EXISTS idx_late_event_log_event_id
    ON late_event_log (event_id);

CREATE INDEX IF NOT EXISTS idx_late_event_log_event_time
    ON late_event_log (event_time DESC);

CREATE INDEX IF NOT EXISTS idx_late_event_log_created_at
    ON late_event_log (created_at DESC);

CREATE TABLE IF NOT EXISTS feature_user_latest (
    user_id TEXT PRIMARY KEY,
    user_recent_click_categories_10m JSONB NOT NULL DEFAULT '[]'::jsonb,
    user_recent_click_categories_1h JSONB NOT NULL DEFAULT '[]'::jsonb,
    user_click_count_10m BIGINT NOT NULL DEFAULT 0,
    user_click_count_1h BIGINT NOT NULL DEFAULT 0,
    user_view_count_10m BIGINT NOT NULL DEFAULT 0,
    user_add_to_cart_count_1h BIGINT NOT NULL DEFAULT 0,
    user_purchase_count_1h BIGINT NOT NULL DEFAULT 0,
    user_purchase_amount_1h NUMERIC(18, 2) NOT NULL DEFAULT 0,
    user_avg_purchase_amount_24h NUMERIC(18, 2),
    user_last_event_time TIMESTAMPTZ,
    user_last_product_id TEXT,
    features JSONB NOT NULL DEFAULT '{}'::jsonb,
    window_updated_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT feature_user_latest_counts_chk
        CHECK (
            user_click_count_10m >= 0
            AND user_click_count_1h >= 0
            AND user_view_count_10m >= 0
            AND user_add_to_cart_count_1h >= 0
            AND user_purchase_count_1h >= 0
            AND user_purchase_amount_1h >= 0
            AND (
                user_avg_purchase_amount_24h IS NULL
                OR user_avg_purchase_amount_24h >= 0
            )
        )
);

CREATE INDEX IF NOT EXISTS idx_feature_user_latest_updated_at
    ON feature_user_latest (updated_at DESC);

CREATE TABLE IF NOT EXISTS feature_product_latest (
    product_id TEXT PRIMARY KEY,
    category_id TEXT,
    product_view_count_10m BIGINT NOT NULL DEFAULT 0,
    product_click_count_10m BIGINT NOT NULL DEFAULT 0,
    product_add_to_cart_count_10m BIGINT NOT NULL DEFAULT 0,
    product_purchase_count_1h BIGINT NOT NULL DEFAULT 0,
    product_ctr_10m NUMERIC(18, 6) NOT NULL DEFAULT 0,
    product_conversion_rate_1h NUMERIC(18, 6) NOT NULL DEFAULT 0,
    product_popularity_score_10m NUMERIC(18, 2) NOT NULL DEFAULT 0,
    features JSONB NOT NULL DEFAULT '{}'::jsonb,
    window_updated_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT feature_product_latest_metrics_chk
        CHECK (
            product_view_count_10m >= 0
            AND product_click_count_10m >= 0
            AND product_add_to_cart_count_10m >= 0
            AND product_purchase_count_1h >= 0
            AND product_ctr_10m >= 0
            AND product_conversion_rate_1h >= 0
            AND product_popularity_score_10m >= 0
        )
);

CREATE INDEX IF NOT EXISTS idx_feature_product_latest_category_id
    ON feature_product_latest (category_id);

CREATE INDEX IF NOT EXISTS idx_feature_product_latest_popularity_10m
    ON feature_product_latest (product_popularity_score_10m DESC);

CREATE INDEX IF NOT EXISTS idx_feature_product_latest_updated_at
    ON feature_product_latest (updated_at DESC);

CREATE TABLE IF NOT EXISTS feature_category_latest (
    category_id TEXT PRIMARY KEY,
    category_view_count_10m BIGINT NOT NULL DEFAULT 0,
    category_click_count_10m BIGINT NOT NULL DEFAULT 0,
    category_add_to_cart_count_1h BIGINT NOT NULL DEFAULT 0,
    category_purchase_count_1h BIGINT NOT NULL DEFAULT 0,
    category_purchase_amount_1h NUMERIC(18, 2) NOT NULL DEFAULT 0,
    category_popularity_score_10m NUMERIC(18, 2) NOT NULL DEFAULT 0,
    features JSONB NOT NULL DEFAULT '{}'::jsonb,
    window_updated_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT feature_category_latest_metrics_chk
        CHECK (
            category_view_count_10m >= 0
            AND category_click_count_10m >= 0
            AND category_add_to_cart_count_1h >= 0
            AND category_purchase_count_1h >= 0
            AND category_purchase_amount_1h >= 0
            AND category_popularity_score_10m >= 0
        )
);

CREATE INDEX IF NOT EXISTS idx_feature_category_latest_popularity_10m
    ON feature_category_latest (category_popularity_score_10m DESC);

CREATE INDEX IF NOT EXISTS idx_feature_category_latest_updated_at
    ON feature_category_latest (updated_at DESC);

CREATE TABLE IF NOT EXISTS feature_user_history (
    id BIGSERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    window_size TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    user_recent_click_categories JSONB NOT NULL DEFAULT '[]'::jsonb,
    user_click_count BIGINT NOT NULL DEFAULT 0,
    user_view_count BIGINT NOT NULL DEFAULT 0,
    user_add_to_cart_count BIGINT NOT NULL DEFAULT 0,
    user_purchase_count BIGINT NOT NULL DEFAULT 0,
    user_purchase_amount NUMERIC(18, 2) NOT NULL DEFAULT 0,
    user_avg_purchase_amount NUMERIC(18, 2),
    user_last_event_time TIMESTAMPTZ,
    user_last_product_id TEXT,
    features JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT feature_user_history_window_size_chk
        CHECK (window_size IN ('10m', '1h', '24h')),
    CONSTRAINT feature_user_history_window_range_chk
        CHECK (window_start < window_end),
    CONSTRAINT feature_user_history_counts_chk
        CHECK (
            user_click_count >= 0
            AND user_view_count >= 0
            AND user_add_to_cart_count >= 0
            AND user_purchase_count >= 0
            AND user_purchase_amount >= 0
            AND (
                user_avg_purchase_amount IS NULL
                OR user_avg_purchase_amount >= 0
            )
        ),
    CONSTRAINT uq_feature_user_history_window
        UNIQUE (user_id, window_size, window_start, window_end)
);

CREATE INDEX IF NOT EXISTS idx_feature_user_history_user_window_end
    ON feature_user_history (user_id, window_end DESC);

CREATE INDEX IF NOT EXISTS idx_feature_user_history_window
    ON feature_user_history (window_size, window_end DESC);

CREATE TABLE IF NOT EXISTS feature_product_history (
    id BIGSERIAL PRIMARY KEY,
    product_id TEXT NOT NULL,
    category_id TEXT,
    window_size TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    product_view_count BIGINT NOT NULL DEFAULT 0,
    product_click_count BIGINT NOT NULL DEFAULT 0,
    product_add_to_cart_count BIGINT NOT NULL DEFAULT 0,
    product_purchase_count BIGINT NOT NULL DEFAULT 0,
    product_ctr NUMERIC(18, 6) NOT NULL DEFAULT 0,
    product_conversion_rate NUMERIC(18, 6) NOT NULL DEFAULT 0,
    product_popularity_score NUMERIC(18, 2) NOT NULL DEFAULT 0,
    features JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT feature_product_history_window_size_chk
        CHECK (window_size IN ('10m', '1h', '24h')),
    CONSTRAINT feature_product_history_window_range_chk
        CHECK (window_start < window_end),
    CONSTRAINT feature_product_history_metrics_chk
        CHECK (
            product_view_count >= 0
            AND product_click_count >= 0
            AND product_add_to_cart_count >= 0
            AND product_purchase_count >= 0
            AND product_ctr >= 0
            AND product_conversion_rate >= 0
            AND product_popularity_score >= 0
        ),
    CONSTRAINT uq_feature_product_history_window
        UNIQUE (product_id, window_size, window_start, window_end)
);

CREATE INDEX IF NOT EXISTS idx_feature_product_history_product_window_end
    ON feature_product_history (product_id, window_end DESC);

CREATE INDEX IF NOT EXISTS idx_feature_product_history_category_window_end
    ON feature_product_history (category_id, window_end DESC);

CREATE INDEX IF NOT EXISTS idx_feature_product_history_popularity
    ON feature_product_history (window_size, window_end DESC, product_popularity_score DESC);

CREATE TABLE IF NOT EXISTS feature_category_history (
    id BIGSERIAL PRIMARY KEY,
    category_id TEXT NOT NULL,
    window_size TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    category_view_count BIGINT NOT NULL DEFAULT 0,
    category_click_count BIGINT NOT NULL DEFAULT 0,
    category_add_to_cart_count BIGINT NOT NULL DEFAULT 0,
    category_purchase_count BIGINT NOT NULL DEFAULT 0,
    category_purchase_amount NUMERIC(18, 2) NOT NULL DEFAULT 0,
    category_popularity_score NUMERIC(18, 2) NOT NULL DEFAULT 0,
    features JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT feature_category_history_window_size_chk
        CHECK (window_size IN ('10m', '1h', '24h')),
    CONSTRAINT feature_category_history_window_range_chk
        CHECK (window_start < window_end),
    CONSTRAINT feature_category_history_metrics_chk
        CHECK (
            category_view_count >= 0
            AND category_click_count >= 0
            AND category_add_to_cart_count >= 0
            AND category_purchase_count >= 0
            AND category_purchase_amount >= 0
            AND category_popularity_score >= 0
        ),
    CONSTRAINT uq_feature_category_history_window
        UNIQUE (category_id, window_size, window_start, window_end)
);

CREATE INDEX IF NOT EXISTS idx_feature_category_history_category_window_end
    ON feature_category_history (category_id, window_end DESC);

CREATE INDEX IF NOT EXISTS idx_feature_category_history_popularity
    ON feature_category_history (window_size, window_end DESC, category_popularity_score DESC);
