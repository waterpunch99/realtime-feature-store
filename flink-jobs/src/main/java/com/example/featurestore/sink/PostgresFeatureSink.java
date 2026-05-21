package com.example.featurestore.sink;

import com.example.featurestore.model.FeatureUpdate;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class PostgresFeatureSink extends RichSinkFunction<FeatureUpdate> {
    private final String jdbcUrl;
    private final String user;
    private final String password;
    private transient Connection connection;

    public PostgresFeatureSink(String jdbcUrl, String user, String password) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
    }

    @Override
    public void open(Configuration parameters) throws SQLException {
        connection = DriverManager.getConnection(jdbcUrl, user, password);
        connection.setAutoCommit(true);
    }

    @Override
    public void invoke(FeatureUpdate update, Context context) throws SQLException {
        switch (update.getEntityType()) {
            case "user" -> {
                upsertUserLatest(update);
                insertUserHistory(update);
            }
            case "product" -> {
                upsertProductLatest(update);
                insertProductHistory(update);
            }
            case "category" -> {
                upsertCategoryLatest(update);
                insertCategoryHistory(update);
            }
            default -> throw new IllegalArgumentException("Unknown entity_type: " + update.getEntityType());
        }
    }

    @Override
    public void close() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    private void upsertUserLatest(FeatureUpdate update) throws SQLException {
        if ("10m".equals(update.getWindowSize())) {
            execute("""
                    INSERT INTO feature_user_latest (
                        user_id,
                        user_recent_click_categories_10m,
                        user_click_count_10m,
                        user_view_count_10m,
                        user_last_event_time,
                        user_last_product_id,
                        features,
                        window_updated_at,
                        updated_at
                    )
                    VALUES (?, ?::jsonb, ?, ?, ?, ?, ?::jsonb, ?, ?)
                    ON CONFLICT (user_id) DO UPDATE SET
                        user_recent_click_categories_10m = EXCLUDED.user_recent_click_categories_10m,
                        user_click_count_10m = EXCLUDED.user_click_count_10m,
                        user_view_count_10m = EXCLUDED.user_view_count_10m,
                        user_last_event_time = EXCLUDED.user_last_event_time,
                        user_last_product_id = EXCLUDED.user_last_product_id,
                        features = feature_user_latest.features || EXCLUDED.features,
                        window_updated_at = EXCLUDED.window_updated_at,
                        updated_at = EXCLUDED.updated_at
                    """, statement -> {
                Map<String, Object> f = update.getFeatures();
                statement.setString(1, update.getEntityId());
                statement.setString(2, FeatureValueConverters.jsonValue(f.get("user_recent_click_categories_10m")));
                statement.setLong(3, FeatureValueConverters.longValue(f, "user_click_count_10m"));
                statement.setLong(4, FeatureValueConverters.longValue(f, "user_view_count_10m"));
                setTimestamp(statement, 5, FeatureValueConverters.stringValue(f, "user_last_event_time"));
                statement.setString(6, FeatureValueConverters.stringValue(f, "user_last_product_id"));
                statement.setString(7, FeatureValueConverters.featureJson(update));
                statement.setTimestamp(8, FeatureValueConverters.timestamp(update.getWindowEnd()));
                statement.setTimestamp(9, FeatureValueConverters.timestamp(update.getUpdatedAt()));
            });
            return;
        }

        execute("""
                INSERT INTO feature_user_latest (
                    user_id,
                    user_recent_click_categories_1h,
                    user_click_count_1h,
                    user_add_to_cart_count_1h,
                    user_purchase_count_1h,
                    user_purchase_amount_1h,
                    user_last_event_time,
                    user_last_product_id,
                    features,
                    window_updated_at,
                    updated_at
                )
                VALUES (?, ?::jsonb, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?)
                ON CONFLICT (user_id) DO UPDATE SET
                    user_recent_click_categories_1h = EXCLUDED.user_recent_click_categories_1h,
                    user_click_count_1h = EXCLUDED.user_click_count_1h,
                    user_add_to_cart_count_1h = EXCLUDED.user_add_to_cart_count_1h,
                    user_purchase_count_1h = EXCLUDED.user_purchase_count_1h,
                    user_purchase_amount_1h = EXCLUDED.user_purchase_amount_1h,
                    user_last_event_time = EXCLUDED.user_last_event_time,
                    user_last_product_id = EXCLUDED.user_last_product_id,
                    features = feature_user_latest.features || EXCLUDED.features,
                    window_updated_at = EXCLUDED.window_updated_at,
                    updated_at = EXCLUDED.updated_at
                """, statement -> {
            Map<String, Object> f = update.getFeatures();
            statement.setString(1, update.getEntityId());
            statement.setString(2, FeatureValueConverters.jsonValue(f.get("user_recent_click_categories_1h")));
            statement.setLong(3, FeatureValueConverters.longValue(f, "user_click_count_1h"));
            statement.setLong(4, FeatureValueConverters.longValue(f, "user_add_to_cart_count_1h"));
            statement.setLong(5, FeatureValueConverters.longValue(f, "user_purchase_count_1h"));
            statement.setBigDecimal(6, FeatureValueConverters.decimalValue(f, "user_purchase_amount_1h"));
            setTimestamp(statement, 7, FeatureValueConverters.stringValue(f, "user_last_event_time"));
            statement.setString(8, FeatureValueConverters.stringValue(f, "user_last_product_id"));
            statement.setString(9, FeatureValueConverters.featureJson(update));
            statement.setTimestamp(10, FeatureValueConverters.timestamp(update.getWindowEnd()));
            statement.setTimestamp(11, FeatureValueConverters.timestamp(update.getUpdatedAt()));
        });
    }

    private void upsertProductLatest(FeatureUpdate update) throws SQLException {
        if ("10m".equals(update.getWindowSize())) {
            execute("""
                    INSERT INTO feature_product_latest (
                        product_id,
                        product_view_count_10m,
                        product_click_count_10m,
                        product_add_to_cart_count_10m,
                        product_ctr_10m,
                        product_popularity_score_10m,
                        features,
                        window_updated_at,
                        updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?)
                    ON CONFLICT (product_id) DO UPDATE SET
                        product_view_count_10m = EXCLUDED.product_view_count_10m,
                        product_click_count_10m = EXCLUDED.product_click_count_10m,
                        product_add_to_cart_count_10m = EXCLUDED.product_add_to_cart_count_10m,
                        product_ctr_10m = EXCLUDED.product_ctr_10m,
                        product_popularity_score_10m = EXCLUDED.product_popularity_score_10m,
                        features = feature_product_latest.features || EXCLUDED.features,
                        window_updated_at = EXCLUDED.window_updated_at,
                        updated_at = EXCLUDED.updated_at
                    """, statement -> {
                Map<String, Object> f = update.getFeatures();
                statement.setString(1, update.getEntityId());
                statement.setLong(2, FeatureValueConverters.longValue(f, "product_view_count_10m"));
                statement.setLong(3, FeatureValueConverters.longValue(f, "product_click_count_10m"));
                statement.setLong(4, FeatureValueConverters.longValue(f, "product_add_to_cart_count_10m"));
                statement.setBigDecimal(5, FeatureValueConverters.decimalValue(f, "product_ctr_10m"));
                statement.setBigDecimal(6, FeatureValueConverters.decimalValue(f, "product_popularity_score_10m"));
                statement.setString(7, FeatureValueConverters.featureJson(update));
                statement.setTimestamp(8, FeatureValueConverters.timestamp(update.getWindowEnd()));
                statement.setTimestamp(9, FeatureValueConverters.timestamp(update.getUpdatedAt()));
            });
            return;
        }

        execute("""
                INSERT INTO feature_product_latest (
                    product_id,
                    product_purchase_count_1h,
                    product_conversion_rate_1h,
                    features,
                    window_updated_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?::jsonb, ?, ?)
                ON CONFLICT (product_id) DO UPDATE SET
                    product_purchase_count_1h = EXCLUDED.product_purchase_count_1h,
                    product_conversion_rate_1h = EXCLUDED.product_conversion_rate_1h,
                    features = feature_product_latest.features || EXCLUDED.features,
                    window_updated_at = EXCLUDED.window_updated_at,
                    updated_at = EXCLUDED.updated_at
                """, statement -> {
            Map<String, Object> f = update.getFeatures();
            statement.setString(1, update.getEntityId());
            statement.setLong(2, FeatureValueConverters.longValue(f, "product_purchase_count_1h"));
            statement.setBigDecimal(3, FeatureValueConverters.decimalValue(f, "product_conversion_rate_1h"));
            statement.setString(4, FeatureValueConverters.featureJson(update));
            statement.setTimestamp(5, FeatureValueConverters.timestamp(update.getWindowEnd()));
            statement.setTimestamp(6, FeatureValueConverters.timestamp(update.getUpdatedAt()));
        });
    }

    private void upsertCategoryLatest(FeatureUpdate update) throws SQLException {
        if ("10m".equals(update.getWindowSize())) {
            execute("""
                    INSERT INTO feature_category_latest (
                        category_id,
                        category_view_count_10m,
                        category_click_count_10m,
                        category_popularity_score_10m,
                        features,
                        window_updated_at,
                        updated_at
                    )
                    VALUES (?, ?, ?, ?, ?::jsonb, ?, ?)
                    ON CONFLICT (category_id) DO UPDATE SET
                        category_view_count_10m = EXCLUDED.category_view_count_10m,
                        category_click_count_10m = EXCLUDED.category_click_count_10m,
                        category_popularity_score_10m = EXCLUDED.category_popularity_score_10m,
                        features = feature_category_latest.features || EXCLUDED.features,
                        window_updated_at = EXCLUDED.window_updated_at,
                        updated_at = EXCLUDED.updated_at
                    """, statement -> {
                Map<String, Object> f = update.getFeatures();
                statement.setString(1, update.getEntityId());
                statement.setLong(2, FeatureValueConverters.longValue(f, "category_view_count_10m"));
                statement.setLong(3, FeatureValueConverters.longValue(f, "category_click_count_10m"));
                statement.setBigDecimal(4, FeatureValueConverters.decimalValue(f, "category_popularity_score_10m"));
                statement.setString(5, FeatureValueConverters.featureJson(update));
                statement.setTimestamp(6, FeatureValueConverters.timestamp(update.getWindowEnd()));
                statement.setTimestamp(7, FeatureValueConverters.timestamp(update.getUpdatedAt()));
            });
            return;
        }

        execute("""
                INSERT INTO feature_category_latest (
                    category_id,
                    category_add_to_cart_count_1h,
                    category_purchase_count_1h,
                    category_purchase_amount_1h,
                    features,
                    window_updated_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?::jsonb, ?, ?)
                ON CONFLICT (category_id) DO UPDATE SET
                    category_add_to_cart_count_1h = EXCLUDED.category_add_to_cart_count_1h,
                    category_purchase_count_1h = EXCLUDED.category_purchase_count_1h,
                    category_purchase_amount_1h = EXCLUDED.category_purchase_amount_1h,
                    features = feature_category_latest.features || EXCLUDED.features,
                    window_updated_at = EXCLUDED.window_updated_at,
                    updated_at = EXCLUDED.updated_at
                """, statement -> {
            Map<String, Object> f = update.getFeatures();
            statement.setString(1, update.getEntityId());
            statement.setLong(2, FeatureValueConverters.longValue(f, "category_add_to_cart_count_1h"));
            statement.setLong(3, FeatureValueConverters.longValue(f, "category_purchase_count_1h"));
            statement.setBigDecimal(4, FeatureValueConverters.decimalValue(f, "category_purchase_amount_1h"));
            statement.setString(5, FeatureValueConverters.featureJson(update));
            statement.setTimestamp(6, FeatureValueConverters.timestamp(update.getWindowEnd()));
            statement.setTimestamp(7, FeatureValueConverters.timestamp(update.getUpdatedAt()));
        });
    }

    private void insertUserHistory(FeatureUpdate update) throws SQLException {
        execute("""
                INSERT INTO feature_user_history (
                    user_id, window_size, window_start, window_end,
                    user_recent_click_categories, user_click_count, user_view_count,
                    user_add_to_cart_count, user_purchase_count, user_purchase_amount,
                    user_last_event_time, user_last_product_id, features
                )
                VALUES (?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)
                ON CONFLICT (user_id, window_size, window_start, window_end) DO NOTHING
                """, statement -> {
            Map<String, Object> f = update.getFeatures();
            statement.setString(1, update.getEntityId());
            statement.setString(2, update.getWindowSize());
            statement.setTimestamp(3, FeatureValueConverters.timestamp(update.getWindowStart()));
            statement.setTimestamp(4, FeatureValueConverters.timestamp(update.getWindowEnd()));
            statement.setString(5, clickCategoriesJson(f, update.getWindowSize(), "user"));
            statement.setLong(6, longByWindow(f, update.getWindowSize(), "user_click_count"));
            statement.setLong(7, FeatureValueConverters.longValue(f, "user_view_count_10m"));
            statement.setLong(8, FeatureValueConverters.longValue(f, "user_add_to_cart_count_1h"));
            statement.setLong(9, FeatureValueConverters.longValue(f, "user_purchase_count_1h"));
            statement.setBigDecimal(10, FeatureValueConverters.decimalValue(f, "user_purchase_amount_1h"));
            setTimestamp(statement, 11, FeatureValueConverters.stringValue(f, "user_last_event_time"));
            statement.setString(12, FeatureValueConverters.stringValue(f, "user_last_product_id"));
            statement.setString(13, FeatureValueConverters.featureJson(update));
        });
    }

    private void insertProductHistory(FeatureUpdate update) throws SQLException {
        execute("""
                INSERT INTO feature_product_history (
                    product_id, window_size, window_start, window_end,
                    product_view_count, product_click_count, product_add_to_cart_count,
                    product_purchase_count, product_ctr, product_conversion_rate,
                    product_popularity_score, features
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)
                ON CONFLICT (product_id, window_size, window_start, window_end) DO NOTHING
                """, statement -> {
            Map<String, Object> f = update.getFeatures();
            statement.setString(1, update.getEntityId());
            statement.setString(2, update.getWindowSize());
            statement.setTimestamp(3, FeatureValueConverters.timestamp(update.getWindowStart()));
            statement.setTimestamp(4, FeatureValueConverters.timestamp(update.getWindowEnd()));
            statement.setLong(5, FeatureValueConverters.longValue(f, "product_view_count_10m"));
            statement.setLong(6, FeatureValueConverters.longValue(f, "product_click_count_10m"));
            statement.setLong(7, FeatureValueConverters.longValue(f, "product_add_to_cart_count_10m"));
            statement.setLong(8, FeatureValueConverters.longValue(f, "product_purchase_count_1h"));
            statement.setBigDecimal(9, FeatureValueConverters.decimalValue(f, "product_ctr_10m"));
            statement.setBigDecimal(10, FeatureValueConverters.decimalValue(f, "product_conversion_rate_1h"));
            statement.setBigDecimal(11, FeatureValueConverters.decimalValue(f, "product_popularity_score_10m"));
            statement.setString(12, FeatureValueConverters.featureJson(update));
        });
    }

    private void insertCategoryHistory(FeatureUpdate update) throws SQLException {
        execute("""
                INSERT INTO feature_category_history (
                    category_id, window_size, window_start, window_end,
                    category_view_count, category_click_count, category_add_to_cart_count,
                    category_purchase_count, category_purchase_amount, category_popularity_score,
                    features
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)
                ON CONFLICT (category_id, window_size, window_start, window_end) DO NOTHING
                """, statement -> {
            Map<String, Object> f = update.getFeatures();
            statement.setString(1, update.getEntityId());
            statement.setString(2, update.getWindowSize());
            statement.setTimestamp(3, FeatureValueConverters.timestamp(update.getWindowStart()));
            statement.setTimestamp(4, FeatureValueConverters.timestamp(update.getWindowEnd()));
            statement.setLong(5, FeatureValueConverters.longValue(f, "category_view_count_10m"));
            statement.setLong(6, FeatureValueConverters.longValue(f, "category_click_count_10m"));
            statement.setLong(7, FeatureValueConverters.longValue(f, "category_add_to_cart_count_1h"));
            statement.setLong(8, FeatureValueConverters.longValue(f, "category_purchase_count_1h"));
            statement.setBigDecimal(9, FeatureValueConverters.decimalValue(f, "category_purchase_amount_1h"));
            statement.setBigDecimal(10, FeatureValueConverters.decimalValue(f, "category_popularity_score_10m"));
            statement.setString(11, FeatureValueConverters.featureJson(update));
        });
    }

    private String clickCategoriesJson(Map<String, Object> features, String windowSize, String prefix) {
        Object value = features.get(prefix + "_recent_click_categories_" + windowSize);
        return FeatureValueConverters.jsonValue(value == null ? java.util.List.of() : value);
    }

    private long longByWindow(Map<String, Object> features, String windowSize, String prefix) {
        return FeatureValueConverters.longValue(features, prefix + "_" + windowSize);
    }

    private void setTimestamp(PreparedStatement statement, int index, String value) throws SQLException {
        if (value == null) {
            statement.setNull(index, Types.TIMESTAMP_WITH_TIMEZONE);
            return;
        }
        statement.setTimestamp(index, java.sql.Timestamp.from(java.time.Instant.parse(value)));
    }

    private void execute(String sql, StatementBinder binder) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            binder.bind(statement);
            statement.executeUpdate();
        }
    }

    @FunctionalInterface
    private interface StatementBinder {
        void bind(PreparedStatement statement) throws SQLException;
    }
}
