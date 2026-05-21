package com.example.featurestore.config;

import org.apache.flink.api.java.utils.ParameterTool;

public record JobConfig(
        String bootstrapServers,
        String rawTopic,
        String cleanTopic,
        String invalidDlqTopic,
        String lateDlqTopic,
        String featureUserTopic,
        String featureProductTopic,
        String featureCategoryTopic,
        String consumerGroupId,
        int watermarkDelaySeconds,
        long checkpointIntervalMillis,
        String checkpointPath,
        String savepointPath,
        String stateBackend,
        long dedupTtlHours,
        String redisHost,
        int redisPort,
        String jdbcUrl,
        String jdbcUser,
        String jdbcPassword
) {
    public static JobConfig fromArgs(String[] args) {
        ParameterTool params = ParameterTool.fromArgs(args);
        return new JobConfig(
                params.get("bootstrap.servers", "kafka:29092"),
                params.get("raw.topic", "raw-user-events"),
                params.get("clean.topic", "clean-user-events"),
                params.get("invalid.topic", "invalid-user-events-dlq"),
                params.get("late.topic", "late-events-dlq"),
                params.get("feature.user.topic", "feature-user-updates"),
                params.get("feature.product.topic", "feature-product-updates"),
                params.get("feature.category.topic", "feature-category-updates"),
                params.get("group.id", "validation-enrichment-job"),
                params.getInt("watermark.delay.seconds", 120),
                params.getLong("checkpoint.interval.ms", 60000L),
                params.get("checkpoint.path", "file:///tmp/flink-checkpoints"),
                params.get("savepoint.path", "file:///tmp/flink-savepoints"),
                params.get("state.backend", "hashmap"),
                params.getLong("dedup.ttl.hours", 25L),
                params.get("redis.host", "redis"),
                params.getInt("redis.port", 6379),
                params.get("jdbc.url", "jdbc:postgresql://postgres:5432/feature_store"),
                params.get("jdbc.user", "feature_store"),
                params.get("jdbc.password", "feature_store")
        );
    }
}
