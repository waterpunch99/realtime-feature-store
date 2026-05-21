package com.example.featurestore.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.Map;

public class FeatureUpdate {
    @JsonProperty("entity_type")
    private String entityType;

    @JsonProperty("entity_id")
    private String entityId;

    @JsonProperty("window_size")
    private String windowSize;

    @JsonProperty("window_start")
    private Instant windowStart;

    @JsonProperty("window_end")
    private Instant windowEnd;

    @JsonProperty("features")
    private Map<String, Object> features;

    @JsonProperty("updated_at")
    private Instant updatedAt;

    public FeatureUpdate() {
    }

    public FeatureUpdate(
            String entityType,
            String entityId,
            String windowSize,
            Instant windowStart,
            Instant windowEnd,
            Map<String, Object> features,
            Instant updatedAt
    ) {
        this.entityType = entityType;
        this.entityId = entityId;
        this.windowSize = windowSize;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.features = features;
        this.updatedAt = updatedAt;
    }

    public String getEntityType() {
        return entityType;
    }

    public void setEntityType(String entityType) {
        this.entityType = entityType;
    }

    public String getEntityId() {
        return entityId;
    }

    public void setEntityId(String entityId) {
        this.entityId = entityId;
    }

    public String getWindowSize() {
        return windowSize;
    }

    public void setWindowSize(String windowSize) {
        this.windowSize = windowSize;
    }

    public Instant getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(Instant windowStart) {
        this.windowStart = windowStart;
    }

    public Instant getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Instant windowEnd) {
        this.windowEnd = windowEnd;
    }

    public Map<String, Object> getFeatures() {
        return features;
    }

    public void setFeatures(Map<String, Object> features) {
        this.features = features;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }
}
