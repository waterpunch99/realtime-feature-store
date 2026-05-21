package com.example.featurestore.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;

public class DlqEvent {
    @JsonProperty("event_id")
    private final String eventId;

    @JsonProperty("event_type")
    private final String eventType;

    @JsonProperty("user_id")
    private final String userId;

    @JsonProperty("reason_code")
    private final String reasonCode;

    @JsonProperty("reason_detail")
    private final String reasonDetail;

    @JsonProperty("raw_event")
    private final String rawEvent;

    @JsonProperty("created_at")
    private final Instant createdAt;

    public DlqEvent(
            String eventId,
            String eventType,
            String userId,
            String reasonCode,
            String reasonDetail,
            String rawEvent,
            Instant createdAt
    ) {
        this.eventId = eventId;
        this.eventType = eventType;
        this.userId = userId;
        this.reasonCode = reasonCode;
        this.reasonDetail = reasonDetail;
        this.rawEvent = rawEvent;
        this.createdAt = createdAt;
    }

    public String getEventId() {
        return eventId;
    }

    public String getEventType() {
        return eventType;
    }

    public String getUserId() {
        return userId;
    }

    public String getReasonCode() {
        return reasonCode;
    }

    public String getReasonDetail() {
        return reasonDetail;
    }

    public String getRawEvent() {
        return rawEvent;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}

