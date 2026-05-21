package com.example.featurestore.validation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class EventValidatorTest {
    private final Clock clock = Clock.fixed(Instant.parse("2026-05-21T12:00:00Z"), ZoneOffset.UTC);
    private final EventValidator validator = new EventValidator(clock);

    @Test
    void validClickEventRoutesToClean() {
        ValidationResult result = validator.validate("""
                {
                  "event_id": "e_1",
                  "event_type": "click",
                  "user_id": "u_1",
                  "session_id": "s_1",
                  "product_id": "p_1",
                  "category_id": "c_1",
                  "event_time": "2026-05-21T11:59:00Z",
                  "ingest_time": "2026-05-21T12:00:01Z",
                  "properties": {"page": "product_detail"}
                }
                """);

        assertTrue(result.isValid());
        assertEquals("e_1", result.getEvent().getEventId());
        assertEquals("u_1", result.getEvent().getUserId());
    }

    @Test
    void purchaseRequiresPriceAndQuantity() {
        ValidationResult result = validator.validate("""
                {
                  "event_id": "e_2",
                  "event_type": "purchase",
                  "user_id": "u_1",
                  "session_id": "s_1",
                  "product_id": "p_1",
                  "category_id": "c_1",
                  "event_time": "2026-05-21T11:59:00Z",
                  "ingest_time": "2026-05-21T12:00:01Z",
                  "properties": {}
                }
                """);

        assertFalse(result.isValid());
        assertEquals("price_missing", result.getDlqEvent().getReasonCode());
    }

    @Test
    void searchRequiresSearchQuery() {
        ValidationResult result = validator.validate("""
                {
                  "event_id": "e_3",
                  "event_type": "search",
                  "user_id": "u_1",
                  "session_id": "s_1",
                  "event_time": "2026-05-21T11:59:00Z",
                  "ingest_time": "2026-05-21T12:00:01Z",
                  "properties": {}
                }
                """);

        assertFalse(result.isValid());
        assertEquals("search_query_missing", result.getDlqEvent().getReasonCode());
    }

    @Test
    void futureEventMoreThanTenMinutesRoutesToInvalidDlq() {
        ValidationResult result = validator.validate("""
                {
                  "event_id": "e_4",
                  "event_type": "view",
                  "user_id": "u_1",
                  "session_id": "s_1",
                  "product_id": "p_1",
                  "category_id": "c_1",
                  "event_time": "2026-05-21T12:11:00Z",
                  "ingest_time": "2026-05-21T12:00:01Z",
                  "properties": {}
                }
                """);

        assertFalse(result.isValid());
        assertEquals("event_time_too_far_in_future", result.getDlqEvent().getReasonCode());
    }

    @Test
    void malformedJsonRoutesToInvalidDlq() {
        ValidationResult result = validator.validate("{not-json");

        assertFalse(result.isValid());
        assertEquals("json_parse_failed", result.getDlqEvent().getReasonCode());
    }
}

