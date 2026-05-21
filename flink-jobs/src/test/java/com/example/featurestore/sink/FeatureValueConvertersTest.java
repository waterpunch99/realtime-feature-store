package com.example.featurestore.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FeatureValueConvertersTest {
    @Test
    void convertsNumericFeatureValues() {
        Map<String, Object> features = Map.of(
                "count", 3L,
                "score", new BigDecimal("12.50")
        );

        assertEquals(3L, FeatureValueConverters.longValue(features, "count"));
        assertEquals(new BigDecimal("12.50"), FeatureValueConverters.decimalValue(features, "score"));
        assertEquals(BigDecimal.ZERO, FeatureValueConverters.decimalValue(features, "missing"));
    }

    @Test
    void serializesComplexFeatureValueAsJson() {
        String json = FeatureValueConverters.jsonValue(List.of("c_1", "c_2"));

        assertEquals("[\"c_1\",\"c_2\"]", json);
    }

    @Test
    void convertsInstantToSqlTimestamp() {
        assertEquals(
                "2026-05-21 12:00:00.0",
                FeatureValueConverters.timestamp(Instant.parse("2026-05-21T12:00:00Z")).toString()
        );
    }
}
