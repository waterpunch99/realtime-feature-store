package com.example.featurestore.sink;

import com.example.featurestore.model.FeatureUpdate;
import com.example.featurestore.serde.JsonSerdes;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;

public final class FeatureValueConverters {
    private FeatureValueConverters() {
    }

    public static long longValue(Map<String, Object> features, String key) {
        Object value = features.get(key);
        if (value instanceof Number number) {
            return number.longValue();
        }
        return 0L;
    }

    public static BigDecimal decimalValue(Map<String, Object> features, String key) {
        Object value = features.get(key);
        if (value instanceof BigDecimal decimal) {
            return decimal;
        }
        if (value instanceof Number number) {
            return BigDecimal.valueOf(number.doubleValue());
        }
        return BigDecimal.ZERO;
    }

    public static String stringValue(Map<String, Object> features, String key) {
        Object value = features.get(key);
        return value == null ? null : String.valueOf(value);
    }

    public static String jsonValue(Object value) {
        try {
            return JsonSerdes.objectToJson(value);
        } catch (JsonProcessingException exc) {
            throw new IllegalArgumentException("Failed to serialize feature value", exc);
        }
    }

    public static String featureJson(FeatureUpdate update) {
        return jsonValue(update.getFeatures());
    }

    public static Timestamp timestamp(Instant instant) {
        return instant == null ? null : Timestamp.from(instant);
    }
}
