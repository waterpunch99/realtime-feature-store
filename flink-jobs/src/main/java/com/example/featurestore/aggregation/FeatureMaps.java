package com.example.featurestore.aggregation;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

public final class FeatureMaps {
    private FeatureMaps() {
    }

    public static Map<String, Object> userFeatures(FeatureAccumulator acc, String windowSize) {
        Map<String, Object> features = new LinkedHashMap<>();
        if ("10m".equals(windowSize)) {
            features.put("user_recent_click_categories_10m", new ArrayList<>(acc.getClickCategories()));
            features.put("user_click_count_10m", acc.getClickCount());
            features.put("user_view_count_10m", acc.getViewCount());
        } else if ("1h".equals(windowSize)) {
            features.put("user_recent_click_categories_1h", new ArrayList<>(acc.getClickCategories()));
            features.put("user_click_count_1h", acc.getClickCount());
            features.put("user_add_to_cart_count_1h", acc.getAddToCartCount());
            features.put("user_purchase_count_1h", acc.getPurchaseCount());
            features.put("user_purchase_amount_1h", acc.getPurchaseAmount());
        }
        if (acc.getLastEventTimeMillis() > 0) {
            features.put("user_last_event_time", Instant.ofEpochMilli(acc.getLastEventTimeMillis()).toString());
        }
        features.put("user_last_product_id", acc.getLastProductId());
        return features;
    }

    public static Map<String, Object> productFeatures(FeatureAccumulator acc, String windowSize) {
        Map<String, Object> features = new LinkedHashMap<>();
        if ("10m".equals(windowSize)) {
            features.put("product_view_count_10m", acc.getViewCount());
            features.put("product_click_count_10m", acc.getClickCount());
            features.put("product_add_to_cart_count_10m", acc.getAddToCartCount());
            features.put("product_ctr_10m", ratio(acc.getClickCount(), acc.getViewCount()));
            features.put("product_popularity_score_10m", popularityScore(acc));
        } else if ("1h".equals(windowSize)) {
            features.put("product_purchase_count_1h", acc.getPurchaseCount());
            features.put("product_conversion_rate_1h", ratio(acc.getPurchaseCount(), acc.getViewCount()));
        }
        return features;
    }

    public static Map<String, Object> categoryFeatures(FeatureAccumulator acc, String windowSize) {
        Map<String, Object> features = new LinkedHashMap<>();
        if ("10m".equals(windowSize)) {
            features.put("category_view_count_10m", acc.getViewCount());
            features.put("category_click_count_10m", acc.getClickCount());
            features.put("category_popularity_score_10m", popularityScore(acc));
        } else if ("1h".equals(windowSize)) {
            features.put("category_add_to_cart_count_1h", acc.getAddToCartCount());
            features.put("category_purchase_count_1h", acc.getPurchaseCount());
            features.put("category_purchase_amount_1h", acc.getPurchaseAmount());
        }
        return features;
    }

    public static BigDecimal popularityScore(FeatureAccumulator acc) {
        return BigDecimal.valueOf(acc.getViewCount())
                .add(BigDecimal.valueOf(acc.getClickCount()).multiply(BigDecimal.valueOf(3)))
                .add(BigDecimal.valueOf(acc.getAddToCartCount()).multiply(BigDecimal.valueOf(5)))
                .add(BigDecimal.valueOf(acc.getPurchaseCount()).multiply(BigDecimal.valueOf(10)));
    }

    private static BigDecimal ratio(long numerator, long denominator) {
        if (denominator <= 0) {
            return BigDecimal.ZERO;
        }
        return BigDecimal.valueOf(numerator)
                .divide(BigDecimal.valueOf(denominator), 6, RoundingMode.HALF_UP);
    }
}

