package com.example.featurestore.aggregation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.example.featurestore.model.EventProperties;
import com.example.featurestore.model.UserEvent;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FeatureAccumulatorTest {
    @Test
    void accumulatesUserFeatureInputs() {
        FeatureAccumulator acc = new FeatureAccumulator();
        acc.add(event("view", "p_1", "c_1", null, null, "2026-05-21T12:00:00Z"));
        acc.add(event("click", "p_2", "c_2", null, null, "2026-05-21T12:01:00Z"));
        acc.add(event("purchase", "p_3", "c_2", BigDecimal.valueOf(1000), 2, "2026-05-21T12:02:00Z"));

        Map<String, Object> features = FeatureMaps.userFeatures(acc, "1h");

        assertEquals(1L, features.get("user_click_count_1h"));
        assertEquals(1L, features.get("user_purchase_count_1h"));
        assertEquals(BigDecimal.valueOf(2000), features.get("user_purchase_amount_1h"));
        assertEquals("p_3", features.get("user_last_product_id"));
        assertTrue(((List<?>) features.get("user_recent_click_categories_1h")).contains("c_2"));
    }

    @Test
    void calculatesProductCtrAndPopularityScore() {
        FeatureAccumulator acc = new FeatureAccumulator();
        acc.add(event("view", "p_1", "c_1", null, null, "2026-05-21T12:00:00Z"));
        acc.add(event("view", "p_1", "c_1", null, null, "2026-05-21T12:01:00Z"));
        acc.add(event("click", "p_1", "c_1", null, null, "2026-05-21T12:02:00Z"));
        acc.add(event("add_to_cart", "p_1", "c_1", BigDecimal.TEN, 1, "2026-05-21T12:03:00Z"));

        Map<String, Object> features = FeatureMaps.productFeatures(acc, "10m");

        assertEquals(2L, features.get("product_view_count_10m"));
        assertEquals(1L, features.get("product_click_count_10m"));
        assertEquals(new BigDecimal("0.500000"), features.get("product_ctr_10m"));
        assertEquals(BigDecimal.valueOf(10), features.get("product_popularity_score_10m"));
    }

    @Test
    void calculatesCategoryPurchaseAmount() {
        FeatureAccumulator acc = new FeatureAccumulator();
        acc.add(event("purchase", "p_1", "c_1", BigDecimal.valueOf(500), 3, "2026-05-21T12:00:00Z"));

        Map<String, Object> features = FeatureMaps.categoryFeatures(acc, "1h");

        assertEquals(1L, features.get("category_purchase_count_1h"));
        assertEquals(BigDecimal.valueOf(1500), features.get("category_purchase_amount_1h"));
    }

    private UserEvent event(
            String eventType,
            String productId,
            String categoryId,
            BigDecimal price,
            Integer quantity,
            String eventTime
    ) {
        EventProperties properties = new EventProperties();
        properties.setPrice(price);
        properties.setQuantity(quantity);

        UserEvent event = new UserEvent();
        event.setEventId("e_" + eventType + "_" + eventTime);
        event.setEventType(eventType);
        event.setUserId("u_1");
        event.setSessionId("s_1");
        event.setProductId(productId);
        event.setCategoryId(categoryId);
        event.setEventTime(Instant.parse(eventTime));
        event.setIngestTime(Instant.parse("2026-05-21T12:10:00Z"));
        event.setProperties(properties);
        return event;
    }
}

