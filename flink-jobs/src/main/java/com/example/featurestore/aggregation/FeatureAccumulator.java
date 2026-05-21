package com.example.featurestore.aggregation;

import com.example.featurestore.model.UserEvent;
import java.math.BigDecimal;
import java.util.LinkedHashSet;
import java.util.Set;

public class FeatureAccumulator {
    private long viewCount;
    private long clickCount;
    private long addToCartCount;
    private long purchaseCount;
    private BigDecimal purchaseAmount = BigDecimal.ZERO;
    private final Set<String> clickCategories = new LinkedHashSet<>();
    private String lastProductId;
    private long lastEventTimeMillis;

    public void add(UserEvent event) {
        switch (event.getEventType()) {
            case "view" -> viewCount++;
            case "click" -> {
                clickCount++;
                if (event.getCategoryId() != null) {
                    clickCategories.add(event.getCategoryId());
                }
            }
            case "add_to_cart" -> addToCartCount++;
            case "purchase" -> {
                purchaseCount++;
                if (event.getProperties() != null && event.getProperties().getPrice() != null) {
                    BigDecimal quantity = BigDecimal.valueOf(
                            event.getProperties().getQuantity() == null
                                    ? 1L
                                    : event.getProperties().getQuantity()
                    );
                    purchaseAmount = purchaseAmount.add(event.getProperties().getPrice().multiply(quantity));
                }
            }
            default -> {
            }
        }

        if (event.getProductId() != null) {
            lastProductId = event.getProductId();
        }
        if (event.getEventTime() != null
                && event.getEventTime().toEpochMilli() >= lastEventTimeMillis) {
            lastEventTimeMillis = event.getEventTime().toEpochMilli();
        }
    }

    public FeatureAccumulator merge(FeatureAccumulator other) {
        viewCount += other.viewCount;
        clickCount += other.clickCount;
        addToCartCount += other.addToCartCount;
        purchaseCount += other.purchaseCount;
        purchaseAmount = purchaseAmount.add(other.purchaseAmount);
        clickCategories.addAll(other.clickCategories);
        if (other.lastEventTimeMillis >= lastEventTimeMillis) {
            lastEventTimeMillis = other.lastEventTimeMillis;
            lastProductId = other.lastProductId;
        }
        return this;
    }

    public long getViewCount() {
        return viewCount;
    }

    public long getClickCount() {
        return clickCount;
    }

    public long getAddToCartCount() {
        return addToCartCount;
    }

    public long getPurchaseCount() {
        return purchaseCount;
    }

    public BigDecimal getPurchaseAmount() {
        return purchaseAmount;
    }

    public Set<String> getClickCategories() {
        return clickCategories;
    }

    public String getLastProductId() {
        return lastProductId;
    }

    public long getLastEventTimeMillis() {
        return lastEventTimeMillis;
    }
}

