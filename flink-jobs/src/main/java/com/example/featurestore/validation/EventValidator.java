package com.example.featurestore.validation;

import com.example.featurestore.model.DlqEvent;
import com.example.featurestore.model.EventProperties;
import com.example.featurestore.model.UserEvent;
import com.example.featurestore.serde.JsonSerdes;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;

public class EventValidator {
    private static final Set<String> ALLOWED_EVENT_TYPES =
            Set.of("view", "click", "add_to_cart", "purchase", "search");
    private static final Duration MAX_FUTURE_SKEW = Duration.ofMinutes(10);
    private static final Duration MAX_PAST_AGE = Duration.ofHours(24);

    private final Clock clock;

    public EventValidator() {
        this(Clock.systemUTC());
    }

    public EventValidator(Clock clock) {
        this.clock = clock;
    }

    public ValidationResult validate(String rawJson) {
        UserEvent event;
        try {
            event = JsonSerdes.parseUserEvent(rawJson);
        } catch (JsonProcessingException exc) {
            return invalid(null, null, null, "json_parse_failed", exc.getOriginalMessage(), rawJson);
        }

        String reasonCode = validateEvent(event);
        if (reasonCode != null) {
            return invalid(
                    event.getEventId(),
                    event.getEventType(),
                    event.getUserId(),
                    reasonCode,
                    reasonDetail(reasonCode, event),
                    rawJson
            );
        }

        return ValidationResult.valid(event);
    }

    private String validateEvent(UserEvent event) {
        if (isBlank(event.getEventId())) {
            return "event_id_missing";
        }
        if (isBlank(event.getEventType()) || !ALLOWED_EVENT_TYPES.contains(event.getEventType())) {
            return "invalid_event_type";
        }
        if (isBlank(event.getUserId())) {
            return "user_id_missing";
        }
        if (isBlank(event.getSessionId())) {
            return "session_id_missing";
        }
        if (event.getEventTime() == null) {
            return "event_time_missing";
        }
        if (event.getProperties() == null) {
            event.setProperties(new EventProperties());
        }

        String requiredFieldFailure = validateRequiredFields(event);
        if (requiredFieldFailure != null) {
            return requiredFieldFailure;
        }

        String propertyFailure = validateProperties(event.getProperties());
        if (propertyFailure != null) {
            return propertyFailure;
        }

        Instant now = Instant.now(clock);
        if (event.getEventTime().isAfter(now.plus(MAX_FUTURE_SKEW))) {
            return "event_time_too_far_in_future";
        }
        if (event.getEventTime().isBefore(now.minus(MAX_PAST_AGE))) {
            return "event_time_too_old";
        }

        return null;
    }

    private String validateRequiredFields(UserEvent event) {
        return switch (event.getEventType()) {
            case "view", "click" -> {
                if (isBlank(event.getProductId())) {
                    yield "product_id_missing";
                }
                if (isBlank(event.getCategoryId())) {
                    yield "category_id_missing";
                }
                yield null;
            }
            case "add_to_cart", "purchase" -> {
                if (isBlank(event.getProductId())) {
                    yield "product_id_missing";
                }
                if (isBlank(event.getCategoryId())) {
                    yield "category_id_missing";
                }
                if (event.getProperties().getPrice() == null) {
                    yield "price_missing";
                }
                if (event.getProperties().getQuantity() == null) {
                    yield "quantity_missing";
                }
                yield null;
            }
            case "search" -> {
                if (isBlank(event.getProperties().getSearchQuery())) {
                    yield "search_query_missing";
                }
                yield null;
            }
            default -> "invalid_event_type";
        };
    }

    private String validateProperties(EventProperties properties) {
        BigDecimal price = properties.getPrice();
        if (price != null && price.compareTo(BigDecimal.ZERO) < 0) {
            return "invalid_price";
        }
        Integer quantity = properties.getQuantity();
        if (quantity != null && quantity < 1) {
            return "invalid_quantity";
        }
        return null;
    }

    private ValidationResult invalid(
            String eventId,
            String eventType,
            String userId,
            String reasonCode,
            String reasonDetail,
            String rawJson
    ) {
        String resolvedEventId = isBlank(eventId) ? UUID.randomUUID().toString() : eventId;
        return ValidationResult.invalid(
                new DlqEvent(
                        resolvedEventId,
                        eventType,
                        userId,
                        reasonCode,
                        reasonDetail,
                        rawJson,
                        Instant.now(clock)
                )
        );
    }

    private String reasonDetail(String reasonCode, UserEvent event) {
        return switch (reasonCode) {
            case "event_id_missing" -> "event_id must not be empty";
            case "invalid_event_type" -> "event_type is not allowed: " + event.getEventType();
            case "user_id_missing" -> "user_id must not be empty";
            case "session_id_missing" -> "session_id must not be empty";
            case "event_time_missing" -> "event_time must not be null";
            case "product_id_missing" -> "product_id is required for " + event.getEventType();
            case "category_id_missing" -> "category_id is required for " + event.getEventType();
            case "price_missing" -> "price is required for " + event.getEventType();
            case "quantity_missing" -> "quantity is required for " + event.getEventType();
            case "search_query_missing" -> "search_query is required for search";
            case "invalid_price" -> "price must be greater than or equal to 0";
            case "invalid_quantity" -> "quantity must be greater than or equal to 1";
            case "event_time_too_far_in_future" -> "event_time is more than 10 minutes in the future";
            case "event_time_too_old" -> "event_time is more than 24 hours in the past";
            default -> reasonCode;
        };
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}

