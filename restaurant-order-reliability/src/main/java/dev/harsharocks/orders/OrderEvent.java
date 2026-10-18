package dev.harsharocks.orders;

import java.time.Instant;
import java.util.Objects;

/** A minimal, provider-neutral representation of a full order-update event. */
public record OrderEvent(
        String eventId,
        String restaurantId,
        String orderId,
        Instant occurredAt,
        String status,
        long totalCents) {

    public OrderEvent {
        eventId = required(eventId, "eventId");
        restaurantId = required(restaurantId, "restaurantId");
        orderId = required(orderId, "orderId");
        occurredAt = Objects.requireNonNull(occurredAt, "occurredAt must not be null");
        status = required(status, "status");
        if (totalCents < 0) {
            throw new IllegalArgumentException("totalCents must not be negative");
        }
    }

    private static String required(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value.trim();
    }
}
