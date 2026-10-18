package dev.harsharocks.orders;

import java.time.Instant;

/** Latest accepted order state, with a monotonically increasing local revision. */
public record OrderSnapshot(
        String restaurantId,
        String orderId,
        Instant updatedAt,
        String status,
        long totalCents,
        long revision) {
}
