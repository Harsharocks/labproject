package dev.harsharocks.orders;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static dev.harsharocks.orders.ProcessingResult.Outcome;

/**
 * In-memory projection for full order-update events. All operations are synchronized so
 * deduplication and state transitions remain atomic for concurrent deliveries.
 *
 * Events at the same timestamp with different content are intentionally rejected:
 * event time alone cannot establish which update should win. An upstream sequence
 * number or authoritative reconciliation would be needed to resolve that ambiguity.
 */
public final class OrderEventProcessor {
    private record OrderKey(String restaurantId, String orderId) {}

    private final Map<OrderKey, OrderSnapshot> orders = new HashMap<>();
    private final Map<String, OrderEvent> seenEvents = new HashMap<>();

    /**
     * Checks a previously observed event ID without changing state. Used by the durable
     * journal to skip a disk write for exact retries and conflicting ID reuse.
     */
    synchronized Optional<ProcessingResult> alreadySeen(OrderEvent event) {
        Objects.requireNonNull(event, "event must not be null");
        OrderEvent prior = seenEvents.get(event.eventId());
        if (prior == null) return Optional.empty();
        OrderKey key = new OrderKey(event.restaurantId(), event.orderId());
        return Optional.of(new ProcessingResult(prior.equals(event)
                ? Outcome.DUPLICATE_EVENT : Outcome.EVENT_ID_CONFLICT, orders.get(key)));
    }

    public synchronized ProcessingResult accept(OrderEvent event) {
        Objects.requireNonNull(event, "event must not be null");
        Optional<ProcessingResult> duplicate = alreadySeen(event);
        if (duplicate.isPresent()) return duplicate.get();
        OrderKey key = new OrderKey(event.restaurantId(), event.orderId());
        OrderSnapshot current = orders.get(key);

        // Record all valid event IDs, including stale/conflicting updates, to ensure
        // an identical retry has an idempotent outcome rather than repeated processing.
        seenEvents.put(event.eventId(), event);
        if (current != null) {
            int timeOrder = event.occurredAt().compareTo(current.updatedAt());
            if (timeOrder < 0) {
                return new ProcessingResult(Outcome.STALE_EVENT, current);
            }
            if (timeOrder == 0) {
                boolean sameData = current.status().equals(event.status())
                        && current.totalCents() == event.totalCents();
                return new ProcessingResult(
                        sameData ? Outcome.NO_CHANGE : Outcome.TIMESTAMP_CONFLICT, current);
            }
        }

        OrderSnapshot next = new OrderSnapshot(event.restaurantId(), event.orderId(),
                event.occurredAt(), event.status(), event.totalCents(),
                current == null ? 1 : current.revision() + 1);
        orders.put(key, next);
        return new ProcessingResult(Outcome.APPLIED, next);
    }

    public synchronized Optional<OrderSnapshot> get(String restaurantId, String orderId) {
        return Optional.ofNullable(orders.get(new OrderKey(restaurantId, orderId)));
    }

    public synchronized int trackedOrders() {
        return orders.size();
    }

    public synchronized int observedEvents() {
        return seenEvents.size();
    }
}
