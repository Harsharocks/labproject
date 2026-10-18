package dev.harsharocks.orders;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import static dev.harsharocks.orders.ProcessingResult.Outcome;

/** Dependency-free contract tests; throws AssertionError and exits nonzero on failure. */
public final class OrderEventProcessorTest {
    private int assertions;

    public static void main(String[] args) throws Exception {
        var suite = new OrderEventProcessorTest();
        suite.testStateAndIdempotency();
        suite.testOutOfOrderAndTimestampTie();
        suite.testIdCollisionAndRestaurantIsolation();
        suite.testValidation();
        suite.testConcurrentDuplicateDeliveries();
        System.out.println("PASS - " + suite.assertions + " assertions across 5 scenarios");
    }

    private static OrderEvent event(String id, String restaurant, String order,
                                    String time, String status, long cents) {
        return new OrderEvent(id, restaurant, order, Instant.parse(time), status, cents);
    }

    private void check(boolean condition, String message) {
        assertions++;
        if (!condition) throw new AssertionError(message);
    }

    private void testStateAndIdempotency() {
        var processor = new OrderEventProcessor();
        var initial = event("a", "r", "1", "2026-01-01T10:00:00Z", "OPEN", 1200);
        check(processor.accept(initial).outcome() == Outcome.APPLIED, "first event applied");
        check(processor.accept(initial).outcome() == Outcome.DUPLICATE_EVENT, "retry deduplicated");
        check(processor.get("r", "1").orElseThrow().revision() == 1, "no extra revision");
        check(processor.observedEvents() == 1, "single unique event");
        check(processor.trackedOrders() == 1, "single order");
    }

    private void testOutOfOrderAndTimestampTie() {
        var processor = new OrderEventProcessor();
        processor.accept(event("a", "r", "1", "2026-01-01T10:00:00Z", "OPEN", 100));
        processor.accept(event("b", "r", "1", "2026-01-01T10:05:00Z", "PAID", 100));
        var stale = event("c", "r", "1", "2026-01-01T10:03:00Z", "UPDATED", 200);
        check(processor.accept(stale).outcome() == Outcome.STALE_EVENT, "stale update ignored");
        check(processor.accept(stale).outcome() == Outcome.DUPLICATE_EVENT, "stale retry deduplicated");
        check(processor.accept(event("d", "r", "1", "2026-01-01T10:05:00Z", "PAID", 100))
                .outcome() == Outcome.NO_CHANGE, "matching timestamp/data no-op");
        check(processor.accept(event("e", "r", "1", "2026-01-01T10:05:00Z", "CANCELLED", 100))
                .outcome() == Outcome.TIMESTAMP_CONFLICT, "ambiguous tie rejected");
        var snapshot = processor.get("r", "1").orElseThrow();
        check(snapshot.revision() == 2, "only newer revision applied");
        check(snapshot.status().equals("PAID"), "state not rolled back");
    }

    private void testIdCollisionAndRestaurantIsolation() {
        var processor = new OrderEventProcessor();
        processor.accept(event("a", "r1", "1", "2026-01-01T10:00:00Z", "OPEN", 100));
        check(processor.accept(event("a", "r1", "1", "2026-01-01T10:00:00Z", "OPEN", 200))
                .outcome() == Outcome.EVENT_ID_CONFLICT, "reused event ID cannot mutate content");
        check(processor.accept(event("b", "r2", "1", "2026-01-01T10:00:00Z", "PAID", 200))
                .outcome() == Outcome.APPLIED, "same order id across restaurants isolated");
        check(processor.trackedOrders() == 2, "two independent orders");
        check(processor.get("r1", "1").orElseThrow().totalCents() == 100, "r1 unchanged");
        check(processor.get("r2", "1").orElseThrow().totalCents() == 200, "r2 own value");
    }

    private void testValidation() {
        boolean failed = false;
        try {
            event("x", "r", "1", "2026-01-01T10:00:00Z", "OPEN", -1);
        } catch (IllegalArgumentException expected) {
            failed = true;
        }
        check(failed, "negative money rejected");
        failed = false;
        try {
            event(" ", "r", "1", "2026-01-01T10:00:00Z", "OPEN", 1);
        } catch (IllegalArgumentException expected) {
            failed = true;
        }
        check(failed, "empty event ID rejected");
    }

    private void testConcurrentDuplicateDeliveries() throws Exception {
        var processor = new OrderEventProcessor();
        var payload = event("same", "r", "1", "2026-01-01T10:00:00Z", "OPEN", 200);
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Callable<Outcome>> jobs = new ArrayList<>();
            for (int i = 0; i < 100; i++) jobs.add(() -> processor.accept(payload).outcome());
            var futures = pool.invokeAll(jobs);
            int applied = 0;
            int duplicate = 0;
            for (var result : futures) {
                Outcome outcome = result.get();
                if (outcome == Outcome.APPLIED) applied++;
                if (outcome == Outcome.DUPLICATE_EVENT) duplicate++;
            }
            check(applied == 1, "exactly one initial application under concurrency");
            check(duplicate == 99, "all other concurrent attempts deduplicated");
            check(processor.observedEvents() == 1, "single event stored under concurrency");
        }
    }
}
