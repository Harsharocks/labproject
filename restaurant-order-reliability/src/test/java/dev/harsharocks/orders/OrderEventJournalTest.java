package dev.harsharocks.orders;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import static dev.harsharocks.orders.ProcessingResult.Outcome;

/** Dependency-free integration tests for disk-backed replay and write ordering. */
public final class OrderEventJournalTest {
    private int assertions;

    public static void main(String[] args) throws Exception {
        var suite = new OrderEventJournalTest();
        suite.testRestartAndIdempotency();
        suite.testRejectedEventsSurviveRestart();
        suite.testTamperDetection();
        suite.testTruncatedTailDetection();
        suite.testExclusiveLockAndClose();
        suite.testInvalidInputNotAppended();
        suite.testConcurrentAppend();
        System.out.println("PASS - " + suite.assertions + " assertions across 7 journal scenarios");
    }

    private static OrderEvent event(String id, String restaurant, String order,
                                    String time, String status, long cents) {
        return new OrderEvent(id, restaurant, order, Instant.parse(time), status, cents);
    }

    private void check(boolean condition, String message) {
        assertions++;
        if (!condition) throw new AssertionError(message);
    }

    private static Path newJournalPath() throws IOException {
        return Files.createTempDirectory("order-journal-test-").resolve("events.log");
    }

    private void testRestartAndIdempotency() throws Exception {
        Path file = newJournalPath();
        OrderEvent first = event("first", "r/é", "1", "2026-01-01T10:00:00Z", "OPEN", 100);
        OrderEvent second = event("second", "r/é", "1", "2026-01-01T10:05:00Z", "PAID", 100);
        try (var journal = new OrderEventJournal(file)) {
            check(journal.accept(first).outcome() == Outcome.APPLIED, "initial state stored");
            check(journal.accept(second).outcome() == Outcome.APPLIED, "newer state stored");
            check(journal.observedEvents() == 2, "two events in memory");
        }
        try (var journal = new OrderEventJournal(file)) {
            check(journal.get("r/é", "1").orElseThrow().status().equals("PAID"),
                    "latest state recovered with UTF-8 identifiers");
            check(journal.get("r/é", "1").orElseThrow().revision() == 2,
                    "revision recovered");
            check(journal.accept(first).outcome() == Outcome.DUPLICATE_EVENT,
                    "old event id remembered across restart");
            check(journal.observedEvents() == 2, "replay did not create extra unique event");
        }
    }

    private void testRejectedEventsSurviveRestart() throws Exception {
        Path file = newJournalPath();
        var paid = event("paid", "r", "o", "2026-01-01T10:05:00Z", "PAID", 200);
        var stale = event("stale", "r", "o", "2026-01-01T10:01:00Z", "OPEN", 200);
        var collision = event("paid", "r", "o", "2026-01-01T10:05:00Z", "OPEN", 200);
        try (var journal = new OrderEventJournal(file)) {
            check(journal.accept(paid).outcome() == Outcome.APPLIED, "paid applied");
            check(journal.accept(stale).outcome() == Outcome.STALE_EVENT, "stale ignored");
            check(journal.accept(collision).outcome() == Outcome.EVENT_ID_CONFLICT,
                    "event ID collision rejected");
        }
        try (var journal = new OrderEventJournal(file)) {
            check(journal.accept(stale).outcome() == Outcome.DUPLICATE_EVENT,
                    "stale event id remembered");
            check(journal.accept(collision).outcome() == Outcome.EVENT_ID_CONFLICT,
                    "event ID collision still rejected");
            check(journal.get("r", "o").orElseThrow().status().equals("PAID"),
                    "rejected updates did not overwrite state");
        }
    }

    private void testTamperDetection() throws Exception {
        Path file = newJournalPath();
        try (var journal = new OrderEventJournal(file)) {
            journal.accept(event("a", "r", "o", "2026-01-01T10:00:00Z", "OPEN", 100));
        }
        String record = Files.readString(file, StandardCharsets.UTF_8);
        Files.writeString(file, record.replace("\t100\t", "\t900\t"), StandardCharsets.UTF_8);
        boolean failed = false;
        try (var ignored = new OrderEventJournal(file)) {
            ignored.observedEvents();
            // Corrupt data must never be accepted silently.
        } catch (IOException expected) {
            failed = true;
        }
        check(failed, "tampered journal rejected");
    }

    private void testTruncatedTailDetection() throws Exception {
        Path file = newJournalPath();
        try (var journal = new OrderEventJournal(file)) {
            journal.accept(event("a", "r", "o", "2026-01-01T10:00:00Z", "OPEN", 100));
        }
        String record = Files.readString(file, StandardCharsets.UTF_8);
        Files.writeString(file, record.substring(0, record.length() - 1), StandardCharsets.UTF_8);
        boolean failed = false;
        try (var ignored = new OrderEventJournal(file)) {
            ignored.observedEvents();
        } catch (IOException expected) {
            failed = true;
        }
        check(failed, "truncated record rejected");
    }

    private void testExclusiveLockAndClose() throws Exception {
        Path file = newJournalPath();
        try (var journal = new OrderEventJournal(file)) {
            check(journal.observedEvents() == 0, "first lock holder is usable");
            boolean failed = false;
            try (var ignored = new OrderEventJournal(file)) {
                ignored.observedEvents();
            } catch (IOException expected) {
                failed = true;
            }
            check(failed, "second writer cannot acquire journal lock");
        }
        try (var journal = new OrderEventJournal(file)) {
            check(journal.observedEvents() == 0, "lock released after close");
        }
    }

    private void testInvalidInputNotAppended() throws Exception {
        Path file = newJournalPath();
        try (var journal = new OrderEventJournal(file)) {
            boolean failed = false;
            try {
                journal.accept(null);
            } catch (NullPointerException expected) {
                failed = true;
            }
            check(failed, "null event rejected");
            check(Files.size(file) == 0, "invalid event not written");
            String huge = "x".repeat(20_000);
            failed = false;
            try {
                journal.accept(event(huge, "r", "o", "2026-01-01T10:00:00Z", "OPEN", 100));
            } catch (IllegalArgumentException expected) {
                failed = true;
            }
            check(failed, "oversized record rejected");
            check(Files.size(file) == 0, "oversized record not written");
        }
    }

    private void testConcurrentAppend() throws Exception {
        Path file = newJournalPath();
        var payload = event("same", "r", "o", "2026-01-01T10:00:00Z", "OPEN", 200);
        try (var journal = new OrderEventJournal(file);
             var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Callable<Outcome>> jobs = new ArrayList<>();
            for (int i = 0; i < 20; i++) jobs.add(() -> journal.accept(payload).outcome());
            var futures = pool.invokeAll(jobs);
            int applied = 0;
            int duplicate = 0;
            for (var result : futures) {
                if (result.get() == Outcome.APPLIED) applied++;
                else duplicate++;
            }
            check(applied == 1, "exactly one concurrent delivery applied");
            check(duplicate == 19, "remaining deliveries deduplicated");
            check(journal.observedEvents() == 1, "only one unique event in memory");
        }
        try (var journal = new OrderEventJournal(file)) {
            check(journal.observedEvents() == 1, "replay deduplicates concurrent append history");
            check(journal.get("r", "o").orElseThrow().revision() == 1,
                    "concurrent history reconstructs one revision");
        }
    }
}
