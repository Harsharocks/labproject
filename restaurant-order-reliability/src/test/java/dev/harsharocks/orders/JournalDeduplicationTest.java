package dev.harsharocks.orders;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import static dev.harsharocks.orders.ProcessingResult.Outcome;

/** Regression tests for durable event-ID deduplication without redundant fsyncs. */
public final class JournalDeduplicationTest {
    private int assertions;

    public static void main(String[] args) throws Exception {
        var suite = new JournalDeduplicationTest();
        suite.testDuplicateAndConflictDoNotAppend();
        suite.testUniqueRejectedUpdatesRemainDurable();
        suite.testLegacyJournalWithDuplicateRecords();
        suite.testConcurrentRetriesWriteOnlyOnce();
        System.out.println("PASS - " + suite.assertions + " assertions across 4 deduplication scenarios");
    }

    private void check(boolean condition, String message) {
        assertions++;
        if (!condition) throw new AssertionError(message);
    }

    private static OrderEvent event(String id, String restaurant, String order,
                                    String time, String status, long cents) {
        return new OrderEvent(id, restaurant, order, Instant.parse(time), status, cents);
    }

    private static Path journalPath() throws Exception {
        return Files.createTempDirectory("order-dedup-").resolve("events.log");
    }

    private void testDuplicateAndConflictDoNotAppend() throws Exception {
        Path file = journalPath();
        var created = event("e1", "r", "o", "2026-01-01T10:00:00Z", "OPEN", 100);
        var conflicting = event("e1", "r", "o", "2026-01-01T10:00:00Z", "OPEN", 900);
        try (var journal = new OrderEventJournal(file)) {
            check(journal.accept(created).outcome() == Outcome.APPLIED, "first update applied");
            long initialBytes = Files.size(file);
            check(initialBytes > 0, "first event written");
            check(journal.accept(created).outcome() == Outcome.DUPLICATE_EVENT, "retry deduplicated");
            check(Files.size(file) == initialBytes, "retry does not append");
            check(journal.accept(conflicting).outcome() == Outcome.EVENT_ID_CONFLICT, "ID reuse rejected");
            check(Files.size(file) == initialBytes, "ID reuse does not append");
            check(journal.observedEvents() == 1, "only one ID tracked");
        }
        try (var journal = new OrderEventJournal(file)) {
            check(journal.accept(created).outcome() == Outcome.DUPLICATE_EVENT, "retry after restart");
            check(journal.accept(conflicting).outcome() == Outcome.EVENT_ID_CONFLICT, "collision after restart");
            check(journal.get("r", "o").orElseThrow().totalCents() == 100, "state unchanged");
        }
    }

    private void testUniqueRejectedUpdatesRemainDurable() throws Exception {
        Path file = journalPath();
        var paid = event("paid", "r", "o", "2026-01-01T10:05:00Z", "PAID", 100);
        var stale = event("stale", "r", "o", "2026-01-01T10:01:00Z", "OPEN", 100);
        var tie = event("tie", "r", "o", "2026-01-01T10:05:00Z", "CANCELLED", 100);
        var unchanged = event("same", "r", "o", "2026-01-01T10:05:00Z", "PAID", 100);
        try (var journal = new OrderEventJournal(file)) {
            journal.accept(paid);
            long initial = Files.size(file);
            check(journal.accept(stale).outcome() == Outcome.STALE_EVENT, "stale update rejected");
            check(Files.size(file) > initial, "stale ID persisted for future deduplication");
            check(journal.accept(tie).outcome() == Outcome.TIMESTAMP_CONFLICT, "ambiguous tie rejected");
            check(journal.accept(unchanged).outcome() == Outcome.NO_CHANGE, "new ID with same state recorded");
            check(journal.observedEvents() == 4, "all distinct IDs tracked");
        }
        try (var journal = new OrderEventJournal(file)) {
            long size = Files.size(file);
            check(journal.accept(stale).outcome() == Outcome.DUPLICATE_EVENT, "stale ID survives restart");
            check(journal.accept(tie).outcome() == Outcome.DUPLICATE_EVENT, "conflicting-timestamp ID survives");
            check(journal.accept(unchanged).outcome() == Outcome.DUPLICATE_EVENT, "no-change ID survives");
            check(Files.size(file) == size, "retries after restart do not append");
            check(journal.get("r", "o").orElseThrow().status().equals("PAID"), "latest state preserved");
        }
    }

    private void testLegacyJournalWithDuplicateRecords() throws Exception {
        Path file = journalPath();
        var original = event("legacy", "r", "o", "2026-01-01T10:00:00Z", "OPEN", 100);
        try (var journal = new OrderEventJournal(file)) {
            journal.accept(original);
        }
        byte[] record = Files.readAllBytes(file);
        // Previous journal versions persisted every retry. Both records have valid checksums.
        Files.write(file, (new String(record, StandardCharsets.UTF_8)
                + new String(record, StandardCharsets.UTF_8)).getBytes(StandardCharsets.UTF_8));
        try (var journal = new OrderEventJournal(file)) {
            check(journal.observedEvents() == 1, "legacy duplicates replay without adding IDs");
            check(journal.get("r", "o").orElseThrow().revision() == 1, "legacy replay preserves revision");
            long bytes = Files.size(file);
            check(journal.accept(original).outcome() == Outcome.DUPLICATE_EVENT, "legacy retry detected");
            check(Files.size(file) == bytes, "new version does not append another duplicate");
        }
    }

    private void testConcurrentRetriesWriteOnlyOnce() throws Exception {
        Path file = journalPath();
        var original = event("concurrent", "r", "o", "2026-01-01T10:00:00Z", "OPEN", 100);
        try (var journal = new OrderEventJournal(file);
             var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Callable<Outcome>> jobs = new ArrayList<>();
            for (int i = 0; i < 100; i++) jobs.add(() -> journal.accept(original).outcome());
            var futures = pool.invokeAll(jobs);
            int applied = 0;
            int duplicate = 0;
            for (var future : futures) {
                if (future.get() == Outcome.APPLIED) applied++;
                else duplicate++;
            }
            check(applied == 1, "one event applied across 100 concurrent requests");
            check(duplicate == 99, "99 retries deduplicated");
            check(Files.readAllLines(file).size() == 1, "only one physical record appended");
        }
        try (var journal = new OrderEventJournal(file)) {
            check(journal.observedEvents() == 1, "concurrent writes replay to one event");
        }
    }
}
