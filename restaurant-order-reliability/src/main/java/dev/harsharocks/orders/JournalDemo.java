package dev.harsharocks.orders;

import java.nio.file.Path;
import java.time.Instant;

/** Run twice with the same path to see state and event IDs survive a restart. */
public final class JournalDemo {
    private JournalDemo() {}

    public static void main(String[] args) throws Exception {
        if (args.length > 1) {
            throw new IllegalArgumentException("Usage: JournalDemo [journal-file]");
        }
        Path file = args.length == 1 ? Path.of(args[0]) : Path.of("data/orders.journal");
        try (OrderEventJournal journal = new OrderEventJournal(file)) {
            OrderEvent created = new OrderEvent("demo-001", "restaurant-42", "order-7",
                    Instant.parse("2026-10-17T19:00:00Z"), "OPEN", 3200);
            OrderEvent paid = new OrderEvent("demo-002", "restaurant-42", "order-7",
                    Instant.parse("2026-10-17T19:05:00Z"), "PAID", 3200);
            System.out.println("OPEN event: " + journal.accept(created).outcome());
            System.out.println("PAID event: " + journal.accept(paid).outcome());
            System.out.println("Current order: " + journal.get("restaurant-42", "order-7").orElseThrow());
        }
    }
}
