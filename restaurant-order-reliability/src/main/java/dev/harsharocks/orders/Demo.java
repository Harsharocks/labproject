package dev.harsharocks.orders;

import java.time.Instant;

/** Demonstrates duplicate delivery and out-of-order updates using synthetic data. */
public final class Demo {
    private Demo() {}

    public static void main(String[] args) {
        OrderEventProcessor processor = new OrderEventProcessor();
        OrderEvent created = new OrderEvent("event-001", "restaurant-42", "order-7",
                Instant.parse("2026-10-17T19:00:00Z"), "OPEN", 3200);
        OrderEvent paid = new OrderEvent("event-003", "restaurant-42", "order-7",
                Instant.parse("2026-10-17T19:05:00Z"), "PAID", 3200);
        OrderEvent delayed = new OrderEvent("event-002", "restaurant-42", "order-7",
                Instant.parse("2026-10-17T19:02:00Z"), "UPDATED", 3500);

        for (OrderEvent event : new OrderEvent[] {created, paid, created, delayed}) {
            ProcessingResult result = processor.accept(event);
            System.out.printf("%-10s -> %-17s | %s%n",
                    event.eventId(), result.outcome(), result.current());
        }
        System.out.println("Final: " + processor.get("restaurant-42", "order-7").orElseThrow());
    }
}
