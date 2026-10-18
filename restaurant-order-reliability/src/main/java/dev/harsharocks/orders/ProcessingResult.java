package dev.harsharocks.orders;

public record ProcessingResult(Outcome outcome, OrderSnapshot current) {
    public enum Outcome {
        APPLIED,
        DUPLICATE_EVENT,
        STALE_EVENT,
        NO_CHANGE,
        TIMESTAMP_CONFLICT,
        EVENT_ID_CONFLICT
    }
}
