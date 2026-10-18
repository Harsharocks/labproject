# Restaurant Order Reliability

A self-contained Java 21 case study for reliably projecting restaurant order updates from an at-least-once, potentially out-of-order event stream. Inspired by [Toast's public orders webhook documentation](https://doc.toasttab.com/doc/devguide/devOrdersWebhookRef.html), **not** an official Toast integration or a claim about Toast's internal systems.

## Problem statement

A restaurant service receives full snapshots of an order through event notifications. In practice, network retries or parallel workers may deliver the same event more than once, and an older notification may arrive after a newer order state. Blindly applying every update can roll back a payment status or trigger duplicate downstream work.

The goal is to keep one deterministic, current order projection per restaurant/order while resisting retries, stale updates, and conflicting data. The demo uses fictitious order IDs and dollar amounts in integer cents.

## Design and approach

- **Idempotency:** store seen event IDs. Exact replays return `DUPLICATE_EVENT`; reuse of an ID with different content returns `EVENT_ID_CONFLICT`.
- **Ordering:** compare `Instant` event timestamps. An older update returns `STALE_EVENT` and does not overwrite accepted state.
- **Ambiguity:** equal timestamps with identical state produce `NO_CHANGE`; equal timestamps with different state return `TIMESTAMP_CONFLICT` rather than arbitrarily picking a winner.
- **Tenant isolation:** state is keyed by `(restaurantId, orderId)`, not only the order ID.
- **Concurrency:** the acceptance operation is synchronized, so deduplication and projection happen atomically in a single JVM.
- **Observability:** callers receive a typed outcome and the current state, including a local revision counter.

The core classes are pure Java without a framework, database, network credentials or external dependencies. An HTTP adapter, durable event inbox and database transaction boundary would be the next production steps.

## Requirements and installation

Install a **JDK 21 or later** (for example Eclipse Temurin 21) and verify `java -version` and `javac -version` both work. Git is optional if you download the source directly. No Maven, Gradle or external libraries are needed for this increment.

Clone:

```sh
git clone https://github.com/Harsharocks/labproject.git
cd labproject/restaurant-order-reliability
```

## Build and execute

### macOS / Linux (Bash)

```sh
mkdir -p out
javac -d out src/main/java/dev/harsharocks/orders/*.java
java -cp out dev.harsharocks.orders.Demo
```

### Windows (PowerShell)

```powershell
New-Item -ItemType Directory -Force out | Out-Null
javac -d out (Get-ChildItem src/main/java/dev/harsharocks/orders/*.java)
java -cp out dev.harsharocks.orders.Demo
```

The example processes `OPEN`, then `PAID`, then a duplicate `OPEN`, then a delayed `UPDATED`. Expected outcomes: `APPLIED`, `APPLIED`, `DUPLICATE_EVENT`, `STALE_EVENT`. The final state remains `PAID` at revision `2`.

## Run tests

### macOS / Linux (Bash)

```sh
mkdir -p out
javac -d out src/main/java/dev/harsharocks/orders/*.java src/test/java/dev/harsharocks/orders/*.java
java -cp out dev.harsharocks.orders.OrderEventProcessorTest
```

### Windows (PowerShell)

```powershell
New-Item -ItemType Directory -Force out | Out-Null
javac -d out (Get-ChildItem src/main/java/dev/harsharocks/orders/*.java) (Get-ChildItem src/test/java/dev/harsharocks/orders/*.java)
java -cp out dev.harsharocks.orders.OrderEventProcessorTest
```

The dependency-free test suite checks duplicates, stale updates, timestamp ties, event ID collisions, multi-restaurant isolation, invalid data and concurrent duplicate delivery (100 virtual threads). It exits non-zero on failure.

## Limitations and tradeoffs

- **In-memory only:** event deduplication and order projections disappear on process restart. Use a transactional database and a unique event-ID index for durability and multiple instances.
- **Event time is not authoritative sequencing:** equal timestamps with differing snapshots need upstream sequence numbers, a version field or reconciliation against the authoritative API. A newer timestamp also assumes upstream timestamps reflect the canonical state order.
- **No webhook endpoint or signing yet:** the demo models normalized events, not Toast payload parsing or authentication. A real integration must verify request signatures, limit request size, safely parse JSON and avoid storing sensitive customer information.
- **No exactly-once external effects:** the processor only changes in-memory order state. Transactional outbox/inbox processing would be required before sending payments, emails or other side effects.
- **No bounded retention:** seen-event IDs are kept forever for demonstration. A production TTL requires a documented replay window and persistent deduplication policy.

## Roadmap

1. Add an HTTP JSON adapter with request validation and synthetic webhook payload fixtures.
2. Persist event inbox and current order projection in PostgreSQL with atomic updates.
3. Add an outbox for downstream fulfillment notifications and failure/retry testing.
4. Publish a CI workflow and benchmark ordering and duplicate throughput.

## Public references

- [Toast orders webhook reference](https://doc.toasttab.com/doc/devguide/devOrdersWebhookRef.html)
- [Toast webhook basics and message signing](https://doc.toasttab.com/doc/devguide/apiWebhookBasics.html)
- [Toast webhook headers](https://doc.toasttab.com/doc/devguide/apiHttpHeaders.html)

All data in this project is fictional. This case study is an independently designed illustration of integration-engineering techniques.
