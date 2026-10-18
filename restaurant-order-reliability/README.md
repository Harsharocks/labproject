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

The core classes are pure Java without a framework, database, network credentials or external dependencies. An HTTP adapter and database transaction boundary would be the next production steps. The optional `OrderEventJournal` supports single-process replay durability.

## Requirements and installation

Install a **JDK 21 or later** (for example Eclipse Temurin 21) and verify `java -version` and `javac -version` both work. Git is optional if you download the source directly. No Maven, Gradle or external libraries are needed for this increment.

Clone the feature branch while the journal work is under review:

```sh
git clone --branch feature/order-event-journal https://github.com/Harsharocks/labproject.git
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

## Durable journal demo

`OrderEventJournal` is an opt-in, single-machine event inbox. For previously unseen event IDs, it appends a versioned, SHA-256-checksummed record and calls `FileChannel.force(true)` before updating in-memory state. On restart it replays the file, preserving order revisions and previously seen event IDs. An exclusive file lock prevents two local writers; corrupt or incomplete records stop startup rather than being silently ignored.

Compile the main sources as shown above, then run this command **twice** using the same journal path:

```sh
java -cp out dev.harsharocks.orders.JournalDemo data/orders.journal
```

First run: `OPEN` and `PAID` both return `APPLIED`. Second run: both return `DUPLICATE_EVENT` and the recovered order remains `PAID`, revision 2. The demo uses fictional IDs and amounts; `data/` is excluded by `.gitignore`. The same command works in PowerShell.

**Retry write amplification:** Exact duplicate deliveries and conflicting reuse of an already-seen event ID return immediately without another append or fsync. Unique IDs are persisted even when their updates are stale or conflict with an existing timestamp, so retries retain their idempotency semantics after restart. Older journal files containing redundant duplicate records still replay correctly. This reduces redundant disk writes during at-least-once delivery, but is not a throughput benchmark.

This is not a distributed or tamper-proof event store. Checksums detect accidental damage, not malicious modification. Each distinct event ID consumes storage; journal growth and replay cost are unbounded. A failed write blocks further writes until the file is inspected and reopened. Do not share the file across hosts or use it for real payments. A transactional database inbox/outbox is the next production step.

## Run tests

### macOS / Linux (Bash)

```sh
mkdir -p out
javac -Xlint:all -d out src/main/java/dev/harsharocks/orders/*.java src/test/java/dev/harsharocks/orders/*.java
java -cp out dev.harsharocks.orders.OrderEventProcessorTest
java -cp out dev.harsharocks.orders.OrderEventJournalTest
java -cp out dev.harsharocks.orders.JournalDeduplicationTest
```

### Windows (PowerShell)

```powershell
New-Item -ItemType Directory -Force out | Out-Null
javac -Xlint:all -d out (Get-ChildItem src/main/java/dev/harsharocks/orders/*.java) (Get-ChildItem src/test/java/dev/harsharocks/orders/*.java)
java -cp out dev.harsharocks.orders.OrderEventProcessorTest
java -cp out dev.harsharocks.orders.OrderEventJournalTest
java -cp out dev.harsharocks.orders.JournalDeduplicationTest
```

The dependency-free tests cover duplicate and stale events, timestamp ties, tenant isolation, restart recovery, persisted rejected events, corrupted/truncated journals, file locking, invalid inputs, and concurrent appends. The deduplication suite additionally checks that retries do not grow the journal, legacy files remain readable, distinct rejected events remain durable, and 100 concurrent retries produce one physical record. All three suites run in GitHub Actions.

## Limitations and tradeoffs

- **Opt-in persistence:** `OrderEventProcessor` remains in-memory. `OrderEventJournal` replays a local file after restart, but cannot coordinate multiple hosts. Use a transactional database and unique event keys for production.
- **Event time is not authoritative sequencing:** equal timestamps with differing snapshots need upstream sequence numbers, a version field or reconciliation against the authoritative API. A newer timestamp also assumes upstream timestamps reflect the canonical state order.
- **No webhook endpoint or signing yet:** the demo models normalized events, not Toast payload parsing or authentication. A real integration must verify request signatures, limit request size, safely parse JSON and avoid storing sensitive customer information.
- **No exactly-once external effects:** the processor only changes in-memory order state. Transactional outbox/inbox processing would be required before sending payments, emails or other side effects.
- **No bounded retention:** seen-event IDs are kept forever for demonstration. A production TTL requires a documented replay window and persistent deduplication policy.

## Roadmap

1. Add an HTTP JSON adapter with request validation and synthetic webhook payload fixtures.
2. Persist event inbox and current order projection in PostgreSQL with atomic updates.
3. Add an outbox for downstream fulfillment notifications and failure/retry testing.
4. Benchmark ordering and duplicate throughput, and add journal checkpointing/compaction.

## Public references

- [Toast orders webhook reference](https://doc.toasttab.com/doc/devguide/devOrdersWebhookRef.html)
- [Toast webhook basics and message signing](https://doc.toasttab.com/doc/devguide/apiWebhookBasics.html)
- [Toast webhook headers](https://doc.toasttab.com/doc/devguide/apiHttpHeaders.html)
- [Toast webhook subscription checklist: idempotence using event GUIDs](https://doc.toasttab.com/doc/cookbook/apiWebhookUsageChecklist.html)

All data in this project is fictional. This case study is an independently designed illustration of integration-engineering techniques.
