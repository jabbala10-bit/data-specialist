# Event-Driven Architecture at Scale — Principal Engineer Reference

> File 7 of 7 in the Message Queues & Event Streaming series — the capstone, synthesizing Files 1-6's mechanics into the higher-level architectural patterns (event sourcing, CQRS, sagas) this entire reference set has referenced from the Java System Design guide onward, now given full infrastructure treatment.

**Series:** 1. Messaging Fundamentals · 2. Kafka Architecture Deep Dive · 3. Kafka Producers & Consumers · 4. Exactly-Once Semantics · 5. Schema Management · 6. RabbitMQ & Alternative Brokers · **7. Event-Driven Architecture at Scale (this file)**

---

## Table of Contents

1. [Event Sourcing Infrastructure, Concretely](#1-event-sourcing-infrastructure-concretely)
2. [CQRS Infrastructure — Projections Fed by Kafka](#2-cqrs-infrastructure--projections-fed-by-kafka)
3. [Kafka Streams & ksqlDB for Stream Processing](#3-kafka-streams--ksqldb-for-stream-processing)
4. [Saga Orchestration via Event Streaming](#4-saga-orchestration-via-event-streaming)
5. [Monitoring Event-Driven Pipelines End-to-End](#5-monitoring-event-driven-pipelines-end-to-end)
6. [Common Event-Driven Anti-Patterns](#6-common-event-driven-anti-patterns)
7. [A Worked End-to-End Architecture — Closing the Series](#7-a-worked-end-to-end-architecture--closing-the-series)

---

## 1. Event Sourcing Infrastructure, Concretely

### 1.1 Kafka as the Event Store, Directly

```
Directly extending the Java System Design guide's §6.5 event-sourcing discussion and the
Multi-Agent guide's §5.2 event-sourced-workflow-state pattern: a Kafka COMPACTED topic
(Kafka Architecture guide §4.3) can serve AS the event store itself — every state-
changing event for an entity (keyed by entity ID, ensuring partition-scoped ordering,
File 1 §4.2 of this series) is APPENDED, and the topic's RETENTION (or compaction
policy) determines how much history is genuinely RECOVERABLE by replay.
```

### 1.2 Snapshotting — Avoiding Replaying the Entire History Every Time

```java
// Directly the System Design guide's §6.5 closing caveat ("replaying a LONG event
// history... can become slow without periodic SNAPSHOTTING") made concrete with Kafka infrastructure:
public class AccountSnapshotStore {
    // Snapshots themselves stored in a SEPARATE Kafka COMPACTED topic, keyed by entity ID —
    // "latest snapshot per entity," directly the Kafka Architecture guide §4.3 compaction pattern
    public AccountState reconstructState(String accountId) {
        AccountState snapshot = loadLatestSnapshot(accountId); // O(1)-ish — just the
                                                                    // compacted topic's latest value
        List<AccountEvent> eventsSinceSnapshot = readEventsAfter(accountId, snapshot.offset());
        return eventsSinceSnapshot.stream().reduce(snapshot, AccountState::apply); // replay
    }                                                                                  // ONLY the
}                                                                                          // events SINCE
                                                                                              // the snapshot,
                                                                                              // not the entire history
```

### 1.3 The Genuine Audit-Trail Value, Restated With Kafka-Specific Precision

```
Directly the Database Architecture File 6 §5 audit-trail discussion and the AI/Agentic
guide's File 6 §4 traceability requirement: a Kafka-backed event store provides a
NATURAL, IMMUTABLE, REPLAYABLE audit log — directly satisfying regulated-industry
traceability requirements WITHOUT a SEPARATE, bespoke audit-logging system, since the
EVENT STORE ITSELF, by construction, already IS the complete history — a genuinely
efficient piece of architecture reuse this series has flagged repeatedly across different
contexts (the Multi-Agent guide's §5.2 explicitly noted this SAME dual-purpose benefit
for agent-workflow state specifically).
```

---

## 2. CQRS Infrastructure — Projections Fed by Kafka

### 2.1 The Standard Pipeline

```
WRITE side: a command handler validates business rules and produces an EVENT (§1) to a
  Kafka topic (the SOURCE OF TRUTH for what happened).
PROJECTION consumers (each an independent Kafka consumer group, File 3 §4 of this
  series) READ this event stream and maintain THEIR OWN, independently-optimized READ
  MODEL — directly the Java System Design guide's §4.5 CQRS discussion, now with the
  EXACT messaging infrastructure connecting write and read sides made fully explicit.
```

### 2.2 Multiple, Differently-Shaped Projections From the SAME Event Stream

```java
@KafkaListener(topics = "order-events", groupId = "order-summary-projection")
public void updateOrderSummaryView(OrderEvent event) {
    // maintains a DENORMALIZED, dashboard-optimized read model — directly the
    // Wide-Column scenarios guide's §2 "one event, multiple denormalized destination
    // tables" pattern, here with KAFKA as the event-distribution mechanism feeding
    // MULTIPLE INDEPENDENT projection consumers, each free to use whichever DATABASE
    // TECHNOLOGY (Database Architecture File 7's decision framework) best suits ITS
    // specific read pattern — a relational summary table here, perhaps a search index
    // or a graph database (Database Architecture File 3) for a DIFFERENT projection
    // consuming the SAME underlying event stream entirely independently
    orderSummaryRepository.upsert(projectSummary(event));
}

@KafkaListener(topics = "order-events", groupId = "fraud-detection-graph-projection")
public void updateFraudGraph(OrderEvent event) {
    // a COMPLETELY DIFFERENT projection, from the SAME source topic, feeding the
    // Graph Database scenarios guide's §8 fraud-ring-detection graph specifically —
    // directly demonstrating Kafka's pub-sub fan-out (File 1 §2.2) enabling GENUINELY
    // INDEPENDENT, differently-shaped derived views from ONE canonical event source
    fraudGraphService.recordTransaction(event);
}
```

### 2.3 Rebuilding a Projection From Scratch — The Replay Benefit, Concretely Realized

```
If a projection's logic CHANGES (a new field needed in the read model, a bug-fix to
projection logic), the FIX is to simply RESET that consumer group's offset to the
BEGINNING of the topic (Kafka Architecture guide §4.2's retention permitting) and let it
REPROCESS the entire history, REBUILDING the read model from scratch — directly the
System Design guide's §6.5 "derive NEW projections RETROACTIVELY from the full event
history" benefit, now a CONCRETE, OPERATIONALLY ROUTINE procedure (`kafka-consumer-groups.sh
--reset-offsets`) rather than an abstract architectural promise.
```

---

## 3. Kafka Streams & ksqlDB for Stream Processing

### 3.1 Kafka Streams — Topology-Based Processing, In Your Own Application

```java
StreamsBuilder builder = new StreamsBuilder();
KStream<String, OrderEvent> orders = builder.stream("order-events");
KTable<String, Long> orderCountByCustomer = orders
    .groupBy((key, order) -> order.customerId())
    .count(); // a KTable — directly Kafka's "compacted-topic-as-current-state" abstraction
              // (Kafka Architecture guide §4.3) given a FIRST-CLASS streaming-DSL representation
orderCountByCustomer.toStream().to("customer-order-counts");
```

```
This runs WITHIN your OWN application's JVM process (a regular Spring Boot application,
just using the kafka-streams library) — directly the Java Concurrency guide's general
"prefer a well-tested library over hand-rolling the pattern" philosophy, here for
STREAM AGGREGATION specifically: Kafka Streams handles the exactly-once processing
(File 4 §6 of this series), state-store fault-tolerance, and rebalancing-aware
parallelism automatically, rather than requiring hand-rolled consumer-group logic for an aggregation this common.
```

### 3.2 ksqlDB — SQL Over Streaming Data

```sql
CREATE TABLE order_count_by_customer AS
  SELECT customer_id, COUNT(*) AS order_count
  FROM order_events
  GROUP BY customer_id
  EMIT CHANGES;
```

```
ksqlDB exposes the SAME underlying stream-processing capability (effectively, Kafka
Streams under the hood) via a SQL-like interface — directly lowering the barrier for
teams with STRONG SQL fluency (Database Architecture File 1) but LESS Java-stream-
processing-API familiarity, at the cost of LESS fine-grained control than hand-written
Kafka Streams topologies provide for genuinely complex processing logic — the SAME
"declarative convenience vs full programmatic control" trade-off as Spring Data JPA's
derived-query-methods (Spring Data guide §1.2) vs hand-written `@Query` JPQL.
```

---

## 4. Saga Orchestration via Event Streaming

### 4.1 Choreographed Sagas — Directly Realized With Kafka

```
Directly the Java System Design guide's §4.6 choreographed-saga discussion, now with
its EXACT messaging infrastructure made concrete: each service independently CONSUMES
events relevant to it and PUBLISHES its own completion/failure events — Kafka's pub-sub
model (File 1 §2.2) is PRECISELY what enables this fully decoupled choreography, with
NO central orchestrator needing to know about every participating service in advance.
```

```java
@KafkaListener(topics = "order-placed")
public void onOrderPlaced(OrderPlacedEvent event) {
    boolean reserved = inventoryService.reserve(event.items());
    if (reserved) {
        kafkaTemplate.send("inventory-reserved", event.orderId(), new InventoryReservedEvent(event.orderId()));
    } else {
        kafkaTemplate.send("inventory-reservation-failed", event.orderId(), new ReservationFailedEvent(event.orderId()));
    }
}

@KafkaListener(topics = "inventory-reservation-failed")
public void onReservationFailed(ReservationFailedEvent event) {
    // the COMPENSATING action — directly the System Design guide's §4.6 compensation
    // discussion — published as its OWN event, which the ORIGINAL order-placement
    // service consumes to roll back its own state, completing the saga's compensation
    // chain WITHOUT any single component needing global visibility into the whole saga
    kafkaTemplate.send("order-cancelled", event.orderId(), new OrderCancelledEvent(event.orderId()));
}
```

### 4.2 The Traceability Cost, Restated With This Series' Full Context

```
Directly the System Design guide's §4.6 closing caveat about choreographed sagas' harder
traceability — now connecting to §5 of THIS file: WITHOUT deliberate, comprehensive
DISTRIBUTED TRACING (Cloud-Native guide's File 6 §4) propagating a correlation/trace ID
through EVERY event in the saga's chain, reconstructing "what's the current state of
THIS specific order's saga, across five independently-reacting services" becomes
genuinely difficult — directly motivating §5's monitoring discipline as NON-OPTIONAL for
any production choreographed-saga implementation, not a nice-to-have addition.
```

---

## 5. Monitoring Event-Driven Pipelines End-to-End

### 5.1 Trace Propagation Through Kafka Headers

```java
// Directly extending the Cloud-Native guide's File 6 §4.3 trace-correlation discussion
// to Kafka SPECIFICALLY: propagate the trace context as MESSAGE HEADERS, automatically
// continued by the consuming service's OWN span — most modern OpenTelemetry Kafka
// instrumentation handles this transparently, but it's worth knowing EXPLICITLY that
// this propagation does NOT happen "for free" without instrumentation configured correctly
ProducerRecord<String, OrderEvent> record = new ProducerRecord<>("order-events", orderId, event);
record.headers().add("traceparent", currentTraceContext().getBytes()); // W3C Trace
                                                                            // Context format,
                                                                            // Networking series
                                                                            // File 2's
                                                                            // cross-boundary
                                                                            // propagation discussion
```

### 5.2 The Specific Metrics an Event-Driven Pipeline Needs

```
Directly synthesizing File 1 §6.1 (consumer lag), File 3 §8.4 (rebalance rate, send
latency), and the Cloud-Native guide's File 6 §1 (RED metrics, SLOs) into ONE coherent
monitoring baseline: per-TOPIC produce/consume rate and lag, per-CONSUMER-GROUP rebalance
frequency, end-to-end PIPELINE LATENCY (from original event production to FINAL
projection update, §2, using the trace propagation from §5.1 to measure this precisely),
and DEAD-LETTER-QUEUE depth (File 1 §7.2) as a direct signal of poison-message accumulation.
```

### 5.3 Why End-to-End Latency, Specifically, Is the Metric Most Teams Miss

```
Individual hop-level metrics (THIS service's processing time, THAT consumer's lag) can
ALL look healthy independently while the FULL pipeline (production → projection-1 →
projection-2 → final consumer) accumulates SUBSTANTIAL aggregate latency across many
hops — directly the SAME "aggregate vs per-component" blind spot as the Networking
series' File 7 §8's worked incident (where per-region aggregates masked a real,
segment-specific problem) — an explicit, traced, END-TO-END latency metric is the ONLY
reliable way to catch this multi-hop accumulation before it becomes a customer-visible "why did this take 10 seconds" complaint.
```

---

## 6. Common Event-Driven Anti-Patterns

### 6.1 Treating Every Inter-Service Call as an Event

```
Directly the System Design guide's §8.1 closing principle, restated as an explicit
anti-pattern warning: converting a GENUINELY synchronous, immediate-response-needing
interaction ("is this payment authorized, RIGHT NOW, before I let the customer proceed")
into an asynchronous event-based flow purely because "the platform is event-driven"
introduces UNNECESSARY latency/complexity for an interaction that never needed
decoupling in the first place — event-driven architecture is a TOOL for SPECIFIC
decoupling needs, not a uniform architectural religion applied without exception.
```

### 6.2 Events That Are Actually Disguised RPC Calls

```java
// ANTI-PATTERN — an "event" that's really just a synchronous request awkwardly forced
// into event shape, with the producer BLOCKING and POLLING for a "response event"
publishEvent(new CheckInventoryRequest(productId));
InventoryResponse response = pollForResponseEvent(correlationId, timeout); // defeats
                                                                                // the ENTIRE
                                                                                // decoupling
                                                                                // benefit (§1.1
                                                                                // of File 1),
                                                                                // while adding
                                                                                // ALL the
                                                                                // complexity of
                                                                                // async messaging
```

### 6.3 No Schema Discipline, "We'll Just Use JSON and Figure It Out"

```
Directly File 5's entire existence as a dedicated file: skipping schema-registry
discipline (File 5 §3) "to move fast" works fine until the FIRST uncoordinated,
incompatible producer change breaks an unknown downstream consumer — exactly the kind of
problem this entire series' "every gap discovered becomes a permanent process addition"
closing principle exists to prevent BEFORE it happens, not after.
```

### 6.4 Ignoring Consumer Lag Until It's a Crisis

```
Directly File 1 §6's central warning, restated as a NAMED anti-pattern: treating
consumer lag as "something to check if someone complains" rather than a CONTINUOUSLY
MONITORED, ALERTED-ON metric (§5.2) is precisely how a slow-building backlog becomes a
multi-hour, customer-visible outage rather than a quietly-resolved capacity adjustment caught days earlier.
```

---

## 7. A Worked End-to-End Architecture — Closing the Series

**Scenario:** an FDE engagement building an order-management platform for a multi-region retailer, synthesizing every file in this Message Queues & Event Streaming series, and connecting back to the Database, Cloud-Native, and Networking series this entire reference set has built.

```
1. ORDER PLACEMENT (write side, §2.1): a Spring Boot service validates and persists an
   order to its relational system of record (Database Architecture File 1), publishing
   an OrderPlacedEvent via the Transactional Outbox pattern (File 4 §4.1 of this series,
   Spring Cloud guide §5.3) — guaranteeing the DATABASE WRITE and the EVENT PUBLICATION
   never silently diverge.

2. SCHEMA-GOVERNED EVENTS (File 5): OrderPlacedEvent is an Avro-defined schema, registered
   and compatibility-checked in CI (File 5 §8) before any producer deployment — every
   downstream consumer, including ones added MONTHS later by different teams, can rely
   on the schema-registry's compatibility guarantees rather than informal coordination.

3. MULTIPLE INDEPENDENT PROJECTIONS (§2.2): an order-summary relational projection (for
   the customer-facing order-history page), a fraud-detection graph projection
   (Database Architecture File 3's worked design), and an analytics OLAP projection
   (Database Architecture File 1 §1.3's star-schema discussion) — all independently
   consuming the SAME canonical OrderPlacedEvent stream, each free to evolve its OWN
   read-model technology choice without coordinating with the others.

4. SAGA COORDINATION (§4): a choreographed saga across inventory-reservation, payment-
   processing, and shipping-scheduling services, each reacting to and publishing its
   own events — with COMPENSATING events handling the failure paths, and FULL
   distributed tracing (§5.1) propagated through every hop so the saga's current state
   is always reconstructable.

5. MULTI-REGION TOPOLOGY (Kafka Architecture guide §8): the EU region's Kafka cluster
   handles EU-customer orders exclusively (directly the Cloud-Native guide's File 4 §7
   data-residency discipline), with MirrorMaker selectively replicating ONLY the
   non-PII-bearing analytics-relevant event subset to a global analytics cluster —
   never replicating raw order events containing customer PII across the data-residency boundary.

6. RESILIENCE (Java System Design guide §7, Spring Cloud guide §4): every consumer's
   processing logic wraps downstream calls (payment gateway, shipping API) in circuit
   breakers and retries, with a dead-letter topic (File 1 §7.2) catching genuinely
   poison messages for manual review rather than blocking the pipeline indefinitely.

7. OBSERVABILITY (§5, Cloud-Native guide's File 6): per-topic lag, end-to-end saga
   latency, and dead-letter-queue depth are FIRST-CLASS dashboard metrics and alerts
   from DAY ONE of the platform's operation — not retrofitted reactively after a first incident.

This closes the loop on the now 62-file reference set this conversation has built: an
event-driven platform of this sophistication draws SIMULTANEOUSLY on messaging
internals (this series), database architecture (the Database series), cloud-native
deployment (the Cloud-Native series), and the networking fundamentals underneath all of
it (the Networking series) — and the Principal/FDE-differentiating skill, restated one
final time, is the judgment to compose exactly the right subset of all of it for a
SPECIFIC customer's actual constraints, never defaulting to any single tool or pattern
as a universal answer.
```
