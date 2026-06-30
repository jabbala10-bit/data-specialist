# Exactly-Once Semantics & Transactional Messaging — Principal Engineer Reference

> File 4 of 7 in the Message Queues & Event Streaming series. Unpacks what "exactly-once" actually means mechanically, where Kafka's native transactions deliver it genuinely, and — critically — where the guarantee stops and idempotency (Java System Design guide §4.4) must take over.

**Series:** 1. Messaging Fundamentals · 2. Kafka Architecture Deep Dive · 3. Kafka Producers & Consumers · **4. Exactly-Once Semantics (this file)** · 5. Schema Management · 6. RabbitMQ & Alternative Brokers · 7. Event-Driven Architecture at Scale

---

## Table of Contents

1. [Why Exactly-Once Is Fundamentally Hard](#1-why-exactly-once-is-fundamentally-hard)
2. [Kafka Transactions — The Mechanism](#2-kafka-transactions--the-mechanism)
3. [The Read-Process-Write Pattern](#3-the-read-process-write-pattern)
4. [Transactional Outbox, Revisited With Kafka Specifics](#4-transactional-outbox-revisited-with-kafka-specifics)
5. [Idempotent Consumers — The Application-Level Complement](#5-idempotent-consumers--the-application-level-complement)
6. [Exactly-Once in Kafka Streams](#6-exactly-once-in-kafka-streams)
7. [Where Exactly-Once Genuinely Ends](#7-where-exactly-once-genuinely-ends)
8. [Testing Exactly-Once Guarantees](#8-testing-exactly-once-guarantees)

---

## 1. Why Exactly-Once Is Fundamentally Hard

### 1.1 The Two Separate Problems Bundled Into "Exactly-Once"

```
Directly decomposing the File 1 §3.3 caveat into its TWO genuinely distinct sub-problems:
(1) DUPLICATE PREVENTION — ensuring a message isn't processed MORE than once (directly
    File 3 §3's idempotent-producer discussion, and §5 of this file's idempotent-consumer
    discussion), and
(2) ATOMICITY ACROSS MULTIPLE OPERATIONS — ensuring a "read message, do work, write
    result(s)" sequence either ALL happens or NONE of it does, even across MULTIPLE
    Kafka topics/partitions — directly the SAME atomicity concern as a relational
    transaction (Database Architecture File 1 §5), but spanning a DISTRIBUTED log
    rather than a single database's rows.
Kafka's transactional API (§2) solves BOTH simultaneously, specifically WITHIN Kafka's
own boundary — this scoping precision is exactly what File 1 §3.3 flagged as the critical caveat.
```

---

## 2. Kafka Transactions — The Mechanism

### 2.1 The Transactional Producer API

```java
producer.initTransaction();
try {
    producer.beginTransaction();
    producer.send(new ProducerRecord<>("inventory-reserved", orderId, reservationEvent));
    producer.send(new ProducerRecord<>("notifications", orderId, notificationEvent));
    producer.commitTransaction(); // BOTH sends become visible to consumers ATOMICALLY —
} catch (Exception e) {              // either BOTH appear, or NEITHER does
    producer.abortTransaction();
}
```

### 2.2 How This Actually Works — Transaction Markers

```
Kafka writes special TRANSACTION MARKER records into EVERY partition involved in a
transaction, indicating commit or abort — consumers configured with
`isolation.level=read_committed` (§2.3) FILTER OUT records belonging to an ABORTED (or
still in-progress) transaction, seeing ONLY records from COMMITTED transactions —
directly analogous to the Database Architecture File 1 §5.2 MVCC mechanism: multiple
"versions" of in-flight data exist simultaneously, and the READER'S isolation
configuration determines WHICH versions it's permitted to see, rather than the WRITER
needing to somehow physically prevent uncommitted data from ever being written at all.
```

### 2.3 `isolation.level=read_committed` — The Consumer-Side Requirement

```properties
isolation.level=read_committed  # WITHOUT this, a consumer sees EVERY record, including
                                    # ones from a transaction that was later ABORTED —
                                    # directly defeating the entire point of using
                                    # transactions in the first place; this setting is a
                                    # NON-OPTIONAL companion to producer-side transactions,
                                    # not an independent tuning knob
```

---

## 3. The Read-Process-Write Pattern

### 3.1 The Canonical Exactly-Once Use Case — Kafka-to-Kafka Processing

```java
// A consumer reads from topic A, transforms, writes to topic B — and COMMITS THE
// CONSUMER OFFSET as PART OF THE SAME TRANSACTION as the write to topic B
producer.initTransaction();
while (running) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
    producer.beginTransaction();
    for (ConsumerRecord<String, String> record : records) {
        OrderEvent transformed = transform(record.value());
        producer.send(new ProducerRecord<>("transformed-orders", record.key(), transformed));
    }
    // sendOffsetsToTransaction makes the OFFSET COMMIT itself part of the SAME atomic
    // transaction as the topic-B writes — directly solving §1.1's atomicity problem
    // for the SPECIFIC "consume from A, produce to B" pattern
    producer.sendOffsetsToTransaction(currentOffsets(records), consumerGroupMetadata);
    producer.commitTransaction();
}
```

### 3.2 Why This Specifically Solves the Read-Process-Write Atomicity Problem

```
WITHOUT this pattern, a crash BETWEEN "write to topic B" and "commit the offset for
topic A" would, on restart, RE-READ the same record from topic A (since its offset was
never committed) and RE-PROCESS it, producing a DUPLICATE write to topic B — directly
File 1 §3.2's at-least-once duplicate risk. By making the OFFSET COMMIT and the
DOWNSTREAM WRITE part of the SAME atomic transaction, EITHER BOTH happen (the offset
genuinely advanced AND the downstream write genuinely occurred) or NEITHER does (a
restart correctly RE-PROCESSES the record, but the PRIOR transaction's write was
ABORTED and is invisible to `read_committed` consumers, §2.3) — genuinely eliminating the duplicate, not just reducing its likelihood.
```

---

## 4. Transactional Outbox, Revisited With Kafka Specifics

### 4.1 Directly Extending the Spring Cloud Guide's §5.3 Discussion

```
The Transactional Outbox pattern (Spring Cloud guide §5.3) solves a DIFFERENT, though
related, atomicity problem than §3's read-process-write: atomically writing to a
RELATIONAL DATABASE and PUBLISHING a Kafka event — a boundary Kafka's OWN transactions
(§2) cannot span, since they only coordinate ACROSS KAFKA topics/partitions, NOT across
an EXTERNAL database's transaction. The outbox pattern works around this by making the
"event to publish" part of the SAME LOCAL DATABASE TRANSACTION as the business write,
with a SEPARATE relay process (CDC, Database Architecture File 7 §4.2, or a polling
publisher) handling the EVENTUALLY-CONSISTENT relay to Kafka.
```

### 4.2 The Combined Pattern for Maximum End-to-End Guarantee

```
For a pipeline genuinely needing "database write + Kafka publish + downstream Kafka
processing," ALL exactly-once: (1) Transactional Outbox (§4.1) for the database-to-Kafka
boundary, (2) Kafka's native transactions + `read_committed` (§2-3) for the Kafka-to-Kafka
processing stages, and (3) idempotent handling (§5) at the FINAL boundary where a Kafka
event triggers an effect in ANOTHER external system (a different database, a third-party
API) that Kafka's transactions cannot reach — directly synthesizing every mechanism
covered across this series' coverage of the dual-write problem (Spring Cloud guide §5.3,
System Design guide §4.4) into one complete, end-to-end picture.
```

---

## 5. Idempotent Consumers — The Application-Level Complement

### 5.1 Why This Remains Necessary Even With Kafka Transactions Enabled

```
Kafka's transactional guarantees (§2-3) protect the KAFKA-INTERNAL portion of a
pipeline — they do NOT protect against a CONSUMER that processes a message, triggers an
EXTERNAL side effect (calling a payment API, writing to a non-transactional external
database), and THEN crashes before committing its offset — on restart, the SAME message
is redelivered (correctly, per at-least-once semantics, File 1 §3.2) and the EXTERNAL
side effect risks happening TWICE, since Kafka's transaction boundary never extended to cover that external call at all.
```

### 5.2 The Idempotency-Key Pattern, Applied to Kafka Consumption Specifically

```java
@KafkaListener(topics = "payment-requests")
public void processPayment(PaymentRequest request, Acknowledgment ack) {
    // directly the System Design guide's §4.4 idempotency-key pattern, here using the
    // Kafka message's OWN key (or a dedicated idempotency field within the payload) as
    // the deduplication key against the EXTERNAL system being called
    if (paymentService.alreadyProcessed(request.idempotencyKey())) {
        ack.acknowledge(); // already done — SAFE to acknowledge and move on without
        return;                // re-triggering the external effect
    }
    paymentService.charge(request); // records the idempotencyKey as part of THIS write,
    ack.acknowledge();                  // atomically with the charge itself, per the
}                                          // System Design guide §4.4's exact mechanism
```

---

## 6. Exactly-Once in Kafka Streams

### 6.1 `processing.guarantee=exactly_once_v2`

```java
Properties props = new Properties();
props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
```

```
Kafka Streams (the topology-based stream-processing library built on top of the core
Kafka client APIs) provides exactly-once GUARANTEES for its ENTIRE read-process-write
topology with this SINGLE configuration flag — internally, this is implemented via
PRECISELY the §3 read-process-write transactional pattern, automated and managed by the
framework so application code doesn't need to manually orchestrate
`beginTransaction`/`sendOffsetsToTransaction`/`commitTransaction` calls itself — directly
the SAME "the framework handles a well-understood pattern correctly, so you don't have
to hand-roll it" value proposition as Spring Data JPA's repository abstraction (Spring
Data guide §1.1) handling routine CRUD, here applied to exactly-once stream processing instead.
```

### 6.2 The Performance Trade-off

```
Directly the §2's latency-vs-durability trade-off, now at the STREAM-PROCESSING-THROUGHPUT
level: exactly-once processing requires the additional transactional coordination
overhead (§2.2's transaction markers, coordinated commits) on EVERY processing step —
measurably LOWER throughput than `at_least_once` processing (Kafka Streams' alternative,
weaker guarantee) — apply exactly-once SPECIFICALLY where the business cost of a
duplicate (double-charging, double-counting a financial aggregate) clearly outweighs
this throughput cost, rather than defaulting to it universally for every stream topology
regardless of actual duplicate-sensitivity.
```

---

## 7. Where Exactly-Once Genuinely Ends

### 7.1 The Boundary, Stated One Final Time, Precisely

```
Directly closing the loop on File 1 §3.3's original framing: Kafka's exactly-once
mechanisms (§2-3, §6) provide a GENUINE, strong guarantee WITHIN the Kafka ecosystem
(topic-to-topic, or Kafka Streams' internal state stores) — the MOMENT a pipeline's
effect crosses INTO an external system Kafka doesn't natively coordinate with (any
database write not using the Transactional Outbox pattern, §4; any external API call,
§5), the guarantee reverts to AT-LEAST-ONCE, requiring APPLICATION-LEVEL idempotency
(§5.2) to achieve an EFFECTIVELY-exactly-once OUTCOME, even though the underlying
DELIVERY mechanism is no longer providing that guarantee natively.
```

### 7.2 The Practical Synthesis for an FDE Engagement

```
When a customer or colleague asks "does our pipeline guarantee exactly-once," the
PRINCIPAL-LEVEL answer is never a bare "yes" or "no" — it's a PRECISE description of
EXACTLY WHICH BOUNDARIES carry the native Kafka guarantee (§2-3, §6) and WHICH boundaries
rely on APPLICATION-LEVEL idempotency (§5) instead — directly the SAME "every claim
should be precise about its actual scope" discipline as the AI/Agentic guide's File 6
EU AI Act discussion's insistence on precise risk-tier classification rather than a vague,
overconfident compliance claim.
```

---

## 8. Testing Exactly-Once Guarantees

### 8.1 Chaos-Testing the Failure Modes That Actually Matter

```
Directly extending the Cloud-Native guide's File 6 §8 chaos-engineering discussion to
THIS specific guarantee: the genuinely valuable test isn't "does the happy path work" —
it's DELIBERATELY killing a producer/consumer MID-TRANSACTION (between §2.1's
`beginTransaction` and `commitTransaction`) and verifying: (1) the partial writes are
NEVER visible to a `read_committed` consumer (§2.3), and (2) RESTARTING the producer/
consumer correctly RE-PROCESSES the affected messages WITHOUT producing a duplicate
downstream effect — directly the same "verify the resilience CLAIM, don't just trust the
configuration," chaos-engineering principle, applied to a transactional-correctness claim specifically.
```

### 8.2 A Concrete Test Harness Pattern

```java
@Test
void crashDuringTransactionDoesNotProduceDuplicates() {
    // 1. Start a transaction, send records, but DELIBERATELY kill the producer process
    //    (or simulate via a test harness injecting a failure) BEFORE commitTransaction()
    // 2. Restart a fresh producer/consumer instance against the SAME topics
    // 3. Assert: the aborted transaction's records are ABSENT from a read_committed
    //    consumer's view (Testcontainers-backed real Kafka, Spring Testing &
    //    Observability guide §3, gives the GENUINE fidelity this test needs — a mocked
    //    Kafka client cannot meaningfully exercise REAL transaction-marker semantics)
    // 4. Assert: re-processing the original input produces EXACTLY ONE final downstream
    //    record, not zero and not two
}
```
