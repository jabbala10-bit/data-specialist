# Kafka Architecture Deep Dive — Principal Engineer Reference

> File 2 of 7 in the Message Queues & Event Streaming series. Covers what's actually happening inside a Kafka cluster — partitions, replication, the controller, and KRaft — the mechanical foundation for every Kafka-related reference throughout this series.

**Series:** 1. Messaging Fundamentals · **2. Kafka Architecture Deep Dive (this file)** · 3. Kafka Producers & Consumers · 4. Exactly-Once Semantics · 5. Schema Management · 6. RabbitMQ & Alternative Brokers · 7. Event-Driven Architecture at Scale

---

## Table of Contents

1. [Topics, Partitions & the Log Structure](#1-topics-partitions--the-log-structure)
2. [Replication & the ISR Model](#2-replication--the-isr-model)
3. [The Controller & KRaft](#3-the-controller--kraft)
4. [Log Segments, Retention & Compaction](#4-log-segments-retention--compaction)
5. [Partition Leadership & Failover](#5-partition-leadership--failover)
6. [Why Kafka Achieves High Throughput — Mechanically](#6-why-kafka-achieves-high-throughput--mechanically)
7. [Cluster Topology & Capacity Planning](#7-cluster-topology--capacity-planning)
8. [Multi-Cluster & Cross-Region Replication](#8-multi-cluster--cross-region-replication)

---

## 1. Topics, Partitions & the Log Structure

### 1.1 A Partition Is an Append-Only Log File, Mechanically

```
Directly extending the Database Architecture File 1 §2.3 LSM-tree/§2.5 write-ahead-log
discussion: a Kafka PARTITION is, physically, an APPEND-ONLY sequence of log SEGMENTS
(ordinary files on disk) — every produced message is appended SEQUENTIALLY (never an
in-place modification), assigned a monotonically-increasing OFFSET within that partition.
This is precisely WHY Kafka achieves such high WRITE throughput (§6): sequential disk
writes are dramatically faster than random-access writes, the SAME fundamental hardware
characteristic motivating LSM-tree-based storage engines generally (Database Architecture
File 1 §2.3, File 2 §1.1 of the Database series).
```

### 1.2 Why Topics Are Split Into Multiple Partitions At All

```
Directly the Java System Design guide's §6.2 framing: a partition is the UNIT OF
PARALLELISM. A topic with ONLY one partition can be written to and read from by
EXACTLY one producer-thread-equivalent and consumed by exactly ONE consumer (within a
group, File 1 §2.3 of this series) AT A TIME — splitting a topic into N partitions allows
UP TO N consumers (within one group) to process IN PARALLEL, and allows the topic's
AGGREGATE throughput to scale roughly linearly with partition count, up to the limits of
the underlying broker hardware/network.
```

### 1.3 The Partition-Count Decision — Largely Irreversible Once Chosen Poorly

```
Directly extending the NoSQL guide's §3.2 sharding-key-choice "genuinely hard to change
later" caution: INCREASING a topic's partition count later is technically possible, but
BREAKS the existing key-to-partition mapping (File 1 §4.2 of this series) for ANY
EXISTING keyed data — messages for the SAME key may now land on a DIFFERENT partition
than historical messages for that key did, silently breaking partition-scoped ordering
guarantees for data that straddles the change. Partition count should be chosen
DELIBERATELY upfront, based on projected throughput/parallelism needs (§7's capacity-
planning discussion), not treated as a casually-adjustable runtime parameter.
```

---

## 2. Replication & the ISR Model

### 2.1 Replication Factor & Leader/Follower Roles

```
Each partition has ONE LEADER replica (handling ALL reads and writes for that partition)
and N-1 FOLLOWER replicas (continuously replicating the leader's log) — directly the
SAME leader/follower architecture as the Database Architecture File 1 §7.1 relational
streaming-replication discussion, here applied to Kafka's own partition-level replication.
A replication factor of 3 (the common production default) means each partition's data
exists on 3 separate brokers, tolerating up to 2 simultaneous broker failures without DATA LOSS.
```

### 2.2 In-Sync Replicas (ISR) — The Mechanism Behind Kafka's Durability Guarantee

```
The ISR is the SUBSET of a partition's replicas that are CURRENTLY caught up with the
leader (within a configurable lag tolerance) — a replica that falls TOO FAR behind
(network issues, an overloaded broker) is REMOVED from the ISR until it catches back up.
This directly determines what `acks=all` (§2.3) actually MEANS: a producer's write is
considered DURABLY ACKNOWLEDGED once EVERY member of the CURRENT ISR has received it —
NOT necessarily every configured replica, since an out-of-sync replica is, by definition,
EXCLUDED from this durability guarantee until it rejoins the ISR.
```

### 2.3 `acks` — The Producer-Side Durability/Latency Trade-off

```java
Properties props = new Properties();
props.put(ProducerConfig.ACKS_CONFIG, "all"); // directly the System Design guide's §3.1
                                                  // synchronous-replication trade-off,
                                                  // here as a per-producer configuration:
// acks=0 — fire and forget, NO acknowledgment waited for at all — lowest latency,
//          GENUINE risk of silent data loss on ANY failure
// acks=1 — waits for the LEADER ONLY to acknowledge — moderate durability (survives a
//          FOLLOWER failure, but NOT a LEADER failure before replication completes)
// acks=all — waits for the FULL ISR (§2.2) to acknowledge — strongest durability,
//          highest latency, directly the System Design guide's §3.1 "synchronous
//          replication... higher write latency... bounded by the SLOWEST acknowledging replica"
```

### 2.4 `min.insync.replicas` — Refusing Writes When Durability Can't Be Guaranteed

```
A topic-level setting REQUIRING the ISR to contain AT LEAST N members for a write with
`acks=all` to succeed AT ALL — if the ISR shrinks below this threshold (e.g., two
brokers down simultaneously, with `min.insync.replicas=2`), PRODUCERS RECEIVE AN ERROR
rather than a write silently succeeding with WEAKER durability than intended. This is
directly the System Design guide's §4.2 CP-vs-AP choice MADE EXPLICIT and CONFIGURABLE:
setting `min.insync.replicas=2` with `replication.factor=3` deliberately CHOOSES
consistency/durability over availability during a partial outage — refusing writes
rather than risking data that might be lost if the SOLE remaining in-sync replica also fails.
```

---

## 3. The Controller & KRaft

### 3.1 The Controller's Role

```
ONE broker in a Kafka cluster acts as the CONTROLLER — responsible for partition-leader
ELECTION (when a leader broker fails, the controller selects a new leader from the
remaining ISR, §5), tracking broker liveness, and propagating metadata (which broker
leads which partition) to every other broker and to clients. This is structurally a
SPECIALIZED, NARROWER version of the consensus-driven coordination role described
generically in the Java System Design guide's §5 (leader election, §5.4).
```

### 3.2 The Historical ZooKeeper Dependency, and Why It Was Removed

```
Kafka historically depended on Apache ZooKeeper EXTERNALLY for exactly this controller-
election and metadata-coordination role — directly an instance of the System Design
guide's §5.4 "use a battle-tested coordination service rather than implementing
consensus yourself" pattern, with ZooKeeper filling that role. KRaft (Kafka's own,
built-in Raft-based consensus implementation, directly the Raft mechanics covered in the
Database Architecture File 1's distributed-systems-adjacent discussion and the System
Design guide's §5.2) REMOVED this external dependency, with Kafka brokers THEMSELVES
running the consensus protocol for controller election and metadata management — directly
reducing OPERATIONAL complexity (one fewer distributed system to operate, monitor, and
keep version-compatible) while providing the SAME underlying majority-quorum safety guarantees described in that section.
```

### 3.3 Why This Matters for an FDE Engagement Specifically

```
A LEGACY Kafka deployment (pre-KRaft, still running ZooKeeper) represents GENUINE
additional operational surface area (per the System Design guide's §5.4 framing) — an
FDE engagement encountering such a deployment should recognize this as a MIGRATION
candidate (modern Kafka versions support KRaft-mode migration) rather than assuming the
ZooKeeper dependency is a permanent, unavoidable architectural fact — directly the SAME
"recognize a legacy pattern and its modern replacement" skill this series has emphasized
for other technology transitions throughout (e.g., the Spring Security guide's
`WebSecurityConfigurerAdapter` deprecation discussion).
```

---

## 4. Log Segments, Retention & Compaction

### 4.1 Segments — Why a Partition's Log Isn't One Giant File

```
A partition's log is physically split into multiple SEGMENT files, rolled over at a
configured size/time threshold — directly the SAME reasoning as the Database
Architecture File 1 §8.1 table-partitioning discussion: a BOUNDED segment can be
EFFICIENTLY deleted as a whole file once its RETENTION period expires (a near-instant
metadata-level operation), rather than requiring an expensive row-by-row/record-by-record
deletion process — the EXACT SAME "bounded units enable cheap retention enforcement" principle, here at the Kafka segment level.
```

### 4.2 Time/Size-Based Retention — The Default Model

```properties
log.retention.hours=168     # 7 days — messages older than this are eligible for deletion
log.retention.bytes=-1       # unlimited by size (retention governed by TIME alone here) —
                                # or set explicitly for SIZE-bounded retention instead
```

```
This is the DEFAULT Kafka retention model — directly suited to the "EVENT STREAM as a
bounded-replay-window buffer" use case (File 1 §8.2 of this series): consumers can
REPLAY recent history, but the topic does NOT retain data indefinitely by default.
```

### 4.3 Log Compaction — A Fundamentally Different Retention Model

```properties
cleanup.policy=compact
```

```
Rather than deleting OLD messages by age, COMPACTION retains only the MOST RECENT message
PER KEY, eventually removing OLDER messages for keys that have since been UPDATED —
directly turning a Kafka topic into something resembling a DISTRIBUTED, REPLAYABLE
KEY-VALUE STORE rather than a time-bounded event buffer. This is the EXACT mechanism
underlying Kafka Streams' "KTable" abstraction (File 7 of this series) and a common
pattern for maintaining a CHANGELOG of "current state per entity" (e.g., the latest known
state of every customer record) that a NEW consumer can fully reconstruct by reading the
ENTIRE compacted topic from the beginning — directly the Database Architecture File 1
§2.5 "replay the log to reconstruct state" principle, here as Kafka's OWN native mechanism
for this pattern, rather than requiring an external event-sourcing framework.
```

---

## 5. Partition Leadership & Failover

### 5.1 The Failover Sequence

```
1. The CONTROLLER (§3.1) detects a broker failure (via the SAME heartbeat/liveness
   mechanism underlying the KRaft consensus protocol, §3.2)
2. For EVERY partition where the failed broker was the LEADER, the controller selects a
   NEW leader from that partition's CURRENT ISR (§2.2) — directly the System Design
   guide's §5.2 Raft-leader-election mechanics, applied here at the PARTITION level
   rather than the whole-cluster level
3. The new leadership assignment is propagated to all brokers and, eventually, to clients
   (which discover the new leader via a metadata refresh, directly analogous to a DNS
   TTL-driven re-resolution, Networking series File 3 §3.1, just for Kafka's OWN internal
   broker-metadata protocol instead of DNS)
```

### 5.2 "Unclean" Leader Election — A Real Durability Trade-off

```properties
unclean.leader.election.enable=false  # the SAFE default — if NO in-sync replica is
                                          # available, the partition becomes UNAVAILABLE
                                          # for writes/reads rather than electing an
                                          # OUT-OF-SYNC replica as leader (which would risk
                                          # SILENT data loss/inconsistency, since that
                                          # replica may be missing recently-committed messages)
```

```
Setting this to `true` trades the SAME durability-vs-availability decision as §2.4's
`min.insync.replicas`, but in the OPPOSITE direction — choosing to KEEP a partition
AVAILABLE even at the risk of silently losing recently-acknowledged messages, directly
the System Design guide's §4.2 AP-over-CP choice, here made explicit as a Kafka
configuration parameter rather than an abstract architectural discussion.
```

---

## 6. Why Kafka Achieves High Throughput — Mechanically

### 6.1 Sequential I/O & the OS Page Cache

```
Beyond §1.1's sequential-write benefit, Kafka deliberately relies HEAVILY on the
OPERATING SYSTEM's own page cache rather than implementing its own application-level
caching layer — directly analogous to the Java Concurrency guide's §5 virtual-threads
philosophy of "let an existing, highly-optimized layer do the work rather than
reimplementing it" — recently-written data remains in the OS page cache, meaning
RECENT-history reads (the common case for most consumers, who are reading near the
current end of the log) are served from MEMORY, not disk, despite Kafka's data being
fully PERSISTED to disk for durability — a deliberately clever exploitation of how
modern operating systems already manage file-backed memory.
```

### 6.2 Zero-Copy Transfer

```
Directly the Java I/O guide's §2.4 `transferTo`/zero-copy discussion: Kafka uses this
EXACT mechanism to send data from a partition's log file DIRECTLY to a consumer's
network socket, WITHOUT copying it through application-level (JVM heap) buffers at all —
a broker serving MANY consumers reading the SAME recent data benefits substantially from
avoiding this copy overhead PER CONSUMER, directly explaining part of Kafka's
exceptional throughput-per-broker characteristics relative to a more naively-implemented message broker.
```

### 6.3 Batching — At Every Layer

```
Producers BATCH multiple messages into a single network request (configurable via
`batch.size`/`linger.ms`, trading a small amount of LATENCY for substantially better
THROUGHPUT — directly the SAME Nagle's-algorithm trade-off from the Networking series'
File 1 §5, here at the application-protocol layer instead of the TCP layer), and
consumers FETCH multiple messages per request rather than one at a time — batching
consistently, at every layer of the system, to amortize per-request overhead across many messages.
```

---

## 7. Cluster Topology & Capacity Planning

### 7.1 Broker Count & Replication Factor — Directly Extending Database Architecture File 5

```
Apply the EXACT SAME capacity-planning methodology from the Database Architecture File 5
series to a Kafka cluster: characterize the workload (aggregate produce/consume
throughput, retention requirements translating to storage, §4), and size BROKER COUNT to
support BOTH the required PARALLELISM (at least as many brokers as the topic with the
HIGHEST partition count genuinely needs to distribute load across) and the required
REPLICATION FACTOR (§2.1) with adequate FAILURE-DOMAIN spread (brokers across multiple
availability zones, directly the Cloud-Native guide's File 4 §4 networking-topology discussion).
```

### 7.2 Rack Awareness — Spreading Replicas Across Failure Domains

```properties
broker.rack=eu-central-1a  # configured per broker — Kafka uses this to AVOID placing
                              # ALL replicas of a partition in the SAME availability zone,
                              # directly the System Design guide's §3.1 "replication for
                              # AVAILABILITY, requiring genuinely independent failure
                              # domains" principle, applied as a concrete Kafka configuration
```

---

## 8. Multi-Cluster & Cross-Region Replication

### 8.1 MirrorMaker 2 — Replicating Between Kafka Clusters

```
For multi-region architectures (directly the Cloud-Native guide's File 4 §7 EU-residency
discussion, and the Wide-Column guide's §4 multi-region active-active pattern), MirrorMaker
2 replicates topics FROM one Kafka cluster TO another — commonly used for: disaster
recovery (a fully-replicated standby cluster in a different region), data-residency-
driven topology (an EU cluster mirroring ONLY the subset of topics permitted to leave the
EU to a global analytics cluster), or active-active multi-region deployments needing
eventually-consistent cross-region event visibility.
```

### 8.2 The Cross-Cluster Consistency Caveat

```
Directly the System Design guide's §4.3 eventual-consistency framing, restated for
cross-cluster Kafka replication specifically: MirrorMaker-replicated topics are
EVENTUALLY consistent across clusters, with REAL replication lag (monitorable via the
same consumer-lag mechanisms from File 1 §6.1 of this series, applied to the mirroring
process itself) — never assume cross-cluster replicated data is available with the SAME
immediacy as same-cluster replication (§2), and design any cross-region consumer logic
with this lag window explicitly in mind, exactly the same discipline applied to cross-
region database replication throughout the Database Architecture series.
```
