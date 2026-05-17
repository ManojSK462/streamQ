# StreamQ — Persistent Event Streaming Broker
## Design Document

## Goal
Build a persistent, ordered event streaming system in Go that serves as the event backbone for distKV. Config changes and session events in distKV are published to StreamQ topics in real time. Subscribers receive events instantly without polling. Academic-level project targeting portfolio use for FAANG-level interviews.

---

## The Problem It Solves

### Without StreamQ
distKV stores config and session state with strong consistency. But consumers have no way to know when something changed — they poll distKV every N seconds, wasting network calls and always being slightly behind.

10 services polling every second = 10 unnecessary RPCs/second even when nothing changed. A config change takes up to N seconds to propagate. There is no audit trail of what changed and when.

### With StreamQ
Every write to distKV publishes an event to StreamQ. Subscribers receive that event the instant it's committed — no polling, no lag, no wasted calls. Every change is persisted in an ordered log, replayable from any point in time.

### The Combined System Story
```
distKV (consistent state) + StreamQ (event propagation) = complete infrastructure primitive

Strong consistency on writes via Raft
Instant propagation to consumers via pub/sub
Ordered, replayable history of every change
```

This is roughly how etcd and Kafka are used together at Uber, Cloudflare, and similar companies.

---

## Tech Stack

| Layer | Choice | Reason |
|---|---|---|
| Language | Go 1.21+ | Goroutines map naturally to concurrent producer/consumer model |
| Transport | net/rpc over TCP | No external deps, consistent with distKV |
| Storage | Append-only log files per topic | Simple, correct, demonstrates I/O design decisions |
| Consumer state | In-memory offset map + periodic snapshot | Fast reads, survives restarts |
| CLI | Cobra | Consistent with distKV |
| Integration | distKV client SDK | distKV publishes events on every committed write |

---

## Architecture Overview

```
distKV Node (Leader)
    |
    | on every committed write
    v
[StreamQ Producer Client]
    |
    v
[StreamQ Broker]
    |
    +-- Topic: config.changes  --> [Subscriber: Service A]
    |                          --> [Subscriber: Service B]
    |
    +-- Topic: session.events  --> [Subscriber: Auth Logger]
    |                          --> [Subscriber: Analytics]
    |
    +-- Topic: session.expired --> [Subscriber: Cleanup Worker]

Each topic is an ordered, append-only log on disk.
Consumers track their own offset — where they last read up to.
```

---

## Core Concepts

### Topic
A named, ordered, append-only log. Messages within a topic are strictly ordered by offset. Topics are created on first publish.

### Message
```go
type Message struct {
    Offset    uint64    // monotonically increasing within topic
    Topic     string
    Key       string    // optional routing key
    Value     []byte    // payload
    Timestamp time.Time
}
```

### Producer
Publishes messages to a topic. Gets back the offset of the published message.

### Consumer
Reads messages from a topic starting at a given offset. Tracks its own offset — can replay from any point.

### Consumer Group
Multiple consumers in a group share the workload — each message goes to exactly one consumer in the group. Different groups each get all messages independently.

```
Topic: config.changes
    Message 0 → Group A (Consumer 1) AND Group B (Consumer 1)
    Message 1 → Group A (Consumer 2) AND Group B (Consumer 1)
    Message 2 → Group A (Consumer 1) AND Group B (Consumer 1)

Group A splits messages across its consumers (load balancing)
Group B gets all messages on one consumer (single reader)
```

---

## Core Components

### 1. Broker
```go
type Broker struct {
    topics map[string]*Topic
    mu     sync.RWMutex
    dataDir string
}

func (b *Broker) Publish(topic, key string, value []byte) (uint64, error)
func (b *Broker) Subscribe(topic, group string, offset uint64) (<-chan Message, error)
func (b *Broker) Commit(topic, group string, offset uint64) error
func (b *Broker) ListTopics() []string
func (b *Broker) TopicStats(topic string) TopicStats
```

### 2. Topic
```go
type Topic struct {
    name     string
    log      *AppendLog        // on-disk ordered log
    groups   map[string]*Group // consumer groups
    mu       sync.RWMutex
}

type TopicStats struct {
    Name          string
    MessageCount  uint64
    OldestOffset  uint64
    NewestOffset  uint64
    ConsumerGroups []GroupStats
}
```

### 3. Append Log (persistence)
```go
type AppendLog struct {
    file     *os.File
    mu       sync.Mutex
    nextOffset uint64
}

// Each message encoded as:
// [8 bytes offset][8 bytes timestamp][4 bytes key len][key bytes][4 bytes value len][value bytes]

func (l *AppendLog) Append(key string, value []byte) (uint64, error)
func (l *AppendLog) ReadFrom(offset uint64, maxMessages int) ([]Message, error)
func (l *AppendLog) Truncate(retainFrom uint64) error  // retention/compaction
```

### 4. Consumer Group
```go
type Group struct {
    name      string
    consumers map[string]*Consumer  // consumer ID → consumer
    offsets   map[string]uint64     // partition/consumer → committed offset
    mu        sync.Mutex
}

func (g *Group) Assign(consumerID string) uint64  // returns starting offset
func (g *Group) Commit(consumerID string, offset uint64)
func (g *Group) Rebalance()  // redistribute on consumer join/leave
```

### 5. RPC Messages
```go
// Publish
type PublishArgs struct {
    Topic string
    Key   string
    Value []byte
}
type PublishReply struct {
    Offset uint64
    Error  string
}

// Fetch (pull-based consumption)
type FetchArgs struct {
    Topic       string
    Group       string
    ConsumerID  string
    FromOffset  uint64
    MaxMessages int
}
type FetchReply struct {
    Messages []Message
    Error    string
}

// Commit offset
type CommitArgs struct {
    Topic      string
    Group      string
    ConsumerID string
    Offset     uint64
}
type CommitReply struct {
    Error string
}
```

---

## Delivery Guarantees

### At-least-once delivery
Default mode. A message is re-delivered if the consumer crashes before committing its offset. Consumers must handle duplicates.

### Exactly-once (best effort)
Consumer commits offset only after successfully processing. Combined with idempotent processing logic on the consumer side, this approximates exactly-once.

### Why not exactly-once by default
True exactly-once requires distributed transactions between the broker and the consumer's state machine. Out of scope for this project — but worth explaining in interviews as a known tradeoff.

---

## Retention Policy

Messages are not kept forever. Two retention modes:

**Time-based:** Delete messages older than N hours/days.
```go
RetentionPolicy{
    Mode:     TimeBasedRetention,
    MaxAge:   24 * time.Hour,
}
```

**Size-based:** Delete oldest messages when log exceeds N bytes.
```go
RetentionPolicy{
    Mode:    SizeBasedRetention,
    MaxSize: 1 << 30,  // 1GB
}
```

A background goroutine runs compaction periodically, truncating the log and updating the oldest readable offset.

---

## distKV Integration

This is the key differentiator. distKV publishes events to StreamQ on every committed write.

### In distKV's state machine apply():
```go
func (n *RaftNode) apply(cmd Command) string {
    result := n.executeCommand(cmd)

    // Publish event to StreamQ after successful apply
    if n.streamqClient != nil {
        topic := topicForCommand(cmd)
        event := CommandEvent{
            Op:    cmd.Op,
            Key:   cmd.Key,
            Value: cmd.Value,
            Term:  n.currentTerm,
        }
        payload, _ := json.Marshal(event)
        n.streamqClient.Publish(topic, cmd.Key, payload)
    }

    return result
}

func topicForCommand(cmd Command) string {
    if strings.HasPrefix(cmd.Key, "config::") {
        return "config.changes"
    }
    if strings.HasPrefix(cmd.Key, "session::") {
        if cmd.Op == "DELETE" {
            return "session.expired"
        }
        return "session.events"
    }
    return "kv.changes"
}
```

### Topics published by distKV:
| Topic | Trigger | Payload |
|---|---|---|
| `config.changes` | Any SET/DELETE on config:: keys | key, new value, operation |
| `session.events` | Any SET on session:: keys | user ID, token, TTL |
| `session.expired` | Any DELETE on session:: keys | user ID |
| `kv.changes` | Any other key change | key, value, operation |

---

## Project Structure

```
streamq/
├── go.mod
├── go.sum
├── README.md
├── cmd/
│   ├── broker/
│   │   └── main.go          # start the broker
│   └── client/
│       └── main.go          # CLI: publish, consume, list topics, stats
├── broker/
│   ├── broker.go            # Broker struct, Publish/Subscribe/Commit
│   ├── topic.go             # Topic and consumer group management
│   ├── log.go               # AppendLog, on-disk storage
│   ├── retention.go         # Retention policy and compaction
│   └── rpc.go               # RPC server
├── client/
│   ├── producer.go          # Producer client
│   ├── consumer.go          # Consumer client with offset tracking
│   └── rpc.go               # RPC client wrapper
├── integration/
│   └── distkv/
│       └── publisher.go     # distKV → StreamQ bridge
└── test/
    ├── publish_test.go
    ├── consume_test.go
    ├── group_test.go
    ├── retention_test.go
    └── integration_test.go  # distKV + StreamQ end-to-end
```

---

## Demo Script (for README and interviews)

**Start StreamQ broker:**
```bash
./streamq-broker --port 9001 --data-dir data/streamq &
```

**Start distKV cluster with StreamQ integration:**
```bash
./distkv-server --id 1 --peers 1@localhost:8001,2@localhost:8002,3@localhost:8003 \
    --data-dir data/n1 --streamq localhost:9001 &
```

**Subscribe to config changes in one terminal:**
```bash
./streamq-client consume --broker localhost:9001 \
    --topic config.changes --group service-a --from earliest
# Waiting for messages...
```

**Publish a config change via distKV in another terminal:**
```bash
./distkv-client set config::feature_flags::dark_mode true
# OK
```

**Subscriber receives instantly:**
```
offset=0 key=config::feature_flags::dark_mode value=true op=SET
```

**Replay from the beginning:**
```bash
./streamq-client consume --broker localhost:9001 \
    --topic config.changes --group audit --from 0
# Replays all config changes ever made
```

**Topic stats:**
```bash
./streamq-client stats --broker localhost:9001 --topic config.changes
# Topic: config.changes
# Messages: 47
# Oldest offset: 0
# Newest offset: 46
# Consumer groups: service-a (offset: 46), audit (offset: 12)
```

---

## Tests to Write

```go
// 1. Publish and consume single message on a topic
// 2. Messages within a topic are strictly ordered by offset
// 3. Two consumer groups each receive all messages independently
// 4. Consumer group with two consumers splits messages between them
// 5. Consumer restart resumes from last committed offset
// 6. Replay from offset 0 returns all messages
// 7. Retention policy deletes old messages, oldest offset advances
// 8. distKV write triggers StreamQ publish (integration test)
// 9. Subscriber receives distKV config change within 100ms
// 10. Broker restart — existing topic logs survive, consumers resume
```

---

## Benchmark

```go
// Single producer, single consumer, same topic
// Measure: messages published per second
// Measure: end-to-end latency (publish → consumer receives)
// Measure: consumer lag under high publish rate
// Target: 100k+ messages/second throughput
//         sub-10ms end-to-end latency on localhost
```

---

## Phases

**Phase 1 (Day 1-2):** AppendLog, single topic, publish and fetch via RPC. Basic produce/consume working end to end.

**Phase 2 (Day 3):** Consumer groups, offset tracking, commit. Multiple consumers splitting a topic.

**Phase 3 (Day 4):** Retention policy and compaction background goroutine.

**Phase 4 (Day 5):** distKV integration — publisher bridge, topic routing by key prefix.

**Phase 5 (Day 6):** Cobra CLI client, topic stats, list topics.

**Phase 6 (Day 7):** Tests, benchmark, demo script, README.

---

## What to Tell Interviewers

"I built StreamQ as the event backbone for distKV. The core insight was that distKV already solves config consistency via Raft — but consumers still had to poll to know when something changed. StreamQ adds real-time propagation: every committed write in distKV publishes an event to a topic, and subscribers receive it instantly with no polling. Messages are persisted in an append-only log per topic, consumers track their own offsets so they can replay from any point, and consumer groups let multiple services share a topic without duplicating work. Together, distKV and StreamQ give you strong consistency on writes and instant eventual propagation to consumers — which is roughly how etcd and Kafka are used together in production."

---

## Reference
- Kafka design docs: https://kafka.apache.org/documentation/#design
- NATS documentation: https://docs.nats.io — simpler pub/sub, good contrast to understand
- Martin Kleppmann "Designing Data-Intensive Applications" Chapter 11 — stream processing. Read this before building, it'll make everything click.
