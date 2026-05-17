# StreamQ

A persistent, ordered event streaming broker written in Go. StreamQ is the
event backbone for [distKV](#distkv-integration): every committed write in
distKV is published to a StreamQ topic, and subscribers receive it instantly
without polling. Each topic is an append-only log on disk, consumers track
their own offsets and can replay from any point, and consumer groups let
multiple services share a topic without duplicating work.

## Why it exists

distKV stores config and session state with strong consistency, but consumers
have no way to know when something changed — they poll. Polling wastes calls
and always lags. StreamQ replaces the poll: distKV publishes an event on every
committed write, and subscribers receive it the instant it lands. Strong
consistency on writes (distKV/Raft) plus ordered, replayable propagation
(StreamQ) is roughly how etcd and Kafka are combined in production.

## Architecture

```
distKV node (leader) --on every committed write--> StreamQ producer client
                                                          |
                                                          v
                                                   StreamQ broker
                                                          |
   topic config.changes  (append-only log)  --> group service-a, group audit
   topic session.events  (append-only log)  --> group analytics
   topic session.expired (append-only log)  --> group cleanup-worker
```

- A **topic** is a named, ordered, append-only log on disk, created on first use.
- A **message** has a monotonically increasing **offset** within its topic.
- A **consumer group** shares a topic's messages across its members; separate
  groups each receive the full stream independently.
- The broker serves clients over `net/rpc` on TCP. No external runtime
  dependencies; Cobra is used only to build the CLI.

## Build

```
go build -o bin/streamq-broker ./cmd/broker
go build -o bin/streamq-client ./cmd/client
```

## Quick start

Start the broker:

```
./bin/streamq-broker --port 9001 --data-dir data/streamq
```

Publish two messages:

```
./bin/streamq-client publish --broker localhost:9001 \
    --topic config.changes --key config::dark_mode --value true
./bin/streamq-client publish --broker localhost:9001 \
    --topic config.changes --key config::beta --value on
```

Subscribe — the consumer blocks and receives new messages as they arrive:

```
./bin/streamq-client consume --broker localhost:9001 \
    --topic config.changes --group service-a --from earliest
```

Replay the full history into a fresh group:

```
./bin/streamq-client consume --broker localhost:9001 \
    --topic config.changes --group audit --from 0
```

Inspect a topic:

```
./bin/streamq-client stats --broker localhost:9001 --topic config.changes
```

```
Topic: config.changes
Messages: 2
Oldest offset: 0
Newest offset: 1
Consumer groups: audit (offset: 2, lag: 0), service-a (offset: 2, lag: 0)
```

## CLI reference

| Command   | Purpose                          | Key flags                                         |
|-----------|----------------------------------|---------------------------------------------------|
| `publish` | Append a message to a topic      | `--topic`, `--key`, `--value`                     |
| `consume` | Read a topic as part of a group  | `--topic`, `--group`, `--from`, `--id`, `--max`   |
| `list`    | List all topics                  | `--broker`                                        |
| `stats`   | Show offsets and group lag       | `--topic`                                         |

`--from` accepts `earliest`, `latest`, or an explicit offset. It positions a
brand-new consumer group only; once a group has a committed offset the broker
resumes from there.

## How it works

### Append log

Each topic is one file. Records are length-prefixed and carry a CRC32 over the
body:

```
[4 body length][8 offset][8 timestamp][4 key length][key][4 value length][value][4 crc32]
```

The length prefix lets a recovering broker walk the file without trusting its
contents; the CRC detects a torn final write left by a crash, and that tail is
truncated on startup rather than failing. Writes go to the OS page cache on the
hot path and are flushed to disk separately, which keeps publishes fast while
still surviving a process crash.

### Consumer groups and offsets

A group has a single dispatch cursor. Each fetch hands the requesting consumer
the next undispatched batch and advances the cursor, so multiple consumers in
one group naturally split the topic's work without partitions. Separate groups
have independent cursors and each see the whole stream. Committed offsets are
snapshotted to disk periodically and on shutdown, so groups resume where they
left off after a restart.

### Delivery guarantees

Delivery is **at-least-once**. A dispatched batch is held under a lease; if the
consumer does not commit before the lease expires (or the broker restarts), the
batch is redelivered. Consumers should process idempotently. True exactly-once
would require a distributed transaction between the broker and each consumer's
state machine, which is deliberately out of scope.

### Retention

Topics do not keep history forever. Retention runs as a background pass:

- **Time-based** drops messages older than `--retention-max-age`.
- **Size-based** drops the oldest messages once a log exceeds `--retention-max-size`.

Compaction rewrites the log without the dropped prefix and advances the topic's
oldest readable offset.

## distKV integration

The `integration/distkv` package is the bridge distKV uses to publish events.
It connects to a StreamQ broker, routes each command to a topic by key prefix,
and encodes the event as JSON.

```go
publisher, err := distkv.NewPublisher("localhost:9001")
// ...
offset, err := publisher.Publish(distkv.CommandEvent{
    Op:    cmd.Op,        // "SET" / "DELETE"
    Key:   cmd.Key,
    Value: cmd.Value,
    Term:  node.CurrentTerm,
})
```

Topic routing:

| Key prefix   | Operation | Topic              |
|--------------|-----------|--------------------|
| `config::`   | any       | `config.changes`   |
| `session::`  | SET       | `session.events`   |
| `session::`  | DELETE    | `session.expired`  |
| anything else| any       | `kv.changes`       |

distKV consumes StreamQ as a Go module. In distKV's `go.mod`:

```
require streamq v0.0.0
replace streamq => ../streamq
```

(adjust the relative path to wherever this repository lives). Then call
`publisher.Publish` from the Raft state machine's `apply` step, after a command
is committed and applied.

## Project layout

```
streamq/
  cmd/broker/        broker entry point
  cmd/client/        Cobra CLI: publish, consume, list, stats
  broker/            broker, topic, append log, consumer groups, retention, RPC
  client/            producer and consumer clients
  integration/distkv/ distKV -> StreamQ publisher bridge
  proto/             RPC wire types
  test/              end-to-end tests and benchmarks
```

## Tests and benchmarks

```
go test ./...
go test ./test/ -bench=. -benchmem -run=^$
```

The suite covers ordered publish/consume, independent consumer groups, work
splitting within a group, offset-resumed restart, time- and size-based
retention, and the distKV publish-to-subscribe path.

Measured on an 8-core laptop (Ryzen 7 4800H, Windows, loopback):

| Benchmark            | Result        | Note                                    |
|----------------------|---------------|-----------------------------------------|
| Broker append        | ~7.4 us/op    | ~135k messages/sec at the storage layer |
| End-to-end           | ~0.44 ms/op   | publish to consumer receive             |
| Single sync publish  | ~149 us/op    | bound by the RPC round trip             |

Storage throughput meets the 100k messages/sec target and end-to-end latency is
well under 10ms. A single synchronous producer is round-trip bound; the standard
remedy — producer-side batching, as in Kafka — is a natural next step.
