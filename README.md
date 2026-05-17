# StreamQ

An ordered event streaming broker written in Go. StreamQ is the
event backbone for [distKV](#distkv-integration): every committed write in
distKV is published to a StreamQ topic, and subscribers receive it instantly
without polling. Each topic is an append-only log on disk, consumers track
their own offsets and can replay from any point, and consumer groups let
multiple services share a topic without duplicating work.

## Why?

distKV stores config and session state with strong consistency, but consumers
have no way to know when something changed — they poll. Polling wastes calls
and always lags. StreamQ replaces the poll: distKV publishes an event on every
committed write, and subscribers receive it the instant it lands. Strong
consistency on writes (distKV/Raft) plus ordered, replayable propagation
(StreamQ) is roughly how etcd and Kafka are combined in production.

## Build

```
go build -o bin/streamq-broker ./cmd/broker
go build -o bin/streamq-client ./cmd/client
```

