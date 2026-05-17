// Package proto defines the wire types exchanged between StreamQ clients and
// the broker. They are transported with net/rpc's gob codec, so every field
// must remain exported and gob-encodable.
package proto

import "time"

// Offset sentinels interpreted by the broker when a consumer group is first
// created. Once a group exists its committed offset takes precedence and the
// requested start offset is ignored.
const (
	OffsetEarliest uint64 = 0
	OffsetLatest   uint64 = ^uint64(0)
)

// Message is a single record stored in a topic's log.
type Message struct {
	Offset    uint64
	Topic     string
	Key       string
	Value     []byte
	Timestamp time.Time
}

type PublishArgs struct {
	Topic string
	Key   string
	Value []byte
}

type PublishReply struct {
	Offset uint64
}

// FetchArgs requests the next batch of messages for a consumer within a group.
// FromOffset positions a brand-new group only; MaxWait turns the call into a
// bounded long-poll so idle consumers do not busy-loop.
type FetchArgs struct {
	Topic       string
	Group       string
	ConsumerID  string
	FromOffset  uint64
	MaxMessages int
	MaxWait     time.Duration
}

type FetchReply struct {
	Messages      []Message
	HighWatermark uint64
}

type CommitArgs struct {
	Topic      string
	Group      string
	ConsumerID string
	Offset     uint64
}

type CommitReply struct{}

type ListTopicsArgs struct{}

type ListTopicsReply struct {
	Topics []string
}

type StatsArgs struct {
	Topic string
}

type StatsReply struct {
	Stats TopicStats
}

type TopicStats struct {
	Name         string
	MessageCount uint64
	OldestOffset uint64
	NewestOffset uint64
	Groups       []GroupStats
}

type GroupStats struct {
	Name      string
	Committed uint64
	Lag       uint64
}
