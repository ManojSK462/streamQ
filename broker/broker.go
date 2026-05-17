// Package broker implements the StreamQ broker: persistent per-topic logs,
// consumer groups with offset tracking, retention and an RPC server.
package broker

import (
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"streamq/proto"
)

// DefaultLeaseTTL is how long a dispatched-but-uncommitted batch is reserved
// for its consumer before another consumer may reclaim it.
const DefaultLeaseTTL = 30 * time.Second

// topicNamePattern keeps topic names usable as plain file names across
// platforms; the broker stores one log file per topic.
var topicNamePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]*$`)

type Config struct {
	DataDir   string
	LeaseTTL  time.Duration
	Retention RetentionPolicy
}

type Broker struct {
	cfg    Config
	mu     sync.RWMutex
	topics map[string]*Topic
}

// New opens (or creates) the data directory and restores every topic log and
// its committed consumer offsets found there.
func New(cfg Config) (*Broker, error) {
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = DefaultLeaseTTL
	}
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("broker: data directory is required")
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, err
	}
	b := &Broker{cfg: cfg, topics: make(map[string]*Topic)}
	if err := b.loadTopics(); err != nil {
		return nil, err
	}
	return b, nil
}

func (b *Broker) loadTopics() error {
	entries, err := os.ReadDir(b.cfg.DataDir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".log") {
			continue
		}
		topic := strings.TrimSuffix(name, ".log")
		t, err := openTopic(b.cfg.DataDir, topic, b.cfg.LeaseTTL)
		if err != nil {
			return fmt.Errorf("broker: restoring topic %q: %w", topic, err)
		}
		b.topics[topic] = t
	}
	return nil
}

// topicFor resolves a topic, optionally creating it. Topics are created on
// first publish and on first fetch so a consumer may subscribe and wait before
// any producer exists.
func (b *Broker) topicFor(name string, create bool) (*Topic, error) {
	if !topicNamePattern.MatchString(name) {
		return nil, fmt.Errorf("broker: invalid topic name %q", name)
	}
	b.mu.RLock()
	t := b.topics[name]
	b.mu.RUnlock()
	if t != nil {
		return t, nil
	}
	if !create {
		return nil, fmt.Errorf("broker: unknown topic %q", name)
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if t = b.topics[name]; t != nil {
		return t, nil
	}
	t, err := openTopic(b.cfg.DataDir, name, b.cfg.LeaseTTL)
	if err != nil {
		return nil, err
	}
	b.topics[name] = t
	return t, nil
}

func (b *Broker) Publish(topic, key string, value []byte) (uint64, error) {
	t, err := b.topicFor(topic, true)
	if err != nil {
		return 0, err
	}
	return t.publish(key, value)
}

// Fetch returns the next batch for a consumer. When the batch would be empty
// and MaxWait is positive it blocks until a publish arrives or the deadline
// passes, giving subscribers low-latency delivery without polling.
func (b *Broker) Fetch(req proto.FetchArgs) (proto.FetchReply, error) {
	if req.Group == "" || req.ConsumerID == "" {
		return proto.FetchReply{}, fmt.Errorf("broker: fetch requires a group and consumer id")
	}
	t, err := b.topicFor(req.Topic, true)
	if err != nil {
		return proto.FetchReply{}, err
	}

	deadline := time.Now().Add(req.MaxWait)
	for {
		wait := t.waitChan()
		msgs, hw := t.dispatch(req.Group, req.ConsumerID, req.FromOffset, req.MaxMessages)
		if len(msgs) > 0 || req.MaxWait <= 0 {
			return proto.FetchReply{Messages: msgs, HighWatermark: hw}, nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return proto.FetchReply{Messages: msgs, HighWatermark: hw}, nil
		}
		select {
		case <-wait:
		case <-time.After(remaining):
		}
	}
}

func (b *Broker) Commit(req proto.CommitArgs) error {
	t, err := b.topicFor(req.Topic, false)
	if err != nil {
		return err
	}
	return t.commit(req.Group, req.ConsumerID, req.Offset)
}

func (b *Broker) ListTopics() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	names := make([]string, 0, len(b.topics))
	for name := range b.topics {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (b *Broker) Stats(topic string) (proto.TopicStats, error) {
	t, err := b.topicFor(topic, false)
	if err != nil {
		return proto.TopicStats{}, err
	}
	return t.stats(), nil
}

func (b *Broker) allTopics() []*Topic {
	b.mu.RLock()
	defer b.mu.RUnlock()
	ts := make([]*Topic, 0, len(b.topics))
	for _, t := range b.topics {
		ts = append(ts, t)
	}
	return ts
}

// Snapshot persists committed consumer offsets for every topic.
func (b *Broker) Snapshot() error {
	for _, t := range b.allTopics() {
		if err := t.snapshotOffsets(); err != nil {
			return err
		}
	}
	return nil
}

// Close flushes every log to disk and writes a final offset snapshot.
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	var firstErr error
	for _, t := range b.topics {
		if err := t.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
