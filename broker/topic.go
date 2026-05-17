package broker

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"streamq/proto"
)

// lease records a batch of messages handed to one consumer that has not yet
// been committed. If the consumer does not commit before the deadline another
// consumer may reclaim the batch, which is what makes delivery at-least-once.
type lease struct {
	from     uint64
	to       uint64
	deadline time.Time
}

// group is the broker-side state of one consumer group on one topic.
//
//   - committed is the contiguous prefix every consumer has acknowledged; it is
//     the durable resume point persisted across restarts.
//   - cursor is the next offset not yet handed to any consumer.
//   - inflight holds the outstanding lease per consumer.
//
// Distributing fetches by advancing a single cursor is what splits a topic's
// work across the consumers of a group without needing partitions.
type group struct {
	name      string
	committed uint64
	cursor    uint64
	inflight  map[string]*lease
}

// recomputeCommitted derives the committed prefix as the lowest offset still
// outstanding, or the cursor when nothing is in flight.
func (g *group) recomputeCommitted() {
	lowest := g.cursor
	for _, l := range g.inflight {
		if l.from < lowest {
			lowest = l.from
		}
	}
	g.committed = lowest
}

// Topic is a named, ordered, append-only log together with the consumer groups
// reading from it.
type Topic struct {
	name     string
	dir      string
	log      *appendLog
	leaseTTL time.Duration

	mu       sync.Mutex
	groups   map[string]*group
	notifyCh chan struct{} // closed and replaced on every publish to wake waiters
}

func openTopic(dir, name string, leaseTTL time.Duration) (*Topic, error) {
	log, err := openLog(filepath.Join(dir, name+".log"))
	if err != nil {
		return nil, err
	}
	t := &Topic{
		name:     name,
		dir:      dir,
		log:      log,
		leaseTTL: leaseTTL,
		groups:   make(map[string]*group),
		notifyCh: make(chan struct{}),
	}
	t.loadOffsets()
	return t, nil
}

func (t *Topic) offsetsPath() string {
	return filepath.Join(t.dir, t.name+".offsets")
}

// loadOffsets restores committed group offsets from the last snapshot. In-flight
// leases are intentionally not restored: any work dispatched but not committed
// before the restart is redelivered from the committed offset.
func (t *Topic) loadOffsets() {
	data, err := os.ReadFile(t.offsetsPath())
	if err != nil {
		return
	}
	var stored map[string]uint64
	if json.Unmarshal(data, &stored) != nil {
		return
	}
	for name, committed := range stored {
		t.groups[name] = &group{
			name:      name,
			committed: committed,
			cursor:    committed,
			inflight:  make(map[string]*lease),
		}
	}
}

func (t *Topic) snapshotOffsets() error {
	t.mu.Lock()
	stored := make(map[string]uint64, len(t.groups))
	for name, g := range t.groups {
		stored[name] = g.committed
	}
	t.mu.Unlock()

	data, err := json.MarshalIndent(stored, "", "  ")
	if err != nil {
		return err
	}
	tmp := t.offsetsPath() + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, t.offsetsPath())
}

func (t *Topic) publish(key string, value []byte) (uint64, error) {
	offset, err := t.log.append(key, value, time.Now())
	if err != nil {
		return 0, err
	}
	t.mu.Lock()
	close(t.notifyCh)
	t.notifyCh = make(chan struct{})
	t.mu.Unlock()
	return offset, nil
}

// waitChan returns a channel closed by the next publish. Callers must capture
// it before checking for messages to avoid missing a concurrent publish.
func (t *Topic) waitChan() <-chan struct{} {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.notifyCh
}

func (t *Topic) groupFor(name string, start uint64) *group {
	if g, ok := t.groups[name]; ok {
		return g
	}
	if start == proto.OffsetLatest {
		_, next, _ := t.log.bounds()
		start = next
	}
	g := &group{
		name:      name,
		committed: start,
		cursor:    start,
		inflight:  make(map[string]*lease),
	}
	t.groups[name] = g
	return g
}

// dispatch returns the next batch for a consumer and the topic high watermark.
//
// The order of checks matters: an uncommitted lease is redelivered to its owner
// so a retried fetch is idempotent; an expired lease from any consumer is
// reclaimed before new work is handed out; otherwise a fresh batch is carved
// from the cursor.
func (t *Topic) dispatch(groupName, consumerID string, start uint64, max int) ([]proto.Message, uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	g := t.groupFor(groupName, start)
	now := time.Now()
	hw := t.highWatermark()

	if l, ok := g.inflight[consumerID]; ok {
		l.deadline = now.Add(t.leaseTTL)
		return t.read(l.from, int(l.to-l.from)), hw
	}
	for cid, l := range g.inflight {
		if now.After(l.deadline) {
			delete(g.inflight, cid)
			reclaimed := &lease{from: l.from, to: l.to, deadline: now.Add(t.leaseTTL)}
			g.inflight[consumerID] = reclaimed
			return t.read(reclaimed.from, int(reclaimed.to-reclaimed.from)), hw
		}
	}

	from := g.cursor
	if oldest, _, _ := t.log.bounds(); from < oldest {
		from = oldest
	}
	if from >= hw {
		return nil, hw
	}
	count := int(hw - from)
	if max > 0 && count > max {
		count = max
	}
	msgs := t.read(from, count)
	if len(msgs) == 0 {
		return nil, hw
	}
	g.cursor = from + uint64(len(msgs))
	g.inflight[consumerID] = &lease{from: from, to: g.cursor, deadline: now.Add(t.leaseTTL)}
	return msgs, hw
}

func (t *Topic) read(from uint64, count int) []proto.Message {
	msgs := t.log.readFrom(from, count)
	for i := range msgs {
		msgs[i].Topic = t.name
	}
	return msgs
}

func (t *Topic) highWatermark() uint64 {
	_, next, _ := t.log.bounds()
	return next
}

// commit acknowledges processing up to (but not including) offset. A commit at
// or past the lease end clears it; a partial commit shrinks it so redelivery
// only covers the unprocessed tail.
func (t *Topic) commit(groupName, consumerID string, offset uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	g, ok := t.groups[groupName]
	if !ok {
		return fmt.Errorf("unknown consumer group %q on topic %q", groupName, t.name)
	}
	l, ok := g.inflight[consumerID]
	if !ok {
		return nil
	}
	switch {
	case offset >= l.to:
		delete(g.inflight, consumerID)
	case offset > l.from:
		l.from = offset
	}
	g.recomputeCommitted()
	return nil
}

func (t *Topic) stats() proto.TopicStats {
	oldest, next, count := t.log.bounds()
	st := proto.TopicStats{
		Name:         t.name,
		MessageCount: count,
		OldestOffset: oldest,
	}
	if count > 0 {
		st.NewestOffset = next - 1
	}

	t.mu.Lock()
	for _, g := range t.groups {
		lag := uint64(0)
		if next > g.committed {
			lag = next - g.committed
		}
		st.Groups = append(st.Groups, proto.GroupStats{
			Name:      g.name,
			Committed: g.committed,
			Lag:       lag,
		})
	}
	t.mu.Unlock()

	sort.Slice(st.Groups, func(i, j int) bool {
		return st.Groups[i].Name < st.Groups[j].Name
	})
	return st
}

func (t *Topic) compactByAge(maxAge time.Duration) error {
	return t.log.truncate(t.log.firstOffsetAfter(time.Now().Add(-maxAge)))
}

func (t *Topic) compactBySize(maxSize int64) error {
	return t.log.truncate(t.log.offsetForMaxSize(maxSize))
}

func (t *Topic) close() error {
	if err := t.snapshotOffsets(); err != nil {
		t.log.close()
		return err
	}
	return t.log.close()
}
