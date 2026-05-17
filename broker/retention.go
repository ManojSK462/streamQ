package broker

import (
	"context"
	"time"
)

type RetentionMode int

const (
	RetentionNone RetentionMode = iota
	RetentionTime
	RetentionSize
)

// RetentionPolicy bounds how much history a topic keeps. Time retention drops
// messages older than MaxAge; size retention drops the oldest messages once a
// log exceeds MaxSize bytes.
type RetentionPolicy struct {
	Mode     RetentionMode
	MaxAge   time.Duration
	MaxSize  int64
	Interval time.Duration
}

const defaultRetentionInterval = time.Minute

// RunRetention runs compaction on a fixed interval until ctx is cancelled. It
// is a no-op when retention is disabled.
func (b *Broker) RunRetention(ctx context.Context) {
	policy := b.cfg.Retention
	if policy.Mode == RetentionNone {
		return
	}
	interval := policy.Interval
	if interval <= 0 {
		interval = defaultRetentionInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.Compact()
		}
	}
}

// Compact applies the configured retention policy to every topic once.
func (b *Broker) Compact() {
	policy := b.cfg.Retention
	for _, t := range b.allTopics() {
		switch policy.Mode {
		case RetentionTime:
			t.compactByAge(policy.MaxAge)
		case RetentionSize:
			t.compactBySize(policy.MaxSize)
		}
	}
}
