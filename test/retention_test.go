package test

import (
	"fmt"
	"testing"
	"time"

	"streamq/broker"
	"streamq/client"
)

// Size-based retention drops the oldest messages once a log grows past its
// limit, advancing the topic's oldest readable offset.
func TestSizeBasedRetentionAdvancesOldestOffset(t *testing.T) {
	b, addr := startBroker(t, broker.Config{
		Retention: broker.RetentionPolicy{Mode: broker.RetentionSize, MaxSize: 4 << 10},
	})

	producer, err := client.NewProducer(addr)
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	defer producer.Close()

	const total = 400
	payload := make([]byte, 128)
	for i := 0; i < total; i++ {
		if _, err := producer.Publish("events", "k", payload); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	before, err := b.Stats("events")
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	b.Compact()
	after, err := b.Stats("events")
	if err != nil {
		t.Fatalf("stats: %v", err)
	}

	if after.OldestOffset <= before.OldestOffset {
		t.Fatalf("oldest offset did not advance: before=%d after=%d", before.OldestOffset, after.OldestOffset)
	}
	if after.MessageCount >= before.MessageCount {
		t.Fatalf("message count did not shrink: before=%d after=%d", before.MessageCount, after.MessageCount)
	}
	if after.NewestOffset != before.NewestOffset {
		t.Fatalf("newest offset changed during compaction: before=%d after=%d", before.NewestOffset, after.NewestOffset)
	}

	// Surviving messages remain readable from the new oldest offset.
	msgs := drain(t, addr, "events", "reader")
	if uint64(len(msgs)) != after.MessageCount {
		t.Fatalf("read %d messages, stats report %d retained", len(msgs), after.MessageCount)
	}
	if msgs[0].Offset != after.OldestOffset {
		t.Fatalf("first readable offset = %d, want %d", msgs[0].Offset, after.OldestOffset)
	}
}

// Time-based retention drops messages older than the configured age while
// keeping newer ones.
func TestTimeBasedRetentionDropsOldMessages(t *testing.T) {
	b, addr := startBroker(t, broker.Config{
		Retention: broker.RetentionPolicy{Mode: broker.RetentionTime, MaxAge: 50 * time.Millisecond},
	})

	producer, err := client.NewProducer(addr)
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	defer producer.Close()

	for i := 0; i < 5; i++ {
		if _, err := producer.Publish("events", "old", []byte(fmt.Sprintf("old%d", i))); err != nil {
			t.Fatalf("publish old %d: %v", i, err)
		}
	}
	time.Sleep(120 * time.Millisecond)
	for i := 0; i < 5; i++ {
		if _, err := producer.Publish("events", "new", []byte(fmt.Sprintf("new%d", i))); err != nil {
			t.Fatalf("publish new %d: %v", i, err)
		}
	}

	b.Compact()
	stats, err := b.Stats("events")
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.OldestOffset != 5 || stats.MessageCount != 5 {
		t.Fatalf("after time retention: oldest=%d count=%d, want oldest=5 count=5",
			stats.OldestOffset, stats.MessageCount)
	}

	msgs := drain(t, addr, "events", "reader")
	for _, m := range msgs {
		if m.Key != "new" {
			t.Fatalf("offset %d survived retention but is an old message: %s", m.Offset, m.Value)
		}
	}
}
