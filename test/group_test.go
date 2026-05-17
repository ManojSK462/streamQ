package test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"streamq/broker"
	"streamq/client"
)

// Two consumer groups on the same topic each receive every message: groups are
// independent readers, not a shared queue.
func TestTwoGroupsEachReceiveAllMessages(t *testing.T) {
	_, addr := startBroker(t, broker.Config{})

	producer, err := client.NewProducer(addr)
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	defer producer.Close()

	const total = 40
	for i := 0; i < total; i++ {
		if _, err := producer.Publish("events", "k", []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	groupA := drain(t, addr, "events", "group-a")
	groupB := drain(t, addr, "events", "group-b")
	if len(groupA) != total || len(groupB) != total {
		t.Fatalf("group-a got %d, group-b got %d, want %d each", len(groupA), len(groupB), total)
	}
}

// Two consumers in one group split the topic between them: every message is
// delivered to exactly one consumer and both do some of the work.
func TestConsumerGroupSplitsWorkAcrossConsumers(t *testing.T) {
	_, addr := startBroker(t, broker.Config{})

	producer, err := client.NewProducer(addr)
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	const total = 60
	for i := 0; i < total; i++ {
		if _, err := producer.Publish("events", "k", []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	producer.Close()

	var mu sync.Mutex
	seen := make(map[uint64]string)

	consume := func(id string, counts map[string]int) {
		consumer, err := client.NewConsumer(addr, client.ConsumerOptions{
			Topic: "events", Group: "workers", ID: id,
		})
		if err != nil {
			t.Errorf("new consumer %s: %v", id, err)
			return
		}
		defer consumer.Close()
		for {
			msgs, err := consumer.Fetch(1, 150*time.Millisecond)
			if err != nil {
				t.Errorf("consumer %s fetch: %v", id, err)
				return
			}
			if len(msgs) == 0 {
				return
			}
			for _, m := range msgs {
				mu.Lock()
				if other, dup := seen[m.Offset]; dup {
					mu.Unlock()
					t.Errorf("offset %d delivered to %s and %s", m.Offset, other, id)
					return
				}
				seen[m.Offset] = id
				counts[id]++
				mu.Unlock()
			}
			if err := consumer.Commit(msgs[len(msgs)-1].Offset + 1); err != nil {
				t.Errorf("consumer %s commit: %v", id, err)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}

	counts := make(map[string]int)
	var wg sync.WaitGroup
	for _, id := range []string{"worker-1", "worker-2"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			consume(id, counts)
		}(id)
	}
	wg.Wait()

	if len(seen) != total {
		t.Fatalf("group delivered %d distinct messages, want %d", len(seen), total)
	}
	if counts["worker-1"] == 0 || counts["worker-2"] == 0 {
		t.Fatalf("work was not split: worker-1=%d worker-2=%d", counts["worker-1"], counts["worker-2"])
	}
}
