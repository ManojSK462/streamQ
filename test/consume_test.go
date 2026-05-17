package test

import (
	"fmt"
	"testing"
	"time"

	"streamq/broker"
	"streamq/client"
)

// A fresh consumer group starting at offset zero replays the entire topic
// history, independent of groups that have already consumed it.
func TestReplayFromBeginning(t *testing.T) {
	_, addr := startBroker(t, broker.Config{})

	producer, err := client.NewProducer(addr)
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	defer producer.Close()

	const total = 50
	for i := 0; i < total; i++ {
		if _, err := producer.Publish("config.changes", "k", []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	live := drain(t, addr, "config.changes", "service-a")
	if len(live) != total {
		t.Fatalf("service-a consumed %d messages, want %d", len(live), total)
	}

	replay := drain(t, addr, "config.changes", "audit")
	if len(replay) != total {
		t.Fatalf("audit replay consumed %d messages, want %d", len(replay), total)
	}
	for i, m := range replay {
		if m.Offset != uint64(i) {
			t.Fatalf("replay message %d has offset %d", i, m.Offset)
		}
	}
}

// After a broker restart the topic logs survive and a consumer group resumes
// from its last committed offset rather than re-reading the whole topic.
func TestConsumerResumesAfterBrokerRestart(t *testing.T) {
	dir := t.TempDir()
	cfg := broker.Config{DataDir: dir}

	b, err := broker.New(cfg)
	if err != nil {
		t.Fatalf("new broker: %v", err)
	}
	srv, err := broker.Serve(b, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("serve: %v", err)
	}
	addr := srv.Addr()

	producer, err := client.NewProducer(addr)
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	for i := 0; i < 10; i++ {
		if _, err := producer.Publish("events", "k", []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	producer.Close()

	consumer, err := client.NewConsumer(addr, client.ConsumerOptions{Topic: "events", Group: "g1"})
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	first, err := consumer.Fetch(5, time.Second)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(first) != 5 {
		t.Fatalf("first fetch returned %d messages, want 5", len(first))
	}
	if err := consumer.Commit(5); err != nil {
		t.Fatalf("commit: %v", err)
	}
	consumer.Close()

	srv.Close()
	if err := b.Close(); err != nil {
		t.Fatalf("close broker: %v", err)
	}

	b2, err := broker.New(cfg)
	if err != nil {
		t.Fatalf("reopen broker: %v", err)
	}
	srv2, err := broker.Serve(b2, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	defer func() {
		srv2.Close()
		b2.Close()
	}()

	resumed, err := client.NewConsumer(srv2.Addr(), client.ConsumerOptions{Topic: "events", Group: "g1"})
	if err != nil {
		t.Fatalf("new resumed consumer: %v", err)
	}
	defer resumed.Close()

	msgs, err := resumed.Fetch(100, time.Second)
	if err != nil {
		t.Fatalf("fetch after restart: %v", err)
	}
	if len(msgs) != 5 {
		t.Fatalf("resumed consumer fetched %d messages, want 5", len(msgs))
	}
	if msgs[0].Offset != 5 {
		t.Fatalf("resumed consumer started at offset %d, want 5", msgs[0].Offset)
	}
}
