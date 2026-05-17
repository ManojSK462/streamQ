package test

import (
	"fmt"
	"testing"
	"time"

	"streamq/broker"
	"streamq/client"
)

// Publishing one message and consuming it back returns the same key, value and
// offset zero.
func TestPublishAndConsumeSingleMessage(t *testing.T) {
	_, addr := startBroker(t, broker.Config{})

	producer, err := client.NewProducer(addr)
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	defer producer.Close()

	offset, err := producer.Publish("events", "k1", []byte("hello"))
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if offset != 0 {
		t.Fatalf("first publish offset = %d, want 0", offset)
	}

	consumer, err := client.NewConsumer(addr, client.ConsumerOptions{Topic: "events", Group: "g1"})
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	defer consumer.Close()

	msgs, err := consumer.Fetch(10, time.Second)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("fetched %d messages, want 1", len(msgs))
	}
	if msgs[0].Offset != 0 || msgs[0].Key != "k1" || string(msgs[0].Value) != "hello" {
		t.Fatalf("unexpected message: %+v", msgs[0])
	}
	if msgs[0].Topic != "events" {
		t.Fatalf("message topic = %q, want events", msgs[0].Topic)
	}
}

// Messages within a topic are delivered in strict offset order, regardless of
// how a consumer batches its fetches.
func TestMessagesAreStrictlyOrdered(t *testing.T) {
	_, addr := startBroker(t, broker.Config{})

	producer, err := client.NewProducer(addr)
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	defer producer.Close()

	const total = 200
	for i := 0; i < total; i++ {
		offset, err := producer.Publish("events", "k", []byte(fmt.Sprintf("v%d", i)))
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
		if offset != uint64(i) {
			t.Fatalf("publish %d returned offset %d", i, offset)
		}
	}

	consumer, err := client.NewConsumer(addr, client.ConsumerOptions{Topic: "events", Group: "g1"})
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	defer consumer.Close()

	var got uint64
	for got < total {
		msgs, err := consumer.Fetch(7, time.Second)
		if err != nil {
			t.Fatalf("fetch: %v", err)
		}
		for _, m := range msgs {
			if m.Offset != got {
				t.Fatalf("out-of-order message: got offset %d, want %d", m.Offset, got)
			}
			if want := fmt.Sprintf("v%d", got); string(m.Value) != want {
				t.Fatalf("offset %d carried %q, want %q", m.Offset, m.Value, want)
			}
			got++
		}
		if err := consumer.Commit(got); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}
}
