package test

import (
	"encoding/json"
	"testing"
	"time"

	"streamq/broker"
	"streamq/client"
	"streamq/integration/distkv"
	"streamq/proto"
)

// TopicForCommand routes distKV keys to topics by prefix and operation.
func TestDistKVTopicRouting(t *testing.T) {
	cases := []struct {
		key, op, want string
	}{
		{"config::feature_flags::dark_mode", "SET", distkv.TopicConfigChanges},
		{"config::limits::rps", "DELETE", distkv.TopicConfigChanges},
		{"session::user42", "SET", distkv.TopicSessionEvents},
		{"session::user42", "DELETE", distkv.TopicSessionExpired},
		{"inventory::sku-1", "SET", distkv.TopicKVChanges},
	}
	for _, c := range cases {
		if got := distkv.TopicForCommand(c.key, c.op); got != c.want {
			t.Errorf("TopicForCommand(%q, %q) = %q, want %q", c.key, c.op, got, c.want)
		}
	}
}

// A distKV write published through the bridge reaches a subscriber on the
// matching topic, with the command event intact and delivered well within
// 100ms on loopback.
func TestDistKVWriteReachesSubscriber(t *testing.T) {
	_, addr := startBroker(t, broker.Config{})

	publisher, err := distkv.NewPublisher(addr)
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	defer publisher.Close()

	consumer, err := client.NewConsumer(addr, client.ConsumerOptions{
		Topic: distkv.TopicConfigChanges,
		Group: "service-a",
	})
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	defer consumer.Close()

	type fetchResult struct {
		msgs []proto.Message
		at   time.Time
	}
	done := make(chan fetchResult, 1)
	go func() {
		msgs, err := consumer.Fetch(10, 2*time.Second)
		if err != nil {
			t.Errorf("fetch: %v", err)
		}
		done <- fetchResult{msgs: msgs, at: time.Now()}
	}()

	// Give the consumer time to block inside the long-poll before publishing.
	time.Sleep(50 * time.Millisecond)

	event := distkv.CommandEvent{
		Op:    "SET",
		Key:   "config::feature_flags::dark_mode",
		Value: "true",
		Term:  7,
	}
	publishedAt := time.Now()
	if _, err := publisher.Publish(event); err != nil {
		t.Fatalf("publish event: %v", err)
	}

	result := <-done
	if len(result.msgs) != 1 {
		t.Fatalf("subscriber received %d messages, want 1", len(result.msgs))
	}
	if latency := result.at.Sub(publishedAt); latency > 100*time.Millisecond {
		t.Fatalf("end-to-end latency %v exceeds 100ms", latency)
	}

	var got distkv.CommandEvent
	if err := json.Unmarshal(result.msgs[0].Value, &got); err != nil {
		t.Fatalf("decode event: %v", err)
	}
	if got.Op != event.Op || got.Key != event.Key || got.Value != event.Value || got.Term != event.Term {
		t.Fatalf("event round-trip mismatch: got %+v, want %+v", got, event)
	}
	if result.msgs[0].Key != event.Key {
		t.Fatalf("message key = %q, want %q", result.msgs[0].Key, event.Key)
	}
}
