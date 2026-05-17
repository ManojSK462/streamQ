package test

import (
	"testing"
	"time"

	"streamq/broker"
	"streamq/client"
)

// BenchmarkBrokerPublish measures the storage layer in isolation: encoding a
// record and appending it to the on-disk log, with no RPC in the path.
func BenchmarkBrokerPublish(b *testing.B) {
	bk, err := broker.New(broker.Config{DataDir: b.TempDir()})
	if err != nil {
		b.Fatalf("new broker: %v", err)
	}
	defer bk.Close()

	value := make([]byte, 128)
	b.SetBytes(int64(len(value)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := bk.Publish("bench", "k", value); err != nil {
			b.Fatalf("publish: %v", err)
		}
	}
}

// BenchmarkPublish measures the round-trip latency of a single synchronous
// publish RPC. Throughput here is bound by the network round trip, not the
// broker; see BenchmarkPublishParallel for aggregate broker throughput.
func BenchmarkPublish(b *testing.B) {
	_, addr := startBroker(b, broker.Config{})
	producer, err := client.NewProducer(addr)
	if err != nil {
		b.Fatalf("new producer: %v", err)
	}
	defer producer.Close()

	value := make([]byte, 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := producer.Publish("bench", "k", value); err != nil {
			b.Fatalf("publish: %v", err)
		}
	}
}

// BenchmarkPublishParallel measures aggregate publish throughput with one
// producer connection per goroutine, the realistic shape of many services
// publishing to a shared broker.
func BenchmarkPublishParallel(b *testing.B) {
	_, addr := startBroker(b, broker.Config{})
	b.SetBytes(128)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		producer, err := client.NewProducer(addr)
		if err != nil {
			b.Error(err)
			return
		}
		defer producer.Close()
		value := make([]byte, 128)
		for pb.Next() {
			if _, err := producer.Publish("bench", "k", value); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

// BenchmarkEndToEnd measures the latency of a publish immediately followed by
// a consumer fetching and committing that message.
func BenchmarkEndToEnd(b *testing.B) {
	_, addr := startBroker(b, broker.Config{})
	producer, err := client.NewProducer(addr)
	if err != nil {
		b.Fatalf("new producer: %v", err)
	}
	defer producer.Close()

	consumer, err := client.NewConsumer(addr, client.ConsumerOptions{Topic: "bench", Group: "bench"})
	if err != nil {
		b.Fatalf("new consumer: %v", err)
	}
	defer consumer.Close()

	value := make([]byte, 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := producer.Publish("bench", "k", value); err != nil {
			b.Fatalf("publish: %v", err)
		}
		msgs, err := consumer.Fetch(1, time.Second)
		if err != nil {
			b.Fatalf("fetch: %v", err)
		}
		if len(msgs) != 1 {
			b.Fatalf("fetched %d messages, want 1", len(msgs))
		}
		if err := consumer.Commit(msgs[0].Offset + 1); err != nil {
			b.Fatalf("commit: %v", err)
		}
	}
}
