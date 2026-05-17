package test

import (
	"testing"
	"time"

	"streamq/broker"
	"streamq/client"
	"streamq/proto"
)

// startBroker brings up an in-process broker with an RPC server on a free
// loopback port and returns its address. The broker and server are torn down
// when the test finishes.
func startBroker(tb testing.TB, cfg broker.Config) (*broker.Broker, string) {
	tb.Helper()
	if cfg.DataDir == "" {
		cfg.DataDir = tb.TempDir()
	}
	b, err := broker.New(cfg)
	if err != nil {
		tb.Fatalf("new broker: %v", err)
	}
	srv, err := broker.Serve(b, "127.0.0.1:0")
	if err != nil {
		b.Close()
		tb.Fatalf("serve broker: %v", err)
	}
	tb.Cleanup(func() {
		srv.Close()
		b.Close()
	})
	return b, srv.Addr()
}

// drain consumes a topic from the start of a group, committing as it goes,
// until no further messages are available, and returns everything it read.
func drain(tb testing.TB, addr, topic, group string) []proto.Message {
	tb.Helper()
	consumer, err := client.NewConsumer(addr, client.ConsumerOptions{Topic: topic, Group: group})
	if err != nil {
		tb.Fatalf("new consumer: %v", err)
	}
	defer consumer.Close()

	var all []proto.Message
	for {
		msgs, err := consumer.Fetch(64, 100*time.Millisecond)
		if err != nil {
			tb.Fatalf("fetch: %v", err)
		}
		if len(msgs) == 0 {
			return all
		}
		all = append(all, msgs...)
		if err := consumer.Commit(msgs[len(msgs)-1].Offset + 1); err != nil {
			tb.Fatalf("commit: %v", err)
		}
	}
}
