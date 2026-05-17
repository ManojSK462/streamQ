package client

import (
	"context"
	"fmt"
	"os"
	"time"

	"streamq/proto"
)

// ConsumerOptions configures a Consumer. Start positions a brand-new consumer
// group and is ignored once the group has a committed offset on the broker.
type ConsumerOptions struct {
	Topic string
	Group string
	ID    string
	Start uint64
}

// Consumer reads messages from a topic as a member of a consumer group. The
// broker tracks committed offsets per group, so each Consumer only needs to
// fetch and commit.
type Consumer struct {
	rpc   *rpcClient
	topic string
	group string
	id    string
	start uint64
}

func NewConsumer(brokerAddr string, opts ConsumerOptions) (*Consumer, error) {
	if opts.Topic == "" || opts.Group == "" {
		return nil, fmt.Errorf("client: consumer requires a topic and group")
	}
	c, err := dial(brokerAddr)
	if err != nil {
		return nil, err
	}
	id := opts.ID
	if id == "" {
		id = defaultConsumerID()
	}
	return &Consumer{
		rpc:   c,
		topic: opts.Topic,
		group: opts.Group,
		id:    id,
		start: opts.Start,
	}, nil
}

func (c *Consumer) ID() string { return c.id }

// Fetch returns the next batch of messages for this consumer. When no messages
// are available it blocks up to maxWait for a publish before returning empty.
func (c *Consumer) Fetch(maxMessages int, maxWait time.Duration) ([]proto.Message, error) {
	reply, err := c.rpc.fetch(proto.FetchArgs{
		Topic:       c.topic,
		Group:       c.group,
		ConsumerID:  c.id,
		FromOffset:  c.start,
		MaxMessages: maxMessages,
		MaxWait:     maxWait,
	})
	if err != nil {
		return nil, err
	}
	return reply.Messages, nil
}

// Commit acknowledges that every message below offset has been processed.
func (c *Consumer) Commit(offset uint64) error {
	return c.rpc.commit(proto.CommitArgs{
		Topic:      c.topic,
		Group:      c.group,
		ConsumerID: c.id,
		Offset:     offset,
	})
}

// Run consumes messages until ctx is cancelled, invoking handler for each one.
// A batch is committed only after every message in it is handled successfully,
// which gives at-least-once delivery: a crash mid-batch redelivers it.
func (c *Consumer) Run(ctx context.Context, handler func(proto.Message) error) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		msgs, err := c.Fetch(256, time.Second)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		processed := true
		var last uint64
		for _, m := range msgs {
			if err := handler(m); err != nil {
				processed = false
				break
			}
			last = m.Offset
		}
		if processed && len(msgs) > 0 {
			if err := c.Commit(last + 1); err != nil {
				return err
			}
		}
	}
}

func (c *Consumer) Close() error {
	return c.rpc.close()
}

func defaultConsumerID() string {
	host, err := os.Hostname()
	if err != nil {
		host = "consumer"
	}
	return fmt.Sprintf("%s-%d-%d", host, os.Getpid(), time.Now().UnixNano()%1_000_000)
}
