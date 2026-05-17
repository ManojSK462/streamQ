// Package distkv is the bridge that lets a distKV node publish its committed
// writes to StreamQ. distKV constructs a Publisher once and calls Publish from
// inside its Raft state machine's apply step, after a command is durable.
package distkv

import (
	"encoding/json"
	"strings"
	"time"

	"streamq/client"
)

// Topics distKV publishes to. Routing is by key prefix so distKV does not need
// to know the topic taxonomy beyond passing the key and operation.
const (
	TopicConfigChanges  = "config.changes"
	TopicSessionEvents  = "session.events"
	TopicSessionExpired = "session.expired"
	TopicKVChanges      = "kv.changes"
)

// CommandEvent is the JSON payload published for each committed distKV write.
type CommandEvent struct {
	Op        string    `json:"op"`
	Key       string    `json:"key"`
	Value     string    `json:"value,omitempty"`
	Term      uint64    `json:"term"`
	Timestamp time.Time `json:"timestamp"`
}

// TopicForCommand maps a distKV key and operation to the StreamQ topic that
// should carry it.
func TopicForCommand(key, op string) string {
	switch {
	case strings.HasPrefix(key, "config::"):
		return TopicConfigChanges
	case strings.HasPrefix(key, "session::"):
		if strings.EqualFold(op, "DELETE") {
			return TopicSessionExpired
		}
		return TopicSessionEvents
	default:
		return TopicKVChanges
	}
}

// Publisher publishes distKV command events to a StreamQ broker.
type Publisher struct {
	producer *client.Producer
}

// NewPublisher connects to a StreamQ broker. distKV holds one Publisher for the
// lifetime of the node.
func NewPublisher(brokerAddr string) (*Publisher, error) {
	producer, err := client.NewProducer(brokerAddr)
	if err != nil {
		return nil, err
	}
	return &Publisher{producer: producer}, nil
}

// Publish routes a command event to its topic, encodes it as JSON and appends
// it. The event key is used as the message key, so all events for one distKV
// key stay co-located in the log. It returns the assigned offset.
func (p *Publisher) Publish(ev CommandEvent) (uint64, error) {
	if ev.Timestamp.IsZero() {
		ev.Timestamp = time.Now()
	}
	payload, err := json.Marshal(ev)
	if err != nil {
		return 0, err
	}
	return p.producer.Publish(TopicForCommand(ev.Key, ev.Op), ev.Key, payload)
}

func (p *Publisher) Close() error {
	return p.producer.Close()
}
