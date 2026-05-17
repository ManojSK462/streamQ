package client

import "streamq/proto"

// Producer publishes messages to broker topics.
type Producer struct {
	rpc *rpcClient
}

func NewProducer(brokerAddr string) (*Producer, error) {
	c, err := dial(brokerAddr)
	if err != nil {
		return nil, err
	}
	return &Producer{rpc: c}, nil
}

// Publish appends a message to a topic and returns the offset it was assigned.
func (p *Producer) Publish(topic, key string, value []byte) (uint64, error) {
	reply, err := p.rpc.publish(proto.PublishArgs{Topic: topic, Key: key, Value: value})
	if err != nil {
		return 0, err
	}
	return reply.Offset, nil
}

func (p *Producer) Close() error {
	return p.rpc.close()
}
