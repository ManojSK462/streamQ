// Package client provides producer and consumer clients for talking to a
// StreamQ broker over net/rpc.
package client

import (
	"net/rpc"

	"streamq/proto"
)

const serviceName = "StreamQ"

// rpcClient wraps a net/rpc connection with typed StreamQ calls.
type rpcClient struct {
	conn *rpc.Client
}

func dial(addr string) (*rpcClient, error) {
	conn, err := rpc.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &rpcClient{conn: conn}, nil
}

func (c *rpcClient) publish(args proto.PublishArgs) (proto.PublishReply, error) {
	var reply proto.PublishReply
	err := c.conn.Call(serviceName+".Publish", &args, &reply)
	return reply, err
}

func (c *rpcClient) fetch(args proto.FetchArgs) (proto.FetchReply, error) {
	var reply proto.FetchReply
	err := c.conn.Call(serviceName+".Fetch", &args, &reply)
	return reply, err
}

func (c *rpcClient) commit(args proto.CommitArgs) error {
	var reply proto.CommitReply
	return c.conn.Call(serviceName+".Commit", &args, &reply)
}

func (c *rpcClient) close() error {
	return c.conn.Close()
}

// ListTopics returns the names of every topic known to the broker.
func ListTopics(brokerAddr string) ([]string, error) {
	c, err := dial(brokerAddr)
	if err != nil {
		return nil, err
	}
	defer c.close()
	var reply proto.ListTopicsReply
	if err := c.conn.Call(serviceName+".ListTopics", &proto.ListTopicsArgs{}, &reply); err != nil {
		return nil, err
	}
	return reply.Topics, nil
}

// Stats returns offset and consumer-group statistics for a topic.
func Stats(brokerAddr, topic string) (proto.TopicStats, error) {
	c, err := dial(brokerAddr)
	if err != nil {
		return proto.TopicStats{}, err
	}
	defer c.close()
	var reply proto.StatsReply
	if err := c.conn.Call(serviceName+".Stats", &proto.StatsArgs{Topic: topic}, &reply); err != nil {
		return proto.TopicStats{}, err
	}
	return reply.Stats, nil
}
