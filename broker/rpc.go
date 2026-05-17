package broker

import (
	"net"
	"net/rpc"

	"streamq/proto"
)

// rpcServiceName is the name clients prefix onto every method call.
const rpcServiceName = "StreamQ"

// rpcService adapts the broker to the net/rpc method signature. Errors are
// returned directly; net/rpc relays them to the caller as the call error.
type rpcService struct {
	broker *Broker
}

func (s *rpcService) Publish(args *proto.PublishArgs, reply *proto.PublishReply) error {
	offset, err := s.broker.Publish(args.Topic, args.Key, args.Value)
	if err != nil {
		return err
	}
	reply.Offset = offset
	return nil
}

func (s *rpcService) Fetch(args *proto.FetchArgs, reply *proto.FetchReply) error {
	res, err := s.broker.Fetch(*args)
	if err != nil {
		return err
	}
	*reply = res
	return nil
}

func (s *rpcService) Commit(args *proto.CommitArgs, reply *proto.CommitReply) error {
	return s.broker.Commit(*args)
}

func (s *rpcService) ListTopics(args *proto.ListTopicsArgs, reply *proto.ListTopicsReply) error {
	reply.Topics = s.broker.ListTopics()
	return nil
}

func (s *rpcService) Stats(args *proto.StatsArgs, reply *proto.StatsReply) error {
	stats, err := s.broker.Stats(args.Topic)
	if err != nil {
		return err
	}
	reply.Stats = stats
	return nil
}

// Server accepts net/rpc connections and serves them against a broker. Each
// Server owns a private rpc.Server so multiple brokers can run in one process.
type Server struct {
	broker   *Broker
	listener net.Listener
	rpc      *rpc.Server
}

// Serve binds addr and starts accepting connections in the background. An addr
// with port 0 picks a free port, discoverable through Addr.
func Serve(b *Broker, addr string) (*Server, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	srv := rpc.NewServer()
	if err := srv.RegisterName(rpcServiceName, &rpcService{broker: b}); err != nil {
		ln.Close()
		return nil, err
	}
	s := &Server{broker: b, listener: ln, rpc: srv}
	go s.acceptLoop()
	return s, nil
}

func (s *Server) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		go s.rpc.ServeConn(conn)
	}
}

func (s *Server) Addr() string { return s.listener.Addr().String() }

func (s *Server) Close() error { return s.listener.Close() }
