// Command broker starts a StreamQ broker: it serves RPC, runs retention and
// periodically snapshots consumer offsets, shutting down cleanly on a signal.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"streamq/broker"
)

func main() {
	port := flag.Int("port", 9001, "TCP port to listen on")
	dataDir := flag.String("data-dir", "data/streamq", "directory holding topic logs")
	leaseTTL := flag.Duration("lease-ttl", broker.DefaultLeaseTTL, "redelivery timeout for an uncommitted batch")
	snapshotEvery := flag.Duration("snapshot-interval", 5*time.Second, "how often committed offsets are snapshotted")
	retentionMode := flag.String("retention", "none", "retention mode: none, time or size")
	retentionMaxAge := flag.Duration("retention-max-age", 24*time.Hour, "max message age under time retention")
	retentionMaxSize := flag.Int64("retention-max-size", 1<<30, "max log size in bytes under size retention")
	retentionInterval := flag.Duration("retention-interval", time.Minute, "interval between compaction passes")
	flag.Parse()

	cfg := broker.Config{
		DataDir:  *dataDir,
		LeaseTTL: *leaseTTL,
		Retention: broker.RetentionPolicy{
			Mode:     parseRetentionMode(*retentionMode),
			MaxAge:   *retentionMaxAge,
			MaxSize:  *retentionMaxSize,
			Interval: *retentionInterval,
		},
	}

	b, err := broker.New(cfg)
	if err != nil {
		log.Fatalf("streamq: %v", err)
	}

	srv, err := broker.Serve(b, fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("streamq: %v", err)
	}
	log.Printf("streamq broker listening on %s, data dir %q", srv.Addr(), *dataDir)

	ctx, cancel := context.WithCancel(context.Background())
	go b.RunRetention(ctx)
	go snapshotLoop(ctx, b, *snapshotEvery)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	<-signals

	log.Print("streamq broker shutting down")
	cancel()
	srv.Close()
	if err := b.Close(); err != nil {
		log.Printf("streamq: shutdown error: %v", err)
	}
}

func parseRetentionMode(mode string) broker.RetentionMode {
	switch mode {
	case "time":
		return broker.RetentionTime
	case "size":
		return broker.RetentionSize
	default:
		return broker.RetentionNone
	}
}

func snapshotLoop(ctx context.Context, b *broker.Broker, interval time.Duration) {
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := b.Snapshot(); err != nil {
				log.Printf("streamq: offset snapshot failed: %v", err)
			}
		}
	}
}
