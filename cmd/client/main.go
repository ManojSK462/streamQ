// Command client is the StreamQ command line tool: publish, consume, list and
// inspect topics.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"streamq/client"
	"streamq/proto"
)

func main() {
	root := &cobra.Command{
		Use:           "streamq-client",
		Short:         "StreamQ command line client",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.AddCommand(publishCommand(), consumeCommand(), listCommand(), statsCommand())
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func publishCommand() *cobra.Command {
	var brokerAddr, topic, key, value string
	cmd := &cobra.Command{
		Use:   "publish",
		Short: "Publish a message to a topic",
		RunE: func(*cobra.Command, []string) error {
			producer, err := client.NewProducer(brokerAddr)
			if err != nil {
				return err
			}
			defer producer.Close()
			offset, err := producer.Publish(topic, key, []byte(value))
			if err != nil {
				return err
			}
			fmt.Printf("published topic=%s offset=%d\n", topic, offset)
			return nil
		},
	}
	cmd.Flags().StringVar(&brokerAddr, "broker", "localhost:9001", "broker address")
	cmd.Flags().StringVar(&topic, "topic", "", "topic name")
	cmd.Flags().StringVar(&key, "key", "", "message key")
	cmd.Flags().StringVar(&value, "value", "", "message value")
	cmd.MarkFlagRequired("topic")
	cmd.MarkFlagRequired("value")
	return cmd
}

func consumeCommand() *cobra.Command {
	var brokerAddr, topic, group, consumerID, from string
	var maxMessages int
	cmd := &cobra.Command{
		Use:   "consume",
		Short: "Consume messages from a topic as part of a consumer group",
		RunE: func(*cobra.Command, []string) error {
			start, err := parseFrom(from)
			if err != nil {
				return err
			}
			consumer, err := client.NewConsumer(brokerAddr, client.ConsumerOptions{
				Topic: topic,
				Group: group,
				ID:    consumerID,
				Start: start,
			})
			if err != nil {
				return err
			}
			defer consumer.Close()

			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer stop()

			fmt.Printf("consuming topic=%s group=%s consumer=%s (waiting for messages)\n",
				topic, group, consumer.ID())
			for ctx.Err() == nil {
				msgs, err := consumer.Fetch(maxMessages, time.Second)
				if err != nil {
					if ctx.Err() != nil {
						return nil
					}
					return err
				}
				for _, m := range msgs {
					fmt.Printf("offset=%d key=%s value=%s\n", m.Offset, m.Key, string(m.Value))
				}
				if len(msgs) > 0 {
					if err := consumer.Commit(msgs[len(msgs)-1].Offset + 1); err != nil {
						return err
					}
				}
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&brokerAddr, "broker", "localhost:9001", "broker address")
	cmd.Flags().StringVar(&topic, "topic", "", "topic name")
	cmd.Flags().StringVar(&group, "group", "", "consumer group name")
	cmd.Flags().StringVar(&consumerID, "id", "", "consumer id (defaults to a generated id)")
	cmd.Flags().StringVar(&from, "from", "earliest", "start position for a new group: earliest, latest or an offset")
	cmd.Flags().IntVar(&maxMessages, "max", 256, "maximum messages per fetch")
	cmd.MarkFlagRequired("topic")
	cmd.MarkFlagRequired("group")
	return cmd
}

func listCommand() *cobra.Command {
	var brokerAddr string
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List all topics",
		RunE: func(*cobra.Command, []string) error {
			topics, err := client.ListTopics(brokerAddr)
			if err != nil {
				return err
			}
			if len(topics) == 0 {
				fmt.Println("no topics")
				return nil
			}
			for _, t := range topics {
				fmt.Println(t)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&brokerAddr, "broker", "localhost:9001", "broker address")
	return cmd
}

func statsCommand() *cobra.Command {
	var brokerAddr, topic string
	cmd := &cobra.Command{
		Use:   "stats",
		Short: "Show offset and consumer-group statistics for a topic",
		RunE: func(*cobra.Command, []string) error {
			stats, err := client.Stats(brokerAddr, topic)
			if err != nil {
				return err
			}
			fmt.Printf("Topic: %s\n", stats.Name)
			fmt.Printf("Messages: %d\n", stats.MessageCount)
			fmt.Printf("Oldest offset: %d\n", stats.OldestOffset)
			fmt.Printf("Newest offset: %d\n", stats.NewestOffset)
			if len(stats.Groups) == 0 {
				fmt.Println("Consumer groups: none")
				return nil
			}
			parts := make([]string, 0, len(stats.Groups))
			for _, g := range stats.Groups {
				parts = append(parts, fmt.Sprintf("%s (offset: %d, lag: %d)", g.Name, g.Committed, g.Lag))
			}
			fmt.Printf("Consumer groups: %s\n", strings.Join(parts, ", "))
			return nil
		},
	}
	cmd.Flags().StringVar(&brokerAddr, "broker", "localhost:9001", "broker address")
	cmd.Flags().StringVar(&topic, "topic", "", "topic name")
	cmd.MarkFlagRequired("topic")
	return cmd
}

func parseFrom(from string) (uint64, error) {
	switch from {
	case "", "earliest":
		return proto.OffsetEarliest, nil
	case "latest":
		return proto.OffsetLatest, nil
	default:
		offset, err := strconv.ParseUint(from, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid --from %q: want earliest, latest or an offset", from)
		}
		return offset, nil
	}
}
