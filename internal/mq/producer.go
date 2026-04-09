package mq

import (
	"context"
	"fmt"
	"log"
	"qsys/internal/config"
	"qsys/internal/model"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

type Producer struct {
	writer              *kafka.Writer
	Attempt, Succ, Fail atomic.Uint64
	// 状态控制
	closed atomic.Bool
}

func New() *Producer {
	cfg := config.GlobalConfig.Kafka
	p := &Producer{}
	writer := &kafka.Writer{
		Addr:     kafka.TCP(cfg.Brokers...),
		Topic:    cfg.Topic,
		Balancer: &kafka.Hash{}, // 确保同 client_id 进同一个分区

		Async:        true, // 异步发信
		BatchSize:    cfg.BatchSize,
		BatchTimeout: cfg.BatchTimeout,
		// 失败重试
		// MaxAttempts: 3,
		// WriteBackoffMin: 100 * time.Millisecond,
		WriteTimeout: 8 * time.Second,

		// 安全 & 优化
		RequiredAcks: kafka.RequireAll,
		Compression:  kafka.Snappy,
		Completion: func(messages []kafka.Message, err error) {
			count := uint64(len(messages))
			if err != nil {
				p.Fail.Add(count)
				log.Printf("[Kafka] Failed to send %d messages: %v\n", count, err)
			} else {
				p.Succ.Add(count)
			}
		},
	}

	p.writer = writer
	return p
}

func (p *Producer) SendOrder(ctx context.Context, order *model.Order) error {
	if p.closed.Load() {
		return fmt.Errorf("Producer closing, reject order for client %s", order.ClientId)
	}
	if err := order.Validate(); err != nil {
		return err
	}
	value, err := proto.Marshal(order)
	if err != nil {
		return fmt.Errorf("Marshal .proto order failed: %w", err)
	}
	p.Attempt.Add(1)
	msg := kafka.Message{
		Key:   []byte(order.ClientId), // 同 client_id 进同一个分区
		Value: value,
	}
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("Failed to send message with key %s: %v", string(msg.Key), err)
	}
	return nil
}

// 供外部检查
func (p *Producer) GetQueueLength() int64 {
	return p.writer.Stats().QueueLength
}

func (p *Producer) Close() error {
	if !p.closed.CompareAndSwap(false, true) {
		return nil // 已经关闭了
	}
	return p.writer.Close()
}
