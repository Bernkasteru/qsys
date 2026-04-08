package mq

import (
	"context"
	"fmt"
	"log"
	"qsys/internal/config"
	"qsys/internal/model"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

type Producer struct {
	writer              *kafka.Writer
	Attempt, Succ, Fail atomic.Uint64
	// 状态控制
	closed   atomic.Bool
	taskChan chan *kafka.Message
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
}

func New() *Producer {
	cfg := config.GlobalConfig.Kafka
	ctx, cancel := context.WithCancel(context.Background())
	writer := kafka.Writer{
		Addr:     kafka.TCP(cfg.Brokers...),
		Topic:    cfg.Topic,
		Balancer: &kafka.Hash{}, // 确保同 client_id 进同一个分区

		Async:        false,
		BatchSize:    cfg.BatchSize,
		BatchTimeout: cfg.BatchTimeout,
		// 失败重试
		// MaxAttempts: 3,
		// WriteBackoffMin: 100 * time.Millisecond,
		WriteTimeout: 8 * time.Second,

		// 安全 & 优化
		RequiredAcks: kafka.RequireAll, // 3 副本写成功 -> Ack
		Compression:  kafka.Snappy,
	}

	p := &Producer{
		writer:   &writer,
		taskChan: make(chan *kafka.Message, cfg.MaxQueueSize),
		ctx:      ctx,
		cancel:   cancel,
	}

	// 启动固定规模的 worker
	for i := 0; i < cfg.WorkerNum; i++ {
		p.wg.Add(1)
		go p.runWorker()
	}
	return p
}

func (p *Producer) runWorker() {
	defer p.wg.Done()
	for {
		select {
		case msg, ok := <-p.taskChan:
			if !ok {
				return
			}
			var err error
			for i := 0; i < 3; i++ {
				err = p.writer.WriteMessages(p.ctx, *msg)
				if err == nil {
					break
				} else {
					time.Sleep(time.Duration(i+1) * 100 * time.Millisecond)
				}
			}
			if err != nil {
				p.Fail.Add(1)
				log.Printf("[Kafka] Failed to send message with key %s: %v\n", string(msg.Key), err)
			} else {
				p.Succ.Add(1)
			}
		case <-p.ctx.Done():
			return
		}
	}
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
	msg := &kafka.Message{
		Key:   []byte(order.ClientId), // 同 client_id 进同一个分区
		Value: value,
	}

	tm := time.NewTimer(config.GlobalConfig.Kafka.SendTimeout)
	defer tm.Stop()
	select {
	case p.taskChan <- msg:
		return nil
	case <-ctx.Done():
		p.Fail.Add(1)
		errMsg := fmt.Sprintf("Context canceled, client: %s", order.ClientId)
		log.Printf("[Kafka] %s", errMsg)
		return fmt.Errorf("%s", errMsg)
	case <-tm.C:
		p.Fail.Add(1)
		errMsg := fmt.Sprintf("Send timeout, client: %s", order.ClientId)
		log.Printf("[Kafka] %s", errMsg)
		return fmt.Errorf("%s", errMsg)
	}
}

// 供外部检查
func (p *Producer) GetQueueLength() int {
	return len(p.taskChan)
}

func (p *Producer) Close() error {
	if !p.closed.CompareAndSwap(false, true) {
		return nil // 已经关闭了
	}
	close(p.taskChan)
	p.wg.Wait()
	p.cancel()
	return p.writer.Close()
}
