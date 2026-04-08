package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"qsys/internal/app"
	"qsys/internal/config"
	"qsys/internal/db"
	"runtime/debug"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

type ConsumerEngine struct {
	cfg         *config.Config
	mysqlDB     *db.OrderRepo
	updSvr      *app.UpdSvr
	kafkaReader *kafka.Reader
	dlqWriter   *kafka.Writer

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

const (
	cleanTout   = 2 * time.Second
	fetchIntrvl = 500 * time.Millisecond
)

func NewConsumerEngine(cfgPath string) (*ConsumerEngine, error) {
	cfg, err := config.LoadConfig(cfgPath)
	if err != nil {
		return nil, fmt.Errorf("Failed to load cfg: %w", err)
	}

	mRepo, err := db.NewOrderRepo(cfg.MySQL.DSN)
	if err != nil {
		return nil, fmt.Errorf("Failed to connect mysql: %w", err)
	}

	kfkRdr := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Kafka.Brokers,
		GroupID:        cfg.Kafka.GroupId,
		Topic:          cfg.Kafka.Topic,
		MinBytes:       100,
		MaxBytes:       10e6,
		MaxWait:        cfg.Kafka.BatchTimeout,
		StartOffset:    kafka.FirstOffset, // 可重复, 但不可丢数据
		CommitInterval: 0,
	})

	dlqWtr := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Kafka.Brokers...),
		Topic:        cfg.Kafka.Topic + "_dlq",
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: cfg.Kafka.BatchTimeout,
		RequiredAcks: kafka.RequireAll,
		Compression:  kafka.Gzip,
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &ConsumerEngine{
		cfg:         cfg,
		mysqlDB:     mRepo,
		updSvr:      app.NewUpdSvr(mRepo), // 处理单条逻辑
		kafkaReader: kfkRdr,
		dlqWriter:   dlqWtr,
		ctx:         ctx,
		cancel:      cancel,
	}, nil
}

func (e *ConsumerEngine) Start() {
	// goroutine b: Kafka 消费逻辑
	msgChan := make(chan kafka.Message, e.cfg.Kafka.MaxQueueSize)
	for i := 0; i < e.cfg.Kafka.WorkerNum; i++ {
		e.wg.Add(1)
		go func(wk int) {
			defer e.wg.Done()
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[Csm_kafka/%d] Panic! %v\n%s", wk, r, debug.Stack())
				}
			}()

			for msg := range msgChan {
				hdCtx, hdCancel := context.WithTimeout(e.ctx, e.cfg.Kafka.SendTimeout)
				err := e.updSvr.Handle(hdCtx, msg)
				hdCancel()
				cleanCtx, cleanCancel := context.WithTimeout(context.WithoutCancel(e.ctx), cleanTout)
				if err != nil {
					log.Printf("[Csm_kafka/%d] Handle msg err, send to dlq: %v", wk, err)
					// 尝试写入 dlq
					if dlqErr := e.dlqWriter.WriteMessages(cleanCtx, msg); dlqErr != nil {
						log.Printf("[Csm_kafka/%d] !!Cannot write into dlq, give up committing.. %v", wk, dlqErr)
						cleanCancel()
						continue // 不 Commit, 保留在原 Topic
					}
					log.Printf("[Csm_kafka/%d] Passed to dlq", wk)
				} else {
					// 验证: 利用 Offset, 每处理 200 条打印一次
					if msg.Offset%200 == 0 {
						log.Printf("[Csm_kafka/%d] Consuming all right... db updated: client %s (Offset %d)", wk, string(msg.Key), msg.Offset)
					}
				}

				// 提交 offset
				if err := e.kafkaReader.CommitMessages(cleanCtx, msg); err != nil {
					log.Printf("[Csm_kafka/%d] Warning! Commit offset failed: %v", wk, err)
				}
				cleanCancel()
			}
		}(i)
	}

	// goroutine c: Kafka 拉取分发
	e.wg.Go(func() {
		defer close(msgChan)
		for {
			select {
			case <-e.ctx.Done():
				return
			default:
			}
			msg, err := e.kafkaReader.FetchMessage(e.ctx)
			if err != nil {
				if e.ctx.Err() == nil {
					log.Printf("[Csm_kafka] Fetch msg failed: %v", err)
					time.Sleep(fetchIntrvl)
				}
				continue
			}
			msgChan <- msg
		}
	})
	log.Println("[Csm_main] ConsumerEngine/Upd_svr started ok..")
}

func (e *ConsumerEngine) Close() {
	e.cancel()
	e.wg.Wait()

	// 释放底层资源
	_ = e.kafkaReader.Close()
	_ = e.dlqWriter.Close()
	_ = e.mysqlDB.Close()
	log.Println("[Csm_main] Engine shutdown")
}

func main() {
	cfgPath := "./deploy/config.yml"
	if len(os.Args) > 1 {
		cfgPath = os.Args[1]
	}

	e1, err := NewConsumerEngine(cfgPath)
	if err != nil {
		log.Fatalf("[Csm_main] Init err: %v", err)
	}
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	e1.Start()
	<-sigChan
	log.Println("[Csm_main] Shutdown signal received")
	e1.Close()
	os.Exit(0)
}
