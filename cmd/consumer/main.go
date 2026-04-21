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
	"qsys/internal/model"
	"runtime/debug"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

type ConsumerEngine struct {
	cfg         *config.Config
	mysqlDB     *db.OrderRepo
	redisDB     *db.RedisRepo
	updSvr      *app.UpdSvr
	kafkaReader *kafka.Reader
	dlqWriter   *kafka.Writer

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

const (
	cleanTout   = 6 * time.Second
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
	rRepo := db.NewRedisRepo(db.RedisConfig{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
		PoolSize: 64,
	})

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
		redisDB:     rRepo,
		updSvr:      app.NewUpdSvr(mRepo, rRepo), // 处理单条逻辑
		kafkaReader: kfkRdr,
		dlqWriter:   dlqWtr,
		ctx:         ctx,
		cancel:      cancel,
	}, nil
}

// 手动实现的轻量 FNV-1a 哈希算法,
// client_id -> 特定worker
func hashBytes(data []byte) uint32 {
	var h uint32 = 2166136261 // offset_basis
	for _, b := range data {
		h ^= uint32(b)
		h *= 16777619 // fnv_prime
	}
	return h
}

func (e *ConsumerEngine) runWorker(wk int, ch <-chan kafka.Message) {
	defer e.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[Csm_kafka/%d] Panic! %v\n%s", wk, r, debug.Stack())
		}
	}()

	batchSize := e.cfg.App.DBBatchSize
	batchTout := e.cfg.App.DBBatchTout
	msgBatch := make([]kafka.Message, 0, batchSize)
	fMap := make(map[model.OrderKey]byte, batchSize) // 初始化折叠 map, 复用内存

	tk := time.NewTicker(batchTout)
	defer tk.Stop()

	// 执行批处理的闭包
	flushBatch := func() {
		if len(msgBatch) == 0 {
			return
		}

		// b.1 操作折叠/合并
		var od model.Order
		for _, msg := range msgBatch {
			od.Reset()
			if err := proto.Unmarshal(msg.Value, &od); err != nil {
				log.Printf("[Csm_kafka/%d] Unmarshal failed, dropping dirty data; "+
					"Topic: %s, Partition: %d, Offset: %d, Err: %v", wk, msg.Topic, msg.Partition, msg.Offset, err)
				continue
			}
			key := model.BuildOrderKey(od.ClientId, od.ExchangeType, od.StockCode)
			fMap[key] = od.Action[0]
		}

		// b.2 调用下游 upd_svr 的业务逻辑
		hdCtx, hdCancel := context.WithTimeout(e.ctx, e.cfg.Kafka.SendTimeout)
		err := e.updSvr.HandleBatch(hdCtx, fMap)
		hdCancel()

		// b.3 处理失败 & Offset 提交
		cleanCtx, cleanCancel := context.WithTimeout(context.WithoutCancel(e.ctx), cleanTout)
		commitOffsets := true // 安全标记
		if err != nil {
			// mlen := len(msgBatch)
			log.Printf("[Csm_kafka/%d] HandleBatch err: %v, sending %d msgs to dlq", wk, err, len(msgBatch))
			// 批量写入 dlq
			dlqMsgs := make([]kafka.Message, len(msgBatch))
			for i, m := range msgBatch {
				dlqMsgs[i] = kafka.Message{
					Key:     m.Key,
					Value:   m.Value,
					Headers: m.Headers,
				}
			}
			if dlqErr := e.dlqWriter.WriteMessages(cleanCtx, dlqMsgs...); dlqErr != nil {
				log.Printf("[Csm_kafka/%d] !!Cannot write into dlq, give up committing.. %v", wk, dlqErr)
				commitOffsets = false
			} else {
				log.Printf("[Csm_kafka/%d] Passed %d msgs to dlq", wk, len(msgBatch))
			}
		} else {
			if len(msgBatch) > 0 {
				off1, off2 := msgBatch[0].Offset, msgBatch[len(msgBatch)-1].Offset
				log.Printf("[Csm_kafka/%d] Batch ok, %d msgs (Offset %d -> %d), Partition: %d",
					wk, len(msgBatch), off1, off2, msgBatch[0].Partition)
			}
		}
		// 正常落库/进入dlq, 提交 offset
		if commitOffsets {
			if err := e.kafkaReader.CommitMessages(cleanCtx, msgBatch...); err != nil {
				log.Printf("[Csm_kafka/%d] Warning! Commit offset failed: %v", wk, err)
			}
		}
		cleanCancel()

		// b.4 重置
		msgBatch = msgBatch[:0]
		clear(fMap)
	}

	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				flushBatch()
				return
			}
			msgBatch = append(msgBatch, msg)
			if len(msgBatch) >= batchSize {
				flushBatch()
				tk.Reset(batchTout)
			}
		case <-tk.C: // 时间到期, 没满也刷入数据
			flushBatch()
		}
	}
}

func (e *ConsumerEngine) Start() {
	// goroutine b: Kafka 消费逻辑
	// 4.10 修复bug: 为每个 worker 创建专属 channel
	workerNum := e.cfg.Kafka.WorkerNum
	workerChans := make([]chan kafka.Message, workerNum)
	for i := range workerNum {
		// 通道容量: 最大队列长度/Worker数, 向上取整
		workerChans[i] = make(chan kafka.Message, e.cfg.Kafka.MaxQueueSize/e.cfg.Kafka.WorkerNum+1)
		e.wg.Add(1)
		go e.runWorker(i, workerChans[i])
	}

	// goroutine c: Kafka 拉取分发
	// 仅依据 Key 进行哈希路由
	e.wg.Go(func() {
		for {
			select {
			case <-e.ctx.Done():
				for _, ch := range workerChans {
					close(ch)
				}
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
			// 哈希取模分发; 同 client_id -> 同 worker
			i := hashBytes(msg.Key) % uint32(workerNum)
			workerChans[i] <- msg
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
	_ = e.redisDB.Close()

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
