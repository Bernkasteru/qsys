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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	mysqlorg "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

type UpdEngine struct {
	cfg     *config.Config
	mysqlDB *db.OrderRepo
	redisDB *db.RedisRepo
	updSvr  *app.UpdSvr
	// canal       *canal.Canal
	canalPtr    atomic.Pointer[canal.Canal]
	posManager  *app.PosManager // 自定义记忆点位
	kafkaReader *kafka.Reader
	dlqWriter   *kafka.Writer // Dead letter queue
	initPos     app.LogEntry

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

const (
	leaderKey   = "qsys:canal:leader"
	lockTTL     = 10 * time.Second
	renewIntrvl = 3 * time.Second
	retryIntrvl = 2 * time.Second
	cleanTout   = 2 * time.Second
	fetchIntrvl = 500 * time.Millisecond
	dataDir     = "./canal_data"
)

// NewUpdEngine 失败返回 error，由 main 处理清理逻辑
func NewUpdEngine(cfgPath string) (*UpdEngine, error) {
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
		PoolSize: 32,
	})

	pm := app.NewPosManager(dataDir, rRepo)
	if err = pm.Load(); err != nil {
		log.Printf("[Upd_main] Warning! Cannot load historical pos ヽ(ー_ー)┌: %v", err)
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

	return &UpdEngine{
		cfg:         cfg,
		mysqlDB:     mRepo,
		redisDB:     rRepo,
		updSvr:      app.NewUpdSvr(mRepo), // 处理单条逻辑
		posManager:  pm,
		kafkaReader: kfkRdr,
		dlqWriter:   dlqWtr,
		ctx:         ctx,
		cancel:      cancel,
	}, nil
}

func (e *UpdEngine) buildCanal() (*canal.Canal, error) {
	dsnCfg, err := mysql.ParseDSN(e.cfg.MySQL.DSN)
	if err != nil {
		return nil, err
	}

	canalCfg := canal.NewDefaultConfig()
	canalCfg.Addr = dsnCfg.Addr
	canalCfg.User = dsnCfg.User
	canalCfg.Password = dsnCfg.Passwd
	canalCfg.Dump.ExecutionPath = "" // 禁用 mysqldump, 仅监听 binlog
	canalCfg.Flavor = mysqlorg.MySQLFlavor

	cn, err := canal.NewCanal(canalCfg)
	if err != nil {
		return nil, fmt.Errorf("Failed to init canal: %w", err)
	}
	synch := app.NewRedisSyncer(e.mysqlDB, e.redisDB, e.posManager)
	cn.SetEventHandler(synch)

	return cn, nil
}

func (e *UpdEngine) Start() {
	// goroutine a: canal 选主 + binlog 同步 (mysql -> redis) [1]
	e.wg.Go(func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[Upd_canal] Panic! %v\n%s", r, debug.Stack())
			}
		}()

		hostname, _ := os.Hostname()
		if hostname == "" {
			hostname = "unknown"
		}
		nodename := fmt.Sprintf("%s:%d", hostname, os.Getpid())

		for {
			select {
			case <-e.ctx.Done():
				return
			default:
			}

			// 抢夺主节点锁
			leaderId := uuid.NewString()
			ok, err := e.redisDB.SetNX(e.ctx, leaderKey, leaderId, lockTTL)
			if !ok {
				if err != nil {
					log.Printf("[Upd_canal] Cannot get ld lock: %v", err)
				}
				time.Sleep(retryIntrvl)
				continue
			}

			// 抢锁成功
			curPos := e.posManager.Get()
			log.Printf("[Upd_canal] %s become master, sync from mem", nodename)

			// 构造 canal instance
			cn, err := e.buildCanal()
			if err != nil {
				log.Printf("[Upd_canal] Failed to create canal: %v", err)
				cleanCtx, cleanCancel := context.WithTimeout(context.WithoutCancel(e.ctx), cleanTout)
				if cmdErr := e.redisDB.Eval(cleanCtx, db.UnlockScript, []string{leaderKey}, leaderId).Err(); cmdErr != nil {
					log.Printf("[Upd_canal] Warning! Best-effort unlock failed: %v", cmdErr)
				}
				cleanCancel()
				time.Sleep(retryIntrvl)
				continue
			}
			e.canalPtr.Store(cn)

			// Watchdog goroutine 启动
			wdCtx, wdCancel := context.WithCancel(e.ctx)
			e.wg.Go(func() {
				tk := time.NewTicker(renewIntrvl)
				defer tk.Stop()

				for {
					select {
					case <-tk.C:
						cmd := e.redisDB.Eval(wdCtx, db.RenewScript, []string{leaderKey}, leaderId, int(lockTTL.Seconds()))
						val, err := cmd.Int()
						if err != nil || val != 1 {
							log.Printf("[Upd_canal] Lock lost / Renew failed.. Stop canal")
							if c1 := e.canalPtr.Load(); c1 != nil {
								c1.Close()
							}
							return
						}
					case <-wdCtx.Done():
						return
					}
				}
			})

			// 启动同步, GTID -> 位点 -> 最新
			var runErr error
			isStarted := false
			if curPos.GTID != "" {
				gtidSet, err := mysqlorg.ParseGTIDSet(mysqlorg.MySQLFlavor, curPos.GTID)
				if err != nil {
					log.Printf("[Upd_canal] Warning! Failed to parse gtidSet: %v; try other ways..", err)
				} else {
					runErr = cn.StartFromGTID(gtidSet)
					isStarted = true
				}
			}
			if !isStarted && curPos.Posi.Name != "" && curPos.Posi.Pos > 0 {
				runErr = cn.RunFrom(curPos.Posi)
				isStarted = true
			}
			if !isStarted {
				runErr = cn.Run()
			}

			// 清理逻辑
			wdCancel()
			if runErr != nil && runErr != context.Canceled {
				log.Printf("[Upd_canal] Sync exit with err: %v", runErr)
			}
			e.canalPtr.Store((*canal.Canal)(nil)) // 防脏指针残留

			// 释放 Redis 锁 [使用独立上下文]
			cleanCtx, cleanCancel := context.WithTimeout(context.WithoutCancel(e.ctx), cleanTout)
			if cmdErr := e.redisDB.Eval(cleanCtx, db.UnlockScript, []string{leaderKey}, leaderId).Err(); cmdErr != nil {
				log.Printf("[Upd_canal] Leader unlock failed: %v", cmdErr)
			} else {
				//
			}
			cleanCancel()
			cn.Close()
			log.Println("[Upd_canal] Master exit, retry election..")
			time.Sleep(retryIntrvl)
		}

	})

	// goroutine b: Kafka 消费逻辑 [3]
	msgChan := make(chan kafka.Message, e.cfg.Kafka.MaxQueueSize)
	for i := 0; i < e.cfg.Kafka.WorkerNum; i++ {
		e.wg.Add(1)
		go func(wk int) {
			defer e.wg.Done()
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[Upd_kafka/%d] Panic! %v\n%s", wk, r, debug.Stack())
				}
			}()

			for msg := range msgChan {
				hdCtx, hdCancel := context.WithTimeout(e.ctx, e.cfg.Kafka.SendTimeout)
				err := e.updSvr.Handle(hdCtx, msg)
				hdCancel()
				cleanCtx, cleanCancel := context.WithTimeout(context.WithoutCancel(e.ctx), cleanTout)
				if err != nil {
					log.Printf("[Upd_kafka/%d] Handle msg err, send to dlq: %v", wk, err)
					dlqMsg := kafka.Message{
						Key:     msg.Key,
						Value:   msg.Value,
						Headers: msg.Headers,
					}
					// 尝试写入 dlq
					if dlqErr := e.dlqWriter.WriteMessages(cleanCtx, dlqMsg); dlqErr != nil {
						log.Printf("[Upd_kafka/%d] !!Cannot write into dlq, give up committing.. %v", wk, dlqErr)
						cleanCancel()
						continue // 不 Commit, 保留在原 Topic
					}
					log.Printf("[Upd_kafka/%d] Passed to dlq", wk)
				}

				// 提交 offset
				if err := e.kafkaReader.CommitMessages(cleanCtx, msg); err != nil {
					log.Printf("[Upd_kafka/%d] Warning! Commit offset failed: %v", wk, err)
				}
				cleanCancel()
			}
		}(i)
	}

	// goroutine c: Kafka 拉取分发 [2]
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
					log.Printf("[Upd_kafka] Fetch msg failed: %v", err)
					time.Sleep(fetchIntrvl)
				}
				continue
			}
			msgChan <- msg
		}
	})
	log.Println("[Upd_main] UpdEngine started ok..")
}

func (e *UpdEngine) Close() {
	if c1 := e.canalPtr.Load(); c1 != nil {
		c1.Close()
	}
	e.cancel()
	e.wg.Wait()

	// 释放底层资源
	e.posManager.Close()
	_ = e.kafkaReader.Close()
	_ = e.dlqWriter.Close()
	_ = e.mysqlDB.Close()
	_ = e.redisDB.Close()
	log.Println("[Upd_main] Engine shutdown")
}

func main() {
	cfgPath := "./deploy/config.yml"
	if len(os.Args) > 1 {
		cfgPath = os.Args[1]
	}

	e1, err := NewUpdEngine(cfgPath)
	if err != nil {
		log.Fatalf("[Upd_main] Init err: %v", err)
	}
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	e1.Start()
	<-sigChan
	log.Println("[Upd_main] Shutdown signal received")
	e1.Close()
	os.Exit(0)
}
