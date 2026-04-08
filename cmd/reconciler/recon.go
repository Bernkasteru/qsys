package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os/signal"
	"qsys/internal/config"
	"qsys/internal/db"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type setVal struct{}

const (
	bSize      = 1400
	lockKey    = "qsys:recon:lock"
	lockTTL    = 5 * time.Minute
	renewIntvl = 2 * time.Minute
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var confpath string
	flag.StringVar(&confpath, "conf", "deploy/config.yml", "Path to config file")
	flag.Parse()
	cfg, err := config.LoadConfig(confpath)
	if err != nil {
		log.Fatalf("[Recon] Failed to load config: %v", err)
	}

	log.Println("[Recon] Start external sync..")
	mdb, err := db.NewMySQLConn(cfg.MySQL.DSN)
	if err != nil {
		log.Fatalf("[Recon] Failed to connect mysql: %v", err)
	}
	defer mdb.Close()
	rdb, err := db.NewRedisConn(db.RedisConfig{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
		PoolSize: 16,
	})
	if err != nil {
		log.Fatalf("[Recon] Failed to connect redis: %v", err) // 可能不会被调用
	}
	defer rdb.Close()

	lockVal := uuid.NewString()
	err = rdb.SetArgs(ctx, lockKey, lockVal, redis.SetArgs{
		Mode: "NX",
		TTL:  lockTTL,
	}).Err()
	if err == redis.Nil {
		return
	} else if err != nil {
		log.Fatalf("[Recon] Cannot lock: %v", err)
	}

	// 锁上下文, 可停止 watchdog 续期写成
	lockCtx, cancelLock := context.WithCancel(ctx)
	defer func() {
		cancelLock()
		if err := rdb.Eval(context.WithoutCancel(ctx), db.UnlockScript, []string{lockKey}, lockVal).Err(); err != nil {
			log.Printf("[Recon] Cannot unlock: %v", err)
		} else {
			log.Println("[Recon] Lock safely released")
		}
	}()

	go func() {
		tk := time.NewTicker(renewIntvl)
		defer tk.Stop()
		for {
			select {
			case <-tk.C:
				rdb.Eval(lockCtx, db.RenewScript, []string{lockKey}, lockVal, int(lockTTL.Seconds()))
			case <-lockCtx.Done():
				return
			}
		}
	}()

	st := time.Now()
	var totalDbScan, totalRedisScan, needAdd, needRem int

	// Stage 1: Mysql -> redis, 前有则后有
	lastClientId := ""
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		batchCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		query := `SELECT DISTINCT client_id FROM orders WHERE client_id > ? ORDER BY client_id ASC LIMIT ?`
		rows, err := mdb.QueryContext(batchCtx, query, lastClientId, bSize)
		if err != nil {
			cancel()
			log.Printf("[Recon] Stage 1, Mysql query crashed at cursor %s: %v", lastClientId, err)
			return
		}
		var batch []string
		for rows.Next() {
			var cid string
			if err := rows.Scan(&cid); err != nil {
				log.Printf("Failed to parse client_id: %v", err)
				return
			}
			batch = append(batch, cid)
		}
		if err := rows.Err(); err != nil {
			cancel()
			log.Printf("[Recon] Fatal! Cursor traversal interrupted: %v", err)
			return
		}
		rows.Close()

		n := len(batch)
		if n == 0 {
			cancel()
			break // 扫库结束
		}
		lastClientId = batch[n-1]
		totalDbScan += n

		pipe := rdb.Pipeline() // Redis Pipeline 批量操作
		for _, cid := range batch {
			pipe.SAdd(batchCtx, db.Ac, cid)
		}
		cmds, err := pipe.Exec(batchCtx)
		if err != nil {
			cancel()
			log.Printf("[Recon] Failed to exec pipeline sadd: %v", err)
			return
		}
		for _, cmd := range cmds {
			if intCmd, ok := cmd.(*redis.IntCmd); ok && intCmd.Val() > 0 {
				needAdd++
			}
		}
		cancel()
	}
	log.Printf("[Recon] Stage 1  扫库: %d 人, 补录: %d 人", totalDbScan, needAdd)

	// Stage 2: Redis -> mysql, 前有后无则除
	var cursor uint64
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		batchCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		var keys []string
		var err error
		keys, cursor, err = rdb.SScan(batchCtx, db.Ac, cursor, "", bSize).Result()
		if err != nil {
			cancel()
			log.Printf("[Recon] Cannot sscan: %v", err)
			return
		}

		if len(keys) > 0 {
			n := len(keys)
			totalRedisScan += n

			// 组装 mysql in 查询
			phs := make([]string, n)
			args := make([]any, n)
			for i, k := range keys {
				phs[i] = "?"
				args[i] = k
			}
			inQuery := fmt.Sprintf("SELECT DISTINCT client_id FROM orders WHERE client_id IN (%s)", strings.Join(phs, ","))
			rows, err := mdb.QueryContext(batchCtx, inQuery, args...)
			if err != nil {
				cancel()
				log.Printf("[Recon] Stage 2, Mysql query crashed: %v", err)
				return
			}

			dbActiveSet := make(map[string]setVal)
			for rows.Next() {
				var cid string
				if err := rows.Scan(&cid); err != nil {
					cancel()
					log.Printf("Failed to parse client_id: %v", err)
					return
				}
				dbActiveSet[cid] = setVal{}
			}
			if err := rows.Err(); err != nil {
				cancel()
				log.Printf("[Recon] Fatal! InQuery traversal interrupted: %v", err)
				return
			}
			rows.Close()

			// 寻找幽灵 client
			pipe := rdb.Pipeline()
			var ghostInBatch int
			for _, cid := range keys {
				if _, ok := dbActiveSet[cid]; !ok {
					pipe.SRem(batchCtx, db.Ac, cid)
					ghostInBatch++
				}
			}
			if ghostInBatch > 0 {
				if _, err := pipe.Exec(batchCtx); err != nil {
					log.Printf("[Recon] Pipeline srem failed (skip..): %v", err)
				} else {
					needRem += ghostInBatch
				}
			}
		}
		cancel()

		if cursor == 0 {
			break // Redis 中, 所有 client_id 扫描完成
		}
	}
	log.Printf("[Recon] Stage 2  扫缓存: %d 人, 清理: %d 人", totalRedisScan, needRem)
	log.Printf("[Recon] 总耗时: %v", time.Since(st))
}
