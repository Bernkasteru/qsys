package app

import (
	"context"
	"fmt"
	"log"
	"qsys/internal/db"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

type RedisSyncer struct {
	canal.DummyEventHandler
	mysqlRepo  *db.OrderRepo
	redisRepo  *db.RedisRepo
	posManager *PosManager
	fixes      sync.Map
}

func NewRedisSyncer(m *db.OrderRepo, r *db.RedisRepo, pm *PosManager) *RedisSyncer {
	s := &RedisSyncer{mysqlRepo: m, redisRepo: r, posManager: pm}
	go s.compensate() // 后台巡逻、补偿
	return s
}

func (s *RedisSyncer) String() string {
	return "RedisSyncer"
}

// 实现 OnPosSynced 接口方法
func (s *RedisSyncer) OnPosSynced(header *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, flag bool) error {
	s.posManager.Save(pos, set)
	return nil
}

// OnRow 监听 MySQL, 同步到 Redis
func (s *RedisSyncer) OnRow(e *canal.RowsEvent) error {
	// 仅关注 orders 表, 只允许 Insert、Delete、(Update) 进入
	if e.Table.Name != "orders" || (e.Action != canal.InsertAction && e.Action != canal.DeleteAction && e.Action != canal.UpdateAction) {
		return nil
	}
	const colClientId = 1 // 0: id, 1: client_id
	ctx := context.Background()

	for _, row := range e.Rows {
		clientId := fmt.Sprintf("%v", row[colClientId])
		if _, inFixes := s.fixes.Load(clientId); inFixes {
			log.Printf("[Syncer] Client %s 存在未进行的补偿, 触发修复..", clientId)
			if err := s.lookBack(ctx, clientId); err != nil {
				log.Printf("[Syncer] LookBack failed for client %s: %v, skipping for this time", clientId, err)
				continue
			}
			log.Printf("[Syncer] LookBack success for client %s, removed from 'fixes'", clientId)
		}
		s.trySyncing(ctx, clientId, e.Action)
	}

	return nil
}

// lookBack 对账逻辑; 处理新事件前, 先查该客户是否在补偿中, 是 则对账
func (s *RedisSyncer) lookBack(ctx context.Context, clientId string) error {
	subCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	count, err := s.mysqlRepo.GetOrderCount(subCtx, clientId)
	if err != nil {
		return err
	}
	if count > 0 {
		err = s.redisRepo.SAddActive(subCtx, clientId)
	} else {
		err = s.redisRepo.SRemActive(subCtx, clientId)
	}
	if err != nil {
		return err
	}

	s.fixes.Delete(clientId)
	return nil
}

// trySyncing 尝试同步, 失败则记录到 fixes, 再进行补偿
func (s *RedisSyncer) trySyncing(ctx context.Context, clientId string, action string) {
	var err error
	maxRetries := 3
	for i := 0; i < maxRetries; i++ {
		subCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancel()
		switch action {
		case canal.InsertAction:
			err = s.redisRepo.SAddActive(subCtx, clientId)
		case canal.UpdateAction:
			// 意外更新, 可能发生于手动改库
			// 直接调用 lookBack 逻辑
			err = s.lookBack(subCtx, clientId)
		case canal.DeleteAction:
			var count int
			count, err = s.mysqlRepo.GetOrderCount(subCtx, clientId)
			if err == nil && count == 0 {
				err = s.redisRepo.SRemActive(subCtx, clientId)
			}
		default: // cancel()
			log.Printf("[Sync] Unknown action: %s", action)
			return
		}

		if err == nil {
			return
		}
		log.Printf("[Sync] %d/%d Failed for client %s on action %s : %v", i+1, maxRetries, clientId, action, err)
		time.Sleep(time.Duration(i+1) * 80 * time.Millisecond)
	}

	s.fixes.Store(clientId, time.Now()) // 记入补偿队列 fixes, 值为当前时间戳
	log.Printf("[Sync] Warning! client %s added to 'fixes'", clientId)
}

// compensate 后台巡逻, 处理补偿队列 (fixes)
func (s *RedisSyncer) compensate() {
	tk := time.NewTicker(10 * time.Second) // 每 10 秒一次巡查
	for range tk.C {
		s.fixes.Range(func(key, value any) bool {
			clientId := key.(string)
			ctx := context.Background()
			log.Printf("[Sync] Trying to compensate client %s..", clientId)
			if err := s.lookBack(ctx, clientId); err != nil {
				log.Printf("[Sync] Failed to compensate client %s: %v", clientId, err)
			} else {
				log.Printf("[Sync] Client %s, compensation ok!", clientId)
			}
			return true
		})
	}
}
