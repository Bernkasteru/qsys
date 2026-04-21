package app

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"qsys/internal/db"
	"qsys/internal/model"
	"slices"
	"time"
	"unsafe"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

type UpdSvr struct {
	mysql *db.OrderRepo
	redis *db.RedisRepo
}

type setVal struct{}

const maxRetries = 3

func NewUpdSvr(m *db.OrderRepo, r *db.RedisRepo) *UpdSvr {
	return &UpdSvr{mysql: m, redis: r}
}

func cmpOrderKey(a, b model.OrderKey) int {
	pA := (*[3]uint64)(unsafe.Pointer(&a))
	pB := (*[3]uint64)(unsafe.Pointer(&b))

	for i := range 3 {
		if pA[i] != pB[i] {
			if pA[i] < pB[i] {
				return -1
			}
			return 1
		}
	}
	return 0
}

func cmpOrderKeySafe(a, b model.OrderKey) int {
	// 1. 比较 Client
	if a.Client != b.Client {
		for i := range len(a.Client) {
			if a.Client[i] != b.Client[i] {
				if a.Client[i] < b.Client[i] {
					return -1
				}
				return 1
			}
		}
	}

	// 2. 比较 ExType
	if a.ExType != b.ExType {
		if a.ExType < b.ExType {
			return -1
		}
		return 1
	}

	// 3. 比较 Stock
	if a.Stock != b.Stock {
		for i := range len(a.Stock) {
			if a.Stock[i] != b.Stock[i] {
				if a.Stock[i] < b.Stock[i] {
					return -1
				}
				return 1
			}
		}
	}

	return 0
}

func (s *UpdSvr) Handle(ctx context.Context, msg kafka.Message) error {
	var od model.Order
	// 还原 proto
	if err := proto.Unmarshal(msg.Value, &od); err != nil {
		return fmt.Errorf("Unmarshal failed: %w", err)
	}
	return s.mysql.UpdateOrder(ctx, od.Action, od.ClientId, od.ExchangeType, od.StockCode)
}

// HandleBatch 处理折叠后的批量请求
func (s *UpdSvr) HandleBatch(ctx context.Context, fMap map[model.OrderKey]byte) error {
	if len(fMap) == 0 {
		return nil
	}

	// 分离 cre/del
	cres, dels := make([]model.OrderKey, 0, len(fMap)), make([]model.OrderKey, 0, len(fMap))
	for k, action := range fMap {
		switch action {
		case 'c':
			cres = append(cres, k)
		case 'd':
			dels = append(dels, k)
		}
	}

	// 排序, 防交叉死锁
	if len(cres) > 1 {
		slices.SortFunc(cres, cmpOrderKey)
	}

	if len(dels) > 1 {
		slices.SortFunc(dels, cmpOrderKey)
	}

	execWithRetry := func(op string, do func() error) error {
		var err error
		for i := range maxRetries {
			err = do()
			if err == nil {
				return nil
			}
			if i < maxRetries-1 {
				time.Sleep(5*time.Millisecond + time.Duration(rand.Intn(20))*time.Millisecond)
			}
		}
		return fmt.Errorf("%s failed after %d retries: %w", op, maxRetries, err)
	}

	// 依次批量 sql
	if len(cres) > 0 {
		if err := execWithRetry("BatchCreateOrders", func() error { return s.mysql.BatchCreateOrders(ctx, cres) }); err != nil {
			return err
		}
	}
	if len(dels) > 0 {
		if err := execWithRetry("BatchDeleteOrders", func() error { return s.mysql.BatchDeleteOrders(ctx, dels) }); err != nil {
			return err
		}
	}

	// 维护 Redis 的 active clients
	pipe := s.redis.Pipeline()
	for _, k := range cres {
		pipe.SAdd(ctx, db.Ac, string(k.Client[:]))
	}
	if len(dels) > 0 { // 处理删除
		// 获取本次被删过订单的 client set
		dCliMap := make(map[string]setVal)
		for _, k := range dels {
			dCliMap[string(k.Client[:])] = setVal{}
		}
		// 剪枝, 如果该 client 在本批次中有其它 cre
		// 直接标记为 active
		for _, k := range cres {
			cid := string(k.Client[:])
			delete(dCliMap, cid)
		}
		cliSet := make([]string, 0, len(dCliMap))
		for i := range dCliMap {
			cliSet = append(cliSet, i)
		}

		// 向 mysql 查找是否仍然活跃
		if len(cliSet) > 0 {
			acClis, err := s.mysql.GetActiveClients(ctx, cliSet)
			if err == nil {
				acMap := make(map[string]setVal, len(acClis))
				for _, cid := range acClis {
					acMap[cid] = setVal{}
				}
				// 判断从 redis 移除
				for _, cid := range cliSet {
					if _, isActive := acMap[cid]; !isActive {
						pipe.SRem(ctx, db.Ac, cid)
					}
				}
			} else {
				log.Printf("[Upd_svr] Warning! %v", err)
			}
		}
	}

	if _, err := pipe.Exec(ctx); err != nil {
		log.Printf("[Upd_svr] Warning! Mysql succ but redis sync failed: %v", err)
	}
	return nil
}
