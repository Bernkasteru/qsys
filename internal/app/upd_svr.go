package app

import (
	"context"
	"fmt"
	"math/rand"
	"qsys/internal/db"
	"qsys/internal/model"
	"sort"
	"time"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

type UpdSvr struct {
	mysql *db.OrderRepo
}

const maxRetries = 3

func NewUpdSvr(m *db.OrderRepo) *UpdSvr {
	return &UpdSvr{mysql: m}
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

	// 排序, 防死锁交叉
	if len(cres) > 1 {
		sort.Slice(cres, func(i, j int) bool {
			if cres[i].Client != cres[j].Client {
				return string(cres[i].Client[:]) < string(cres[j].Client[:])
			}
			if cres[i].ExType != cres[j].ExType {
				return cres[i].ExType < cres[j].ExType
			}
			return string(cres[i].Stock[:]) < string(cres[j].Stock[:])
		})
	}

	if len(dels) > 1 {
		sort.Slice(dels, func(i, j int) bool {
			if dels[i].Client != dels[j].Client {
				return string(dels[i].Client[:]) < string(dels[j].Client[:])
			}
			if dels[i].ExType != dels[j].ExType {
				return dels[i].ExType < dels[j].ExType
			}
			return string(dels[i].Stock[:]) < string(dels[j].Stock[:])
		})
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

	return nil
}
