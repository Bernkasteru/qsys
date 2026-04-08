package app

import (
	"context"
	"fmt"
	"qsys/internal/db"
	"qsys/internal/model"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

type UpdSvr struct {
	mysql *db.OrderRepo
}

func NewUpdSvr(m *db.OrderRepo) *UpdSvr {
	return &UpdSvr{mysql: m}
}

func (s *UpdSvr) Handle(ctx context.Context, msg kafka.Message) error {
	var od model.Order
	// 还原 proto
	if err := proto.Unmarshal(msg.Value, &od); err != nil {
		return fmt.Errorf("Unmarshal protobuf failed: %w", err)
	}
	return s.mysql.UpdateOrder(ctx, od.Action, od.ClientId, od.ExchangeType, od.StockCode)
}
