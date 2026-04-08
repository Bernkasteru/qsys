package model

import (
	"fmt"
	"regexp"
	"time"
)

const (
	ActionCreate = "cre"
	ActionDelete = "del"
	ExchangeSH   = "1" // 沪市
	ExchangeSZ   = "2" // 深市
)

var (
	clientIdRegex  = regexp.MustCompile(`^\d{12}$`)
	stockCodeRegex = regexp.MustCompile(`^\d{6}$`)
)

/*
type Order struct {
	// Json tags
	ClientId     string `json:"client_id"`     // 客户号 12位
	ExchangeType string `json:"exchange_type"` // 市场:1-沪, 2-深
	StockCode    string `json:"stock_code"`    // 6位股票代码
	Action       string `json:"action"`        // 操作: cre-创建, del-删除
	Timestamp    int64  `json:"timestamp"`
}
*/

func New(clientId, exchangeType, stockCode, action string) *Order {
	return &Order{
		ClientId:     clientId,
		ExchangeType: exchangeType,
		StockCode:    stockCode,
		Action:       action,
		Timestamp:    time.Now().UnixMilli(),
	}
}

// Validate 检查字段是否合法
func (o *Order) Validate() error {
	if !clientIdRegex.MatchString(o.ClientId) {
		return fmt.Errorf("Invalid client_id: '%s'", o.ClientId)
	}
	if o.ExchangeType != ExchangeSH && o.ExchangeType != ExchangeSZ {
		return fmt.Errorf("Invalid exchange_type: '%s'; must be '1' or '2'", o.ExchangeType)
	}
	if !stockCodeRegex.MatchString(o.StockCode) {
		return fmt.Errorf("Invalid stock_code: '%s'", o.StockCode)
	}
	if o.Action != ActionCreate && o.Action != ActionDelete {
		return fmt.Errorf("Invalid action: '%s'; must be 'cre' or 'del'", o.Action)
	}
	return nil
}

func (o *Order) ShortString() string {
	return fmt.Sprintf("Order {ClientId: %s | ExchangeType: %s | StockCode: %s | Action: %s | Timestamp: %d}",
		o.ClientId, o.ExchangeType, o.StockCode, o.Action, o.Timestamp)
}
