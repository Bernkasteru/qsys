package db

import (
	"context"
	"database/sql"
	"fmt"
	"qsys/internal/model"
	"strings"
	"sync"
	"time"
	"unsafe"

	_ "github.com/go-sql-driver/mysql"
)

type OrderRepo struct {
	db *sql.DB
}

var (
	SqlInsert      = "INSERT IGNORE INTO orders (client_id, exchange_type, stock_code) VALUES (?, ?, ?)"
	SqlDelete      = "DELETE FROM orders WHERE client_id = ? AND exchange_type = ? AND stock_code = ?"
	SqlBatchInsert = "INSERT IGNORE INTO orders (client_id, exchange_type, stock_code) VALUES "
	SqlBatchDelete = "DELETE FROM orders WHERE (client_id, exchange_type, stock_code) IN ("
	SqlGetActive   = "SELECT DISTINCT client_id FROM orders WHERE client_id IN ("
)

// slcPool 负责缓存 []any
var slcPool = sync.Pool{
	New: func() any {
		s := make([]any, 0, 320)
		return &s
	},
}

// sbPool 负责缓存 strings.Builder
var sbPool = sync.Pool{
	New: func() any {
		sb := new(strings.Builder)
		sb.Grow(5120)
		return sb
	},
}

func NewOrderRepo(dsn string) (*OrderRepo, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	// 连接池配置
	db.SetMaxOpenConns(150)
	db.SetMaxIdleConns(140)
	db.SetConnMaxLifetime(1 * time.Hour)
	db.SetConnMaxIdleTime(30 * time.Minute)

	// 测试连接
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return &OrderRepo{db: db}, nil
}

// UpdateOrder 处理 MySQL 逻辑, 仅负责落库, 单条 Sql 语句是原子的
func (r *OrderRepo) UpdateOrder(ctx context.Context, action string, clientId, exType, sCode string) error {
	var err error
	switch action {
	case "cre":
		_, err = r.db.ExecContext(ctx, SqlInsert, clientId, exType, sCode)
	case "del":
		_, err = r.db.ExecContext(ctx, SqlDelete, clientId, exType, sCode)
	default:
		return fmt.Errorf("[Mysql] Unknown action: %s", action)
		// return fmt.Errorf("Unknown action: %s", action)
	}
	return err
}

// BatchCreateOrders 批量插入
func (r *OrderRepo) BatchCreateOrders(ctx context.Context, cres []model.OrderKey) error {
	if len(cres) == 0 {
		return nil
	}

	sb := sbPool.Get().(*strings.Builder)
	sb.Reset() // 清除上轮残留
	defer sbPool.Put(sb)
	pVals := slcPool.Get().(*[]any)
	vals := (*pVals)[:0] // 长度归零
	defer slcPool.Put(pVals)

	sb.WriteString(SqlBatchInsert)

	for i := range cres {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString("(?, ?, ?)")
		// vals = append(vals, string(k.Client[:]), string([]byte{k.ExType}), string(k.Stock[:]))
		vals = append(vals,
			unsafe.String(&cres[i].Client[0], 12),
			unsafe.String(&cres[i].ExType, 1),
			unsafe.String(&cres[i].Stock[0], 6),
		)
	}

	_, err := r.db.ExecContext(ctx, sb.String(), vals...)
	return err
}

// BatchDeleteOrders 批量删除
func (r *OrderRepo) BatchDeleteOrders(ctx context.Context, dels []model.OrderKey) error {
	if len(dels) == 0 {
		return nil
	}

	sb := sbPool.Get().(*strings.Builder)
	sb.Reset()
	defer sbPool.Put(sb)
	pVals := slcPool.Get().(*[]any)
	vals := (*pVals)[:0]
	defer slcPool.Put(pVals)

	sb.WriteString(SqlBatchDelete)

	for i := range dels {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString("(?, ?, ?)")
		// vals = append(vals, string(k.Client[:]), string([]byte{k.ExType}), string(k.Stock[:]))
		vals = append(vals,
			unsafe.String(&dels[i].Client[0], 12),
			unsafe.String(&dels[i].ExType, 1),
			unsafe.String(&dels[i].Stock[0], 6),
		)
	}
	sb.WriteString(")")

	_, err := r.db.ExecContext(ctx, sb.String(), vals...)
	return err
}

// GetActiveClients 传入一批 client_id, 反查 mysql 中活跃/存在的
func (r *OrderRepo) GetActiveClients(ctx context.Context, clientIds []string) ([]string, error) {
	n := len(clientIds)
	if n == 0 {
		return nil, nil
	}
	// 预分配容量, 避免扩容
	args := make([]any, n)
	var sb strings.Builder
	sb.Grow(58 + n*2)
	sb.WriteString(SqlGetActive)

	for i, cid := range clientIds {
		args[i] = cid
		if i > 0 {
			sb.WriteString(",?")
		} else {
			sb.WriteString("?")
		}
	}
	sb.WriteString(")")

	// 执行查询
	rows, err := r.db.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("[Mysql] Gac failed for %d clients: %w", n, err)
	}
	defer rows.Close()

	rst := make([]string, 0, n)
	for rows.Next() {
		var cid string
		if err := rows.Scan(&cid); err != nil {
			return nil, fmt.Errorf("[Mysql] Gac scan failed: %w", err)
		}
		rst = append(rst, cid)
	}
	return rst, rows.Err()
}

func (r *OrderRepo) GetOrderCount(ctx context.Context, clientId string) (int, error) {
	var count int
	err := r.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM orders WHERE client_id = ?",
		clientId).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("[Mysql] GetOrderCount failed for client %s: %w", clientId, err)
		// return 0, fmt.Errorf("GetOrderCount failed for client %s: %w", clientId, err)
	}
	return count, nil
}

func (r *OrderRepo) GetOrders(ctx context.Context, clientId string) ([]*model.OrderInfo, error) {
	query := `SELECT exchange_type, stock_code FROM orders WHERE client_id = ?`
	rows, err := r.db.QueryContext(ctx, query, clientId)
	if err != nil {
		return nil, fmt.Errorf("[Mysql] GetOrders failed for client %s: %w", clientId, err)
	}
	defer rows.Close()

	var items []*model.OrderInfo
	for rows.Next() {
		var exType, sCode string
		if err := rows.Scan(&exType, &sCode); err != nil {
			continue
		}
		items = append(items, &model.OrderInfo{
			ExchangeType: exType,
			StockCode:    sCode,
		})
	}
	return items, nil
}

func (r *OrderRepo) Close() error {
	if r.db == nil {
		return nil
	}

	err := r.db.Close()
	if err != nil {
		return fmt.Errorf("[Mysql] Failed to close connection pool: %w", err)
		// return fmt.Errorf("Failed to close connection pool: %w", err)
	}

	return nil
}
