package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type OrderRepo struct {
	db *sql.DB
}

func NewOrderRepo(dsn string) (*OrderRepo, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	// 连接池配置
	db.SetMaxOpenConns(200)
	db.SetMaxIdleConns(20)
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
		_, err = r.db.ExecContext(ctx,
			"INSERT IGNORE INTO orders (client_id, exchange_type, stock_code) VALUES (?, ?, ?)",
			clientId, exType, sCode)
	case "del":
		_, err = r.db.ExecContext(ctx,
			"DELETE FROM orders WHERE client_id = ? AND exchange_type = ? AND stock_code = ?",
			clientId, exType, sCode)
	default:
		return fmt.Errorf("[Mysql] Unknown action: %s", action)
		// return fmt.Errorf("Unknown action: %s", action)
	}
	return err
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
