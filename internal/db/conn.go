package db

import (
	"context"
	"database/sql"
	"time"

	"github.com/redis/go-redis/v9"
)

func NewMySQLConn(dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(200)
	db.SetMaxIdleConns(20)
	db.SetConnMaxLifetime(1 * time.Hour)
	db.SetConnMaxIdleTime(30 * time.Minute)

	return db, db.Ping()
}

func NewRedisConn(cfg RedisConfig) (*redis.Client, error) {
	if cfg.PoolSize == 0 {
		cfg.PoolSize = 16
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     cfg.PoolSize,
		ReadTimeout:  300 * time.Millisecond,
		WriteTimeout: 300 * time.Millisecond,
		PoolTimeout:  800 * time.Millisecond,
		MaxIdleConns: 10,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		panic("Redis disconnect: " + err.Error())
	}

	return rdb, nil
}
