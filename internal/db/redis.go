package db

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisRepo struct {
	rdb *redis.Client
}

type RedisConfig struct {
	Addr     string
	Password string
	DB       int
	PoolSize int
}

const Ac = "qsys:active_clients"

const UnlockScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
`

const RenewScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("EXPIRE", KEYS[1], ARGV[2])
else
    return 0
end
`

const (
	GetPosScript = `return redis.call('GET', KEYS[1])`
	SetPosScript = `return redis.call('SET', KEYS[1], ARGV[1])`
)

func NewRedisRepo(cfg RedisConfig) *RedisRepo {
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

	return &RedisRepo{rdb: rdb}
}

func (r *RedisRepo) SAddActive(ctx context.Context, clientId string) error {
	if clientId == "" {
		return fmt.Errorf("Empty clientId")
	}
	err := r.rdb.SAdd(ctx, Ac, clientId).Err()
	if err != nil {
		return fmt.Errorf("[Redis] SAdd failed for client %s: %w", clientId, err)
	}
	return nil
}

func (r *RedisRepo) SRemActive(ctx context.Context, clientId string) error {
	if clientId == "" {
		return fmt.Errorf("Empty clientId")
	}
	err := r.rdb.SRem(ctx, Ac, clientId).Err()
	if err != nil {
		return fmt.Errorf("[Redis] SRem failed for client %s: %w", clientId, err)
	}
	return nil
}

func (r *RedisRepo) SIsMember(ctx context.Context, clientId string) (bool, error) {
	return r.rdb.SIsMember(ctx, Ac, clientId).Result()
}

// SetNX 尝试抢锁, 返回是否成功
func (r *RedisRepo) SetNX(ctx context.Context, key, val string, ttl time.Duration) (bool, error) {
	err := r.rdb.SetArgs(ctx, key, val, redis.SetArgs{
		Mode: "NX",
		TTL:  ttl,
	}).Err()

	if err == redis.Nil {
		return false, nil // 锁已占用
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (r *RedisRepo) Eval(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd {
	return r.rdb.Eval(ctx, script, keys, args...)
}

func (r *RedisRepo) Del(ctx context.Context, key string) error {
	return r.rdb.Del(ctx, key).Err()
}

func (r *RedisRepo) Pipeline() redis.Pipeliner {
	return r.rdb.Pipeline()
}

func (r *RedisRepo) Close() error {
	if r.rdb == nil {
		return nil
	}

	err := r.rdb.Close()
	if err != nil {
		return fmt.Errorf("[Redis] Failed to close connection pool: %w", err)
	}

	return nil
}
