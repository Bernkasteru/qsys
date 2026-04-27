package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"qsys/internal/config"
	"qsys/internal/db"
	"qsys/internal/model"
	"qsys/internal/mq"
	"time"

	"github.com/redis/go-redis/v9"
)

type expectedState struct {
	Orders map[string]map[model.OrderKey]bool // ClientId -> OrderKey -> Exist
}

const testTable = "orders_test"
const testRedisKey = "qsys:active_clients_test"

func newExpectedState() *expectedState {
	return &expectedState{
		Orders: make(map[string]map[model.OrderKey]bool),
	}
}

func (e *expectedState) apply(order *model.Order) {
	cid := order.ClientId
	okey := model.BuildOrderKey(cid, order.ExchangeType, order.StockCode)

	switch order.Action {
	case model.ActionCreate:
		if e.Orders[cid] == nil {
			e.Orders[cid] = make(map[model.OrderKey]bool)
		}
		e.Orders[cid][okey] = true
	case model.ActionDelete:
		if e.Orders[cid] != nil {
			delete(e.Orders[cid], okey)
			if len(e.Orders[cid]) == 0 {
				delete(e.Orders, cid)
			}
		}
	}
}

type StockInfo struct {
	Exchange string
	Code     string
}

func main() {
	confPath := flag.String("conf", "./deploy/config.yml", "Path to config file")
	opCount := flag.Int("n", 10000, "Number of random operations to simulate")
	flag.Parse()

	cfg, err := config.LoadConfig(*confPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	ctx := context.Background()
	mdb, err := db.NewMySQLConn(cfg.MySQL.DSN)
	if err != nil {
		log.Fatalf("Mysql connect err: %v", err)
	}
	defer mdb.Close()

	rdb, err := db.NewRedisConn(db.RedisConfig{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})
	if err != nil {
		log.Fatalf("Redis connect err: %v", err)
	}
	defer rdb.Close()

	producer := mq.New()
	defer producer.Close()

	log.Printf("Step 1: Cleaning up [%s] and Redis key [%s]\n", testTable, testRedisKey)
	if _, err := mdb.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE %s", testTable)); err != nil {
		log.Fatalf("Failed to truncate Mysql test table: %v", err)
	}
	if err := rdb.Del(ctx, testRedisKey).Err(); err != nil {
		log.Fatalf("Failed to clear Redis test clients: %v", err)
	}

	log.Printf("Step 2: Generating %d random orders\n", *opCount)
	state := newExpectedState()

	stockPool := []StockInfo{
		{Exchange: "1", Code: "600000"},
		{Exchange: "1", Code: "601318"},
		{Exchange: "1", Code: "600519"},
		{Exchange: "2", Code: "000001"},
		{Exchange: "2", Code: "000651"},
		{Exchange: "2", Code: "300750"},
	}
	actions := []string{"cre", "del"}

	for i := 0; i < *opCount; i++ {
		tar := stockPool[rand.Intn(len(stockPool))]
		baseID := int64(880000000000)
		randOffset := rand.Int63n(10000)

		order := model.New(
			fmt.Sprintf("%012d", baseID+randOffset),
			tar.Exchange,
			tar.Code,
			actions[rand.Intn(len(actions))],
		)

		state.apply(order)
		if err := producer.SendOrder(ctx, order); err != nil {
			log.Printf("Failed to send order: %v", err)
		}
	}
	log.Printf("Finished sending %d orders to Kafka.", *opCount)
	log.Printf("Expected Active Clients Count: %d\n", len(state.Orders))

	log.Println("Step 3: Waiting 10 seconds for Consumers to process messages")
	time.Sleep(10 * time.Second)

	log.Println("Step 4: Asserting DB and Redis data consistency..")
	verifyData(ctx, mdb, rdb, state, testTable, testRedisKey)
}

func verifyData(ctx context.Context, mdb *sql.DB, rdb *redis.Client, expected *expectedState, table, redisKey string) {
	// 校验 Redis
	actualRedisClients, err := rdb.SMembers(ctx, redisKey).Result()
	if err != nil {
		log.Fatalf("Failed to read Redis: %v", err)
	}

	actualRedisMap := make(map[string]bool)
	for _, c := range actualRedisClients {
		actualRedisMap[c] = true
	}

	redisErrors := 0
	for expClient := range expected.Orders {
		if !actualRedisMap[expClient] {
			log.Printf("!! Expected client %s to be active", expClient)
			redisErrors++
		}
	}
	for actClient := range actualRedisMap {
		if _, ok := expected.Orders[actClient]; !ok {
			log.Printf("!! Found ghost client %s in Redis", actClient)
			redisErrors++
		}
	}

	// 校验 Mysql
	query := fmt.Sprintf("SELECT client_id, exchange_type, stock_code FROM %s", table)
	rows, err := mdb.QueryContext(ctx, query)
	if err != nil {
		log.Fatalf("Failed to query Mysql test table: %v", err)
	}
	defer rows.Close()

	actualDbOrders := make(map[string]map[model.OrderKey]bool)
	totalDbRows := 0
	for rows.Next() {
		var cid, ex, code string
		if err := rows.Scan(&cid, &ex, &code); err != nil {
			log.Fatalf("Row scan error: %v", err)
		}
		if actualDbOrders[cid] == nil {
			actualDbOrders[cid] = make(map[model.OrderKey]bool)
		}
		actualDbOrders[cid][model.BuildOrderKey(cid, ex, code)] = true
		totalDbRows++
	}

	dbErrors := 0
	totalExpRows := 0
	for cid, expMap := range expected.Orders {
		for okey := range expMap {
			totalExpRows++
			if actualDbOrders[cid] == nil || !actualDbOrders[cid][okey] {
				log.Printf("!! Expected order not found - Client: %s, Ex: %c, Stock: %s", cid, okey.ExType, string(okey.Stock[:]))
				dbErrors++
			}
		}
	}

	for cid, actMap := range actualDbOrders {
		for okey := range actMap {
			if expected.Orders[cid] == nil || !expected.Orders[cid][okey] {
				log.Printf("!! Unexpected order found - Client: %s, Ex: %c, Stock: %s", cid, okey.ExType, string(okey.Stock[:]))
				dbErrors++
			}
		}
	}

	fmt.Println("\n================  TEST REPORT  ================")
	fmt.Printf("Expected DB Orders:   %d\n", totalExpRows)
	fmt.Printf("Actual DB Orders:     %d\n", totalDbRows)
	fmt.Printf("Expected Redis Users: %d\n", len(expected.Orders))
	fmt.Printf("Actual Redis Users:   %d\n\n", len(actualRedisMap))

	if redisErrors == 0 && dbErrors == 0 {
		fmt.Println("OK")
	} else {
		fmt.Printf("Failed: Redis %d; Mysql %d\n", redisErrors, dbErrors)
	}
}
