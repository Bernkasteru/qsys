package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"qsys/internal/config"
	"qsys/internal/model"
	"qsys/internal/mq"
	"syscall"
	"time"
)

type StockInfo struct {
	Exchange string
	Code     string
}

func main() {
	_, err := config.LoadConfig("./deploy/config.yml")
	if err != nil {
		fmt.Printf("Failed to load config.yml: %v\n", err)
		return
	}

	producer := mq.New()
	defer producer.Close()
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		fmt.Println("向 Kafka 投递 Orders..")
		fmt.Println()
		for {
			time.Sleep(5 * time.Second)
			att, succ, fail := producer.Attempt.Load(), producer.Succ.Load(), producer.Fail.Load()
			qLen := producer.GetQueueLength()
			fmt.Printf("Attempt: %d | Success: %d | Fail: %d | Queue Length | %d\n", att, succ, fail, qLen)
		}
	}()

	tps := 200
	tk := time.NewTicker(time.Second / time.Duration(tps))
	defer tk.Stop()

	ctx := context.Background()
	stockPool := []StockInfo{
		{Exchange: "1", Code: "600000"},
		{Exchange: "1", Code: "601318"},
		{Exchange: "1", Code: "600519"},
		{Exchange: "2", Code: "000001"},
		{Exchange: "2", Code: "000651"},
		{Exchange: "2", Code: "300750"},
	}
	actions := []string{"cre", "del"}
	for {
		select {
		case <-sigChan:
			return
		case <-tk.C:
			tar := stockPool[rand.Intn(len(stockPool))]
			baseID := int64(880000000000)
			randOffset := rand.Int63n(1000)
			order := &model.Order{
				ClientId:     fmt.Sprintf("%012d", baseID+randOffset),
				ExchangeType: tar.Exchange,
				StockCode:    tar.Code,
				Action:       actions[rand.Intn(len(actions))],
				Timestamp:    time.Now().UnixMilli(),
			}
			if err := producer.SendOrder(ctx, order); err != nil {
				fmt.Printf("[Qsim] %v\n", err)
				continue
			}
		}
	}
}
