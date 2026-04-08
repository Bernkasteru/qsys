package main

import (
	"context"
	"fmt"
	"log"
	"qsys/internal/model"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

func main() {
	brks := []string{"localhost:9092", "localhost:9094", "localhost:9096"}
	topic := "conorder"
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brks,
		Topic:    topic,
		GroupID:  "qsys_decoder1",
		MinBytes: 100,
		MaxBytes: 10e6,
	})
	defer reader.Close()
	fmt.Println("解码并打印二进制消息: ")
	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatalf("Failed to read: %v", err)
		}
		var od model.Order
		if err := proto.Unmarshal(m.Value, &od); err != nil {
			fmt.Printf("Failed to decode: %v\n", err)
			continue
		}
		fmt.Printf("· %s\n", od.String())
	}
}
