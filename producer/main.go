package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

type LogEntry struct {
	Service   string `json:"service"`
	Level     string `json:"level"`
	Message   string `json:"message"`
	Timestamp string `json:"timestamp"`
}

var (
	services = []string{"auth", "payment", "inventory", "notification"}
	levels   = []string{"INFO", "WARN", "ERROR", "DEBUG"}
	messages = map[string][]string{
		"auth":         {"user login", "user logout", "login failed", "password reset", "token refresh"},
		"payment":      {"payment processed", "payment failed", "refund issued", "database timeout", "insufficient funds"},
		"inventory":    {"stock updated", "low stock threshold", "item restocked", "inventory check", "warehouse sync"},
		"notification": {"email sent", "sms delivered", "push notification", "webhook called", "notification failed"},
	}
)

func main() {
	rand.New(rand.NewSource(time.Now().UnixNano()))

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewRandomPartitioner

	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "kafka:9092"
	}

	producer, err := sarama.NewSyncProducer([]string{broker}, config)
	if err != nil {
		log.Fatalf(" Failed to connect to Kafka: %v", err)
	}
	defer producer.Close()

	log.Printf(" Producer connected to %s. Sending 1 realistic log/sec...", broker)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Shutting down producer.")
			return
		case t := <-ticker.C:
			service := services[rand.Intn(len(services))]
			level := levels[rand.Intn(len(levels))]
			msgList := messages[service]
			message := msgList[rand.Intn(len(msgList))]

			entry := LogEntry{
				Service:   service,
				Level:     level,
				Message:   message,
				Timestamp: t.Format(time.RFC3339),
			}

			value, _ := json.Marshal(entry)
			msg := &sarama.ProducerMessage{
				Topic: "logs",
				Value: sarama.ByteEncoder(value),
			}

			partition, offset, err := producer.SendMessage(msg)
			if err != nil {
				log.Printf(" Send error: %v", err)
			} else {
				log.Printf(" [%s] %s → %s (p:%d o:%d)", service, level, message, partition, offset)
			}
		}
	}
}