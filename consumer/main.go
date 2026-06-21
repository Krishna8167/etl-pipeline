package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/IBM/sarama"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type RawLog struct {
	Service   string `json:"service"`
	Level     string `json:"level"`
	Message   string `json:"message"`
	Timestamp string `json:"timestamp"`
}

// Prometheus counters (will be scraped later)
var (
	logsProcessed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "logs_processed_total",
		Help: "Total logs successfully processed",
	})
	logsFailed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "logs_failed_total",
		Help: "Total logs that failed processing",
	})
)

func init() {
	prometheus.MustRegister(logsProcessed)
	prometheus.MustRegister(logsFailed)
}

func main() {
	// Start metrics server (prepares for Phase 3)
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
		log.Println("📊 Metrics endpoint ready on :8080/metrics")
		if err := http.ListenAndServe(":8080", nil); err != nil {
			log.Printf("Metrics server error: %v", err)
		}
	}()

	// Connect to PostgreSQL
	pgConn := os.Getenv("PG_CONN")
	if pgConn == "" {
		pgConn = "postgres://postgres:postgres@postgres:5432/etl?sslmode=disable"
	}
	conn, err := pgx.Connect(context.Background(), pgConn)
	if err != nil {
		log.Fatalf("❌ Cannot connect to Postgres: %v", err)
	}
	defer conn.Close(context.Background())
	log.Println("✅ Connected to PostgreSQL")

	// Kafka consumer config
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Return.Errors = true

	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "kafka:9092"
	}
	group := "etl-consumer-group"

	consumer, err := sarama.NewConsumerGroup([]string{broker}, group, config)
	if err != nil {
		log.Fatalf("❌ Failed to create consumer group: %v", err)
	}
	defer consumer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := &ConsumerHandler{conn: conn}

	go func() {
		for {
			if err := consumer.Consume(ctx, []string{"logs"}, handler); err != nil {
				log.Printf("⚠️ Consumer error: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	log.Println("✅ Consumer started. Waiting for messages...")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm
	log.Println("🛑 Shutting down...")
	cancel()
}

// ---- Consumer Group Handler ----

type ConsumerHandler struct {
	conn *pgx.Conn
}

func (h *ConsumerHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *ConsumerHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *ConsumerHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var raw RawLog

		// 1. EXTRACT & VALIDATE (JSON parse)
		err := json.Unmarshal(msg.Value, &raw)
		if err != nil {
			logsFailed.Inc()
			log.Printf("❌ Invalid JSON: %s", string(msg.Value))
			sess.MarkMessage(msg, "")
			continue
		}

		// 2. TRANSFORM
		raw.Level = strings.ToUpper(raw.Level) // e.g., "error" → "ERROR"

		// 3. LOAD into PostgreSQL
		query := `INSERT INTO logs (service, level, message, timestamp) VALUES ($1, $2, $3, $4)`
		_, err = h.conn.Exec(context.Background(), query, raw.Service, raw.Level, raw.Message, raw.Timestamp)
		if err != nil {
			logsFailed.Inc()
			log.Printf("❌ DB insert failed: %v", err)
		} else {
			logsProcessed.Inc()
			log.Printf("✅ Inserted: [%s] %s → %s", raw.Service, raw.Level, raw.Message)
		}

		sess.MarkMessage(msg, "")
	}
	return nil
}