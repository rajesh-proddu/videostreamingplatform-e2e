// Package config provides environment-based configuration for e2e tests.
package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds all service URLs and test parameters.
type Config struct {
	MetadataServiceURL string
	DataServiceURL     string
	DataServiceGRPC    string
	KafkaBrokers       string
	RedisAddr          string

	// Timeouts
	HTTPTimeout    time.Duration
	UploadTimeout  time.Duration
	EventWaitTime  time.Duration

	// Scale test parameters
	BulkCount       int
	ConcurrentUsers int
}

func Load() *Config {
	return &Config{
		MetadataServiceURL: envOr("METADATA_SERVICE_URL", "http://localhost:8080"),
		DataServiceURL:     envOr("DATA_SERVICE_URL", "http://localhost:8081"),
		DataServiceGRPC:    envOr("DATA_SERVICE_GRPC", "localhost:50051"),
		KafkaBrokers:       envOr("KAFKA_BROKERS", "localhost:9092"),
		RedisAddr:          envOr("REDIS_ADDR", "localhost:6379"),
		HTTPTimeout:        durationOr("HTTP_TIMEOUT", 30*time.Second),
		UploadTimeout:      durationOr("UPLOAD_TIMEOUT", 120*time.Second),
		EventWaitTime:      durationOr("EVENT_WAIT_TIME", 5*time.Second),
		BulkCount:          intOr("BULK_COUNT", 50),
		ConcurrentUsers:    intOr("CONCURRENT_USERS", 10),
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func intOr(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func durationOr(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}
