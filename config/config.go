// Package config provides environment-based configuration for e2e tests.
package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	MetadataServiceURL string
	DataServiceURL     string
	DataServiceGRPC    string
	KafkaBrokers       string
	RedisAddr          string

	ElasticsearchURL         string
	ESVideoIndex             string
	RecommendationServiceURL string
	PgVectorDSN              string
	OllamaBaseURL            string

	S3Endpoint             string
	S3Region               string
	S3AccessKey            string
	S3SecretKey            string
	IcebergWarehouseBucket string
	IcebergTablePrefix     string

	CDNProxyURL              string
	CloudFrontDistributionID string

	HTTPTimeout       time.Duration
	UploadTimeout     time.Duration
	EventWaitTime     time.Duration
	AnalyticsWaitTime time.Duration

	BulkCount       int
	ConcurrentUsers int

	// MySQL DSN for direct DB scale probes (default points at local docker mysql).
	MySQLDSN string

	// Scale-test tunables (read by tests under tests/scale/metadata_db).
	ScaleDuration time.Duration
	ScaleWorkers  int
	ScaleCorpus   int
}

func Load() *Config {
	return &Config{
		MetadataServiceURL: envOr("METADATA_SERVICE_URL", "http://127.0.0.1:8080"),
		DataServiceURL:     envOr("DATA_SERVICE_URL", "http://127.0.0.1:8081"),
		DataServiceGRPC:    envOr("DATA_SERVICE_GRPC", "127.0.0.1:50051"),
		KafkaBrokers:       envOr("KAFKA_BROKERS", "127.0.0.1:9092"),
		RedisAddr:          envOr("REDIS_ADDR", "127.0.0.1:6379"),

		ElasticsearchURL:         envOr("ELASTICSEARCH_URL", "http://127.0.0.1:9200"),
		ESVideoIndex:             envOr("ES_VIDEO_INDEX", "videos"),
		RecommendationServiceURL: envOr("RECOMMENDATION_SERVICE_URL", "http://127.0.0.1:8000"),
		PgVectorDSN:              envOr("PGVECTOR_DSN", "postgres://recouser:recopass@127.0.0.1:5432/recommendations?sslmode=disable"),
		OllamaBaseURL:            envOr("OLLAMA_BASE_URL", "http://127.0.0.1:11434"),

		S3Endpoint:             envOr("S3_ENDPOINT", "http://127.0.0.1:4566"),
		S3Region:               envOr("AWS_REGION", "us-east-1"),
		S3AccessKey:            envOr("AWS_ACCESS_KEY_ID", "minioadmin"),
		S3SecretKey:            envOr("AWS_SECRET_ACCESS_KEY", "minioadmin"),
		IcebergWarehouseBucket: envOr("ICEBERG_WAREHOUSE_BUCKET", "iceberg-warehouse"),
		IcebergTablePrefix:     envOr("ICEBERG_TABLE_PREFIX", "analytics.db/watch_history/data"),

		CDNProxyURL:              envOr("CDN_PROXY_URL", "http://127.0.0.1:8090"),
		CloudFrontDistributionID: envOr("CLOUDFRONT_DISTRIBUTION_ID", ""),

		HTTPTimeout:       durationOr("HTTP_TIMEOUT", 30*time.Second),
		UploadTimeout:     durationOr("UPLOAD_TIMEOUT", 120*time.Second),
		EventWaitTime:     durationOr("EVENT_WAIT_TIME", 5*time.Second),
		AnalyticsWaitTime: durationOr("ANALYTICS_WAIT_TIME", 30*time.Second),

		BulkCount:       intOr("BULK_COUNT", 50),
		ConcurrentUsers: intOr("CONCURRENT_USERS", 10),

		MySQLDSN: envOr("MYSQL_DSN", "videouser:videopass@tcp(127.0.0.1:3306)/videoplatform?parseTime=true"),

		ScaleDuration: durationOr("SCALE_DURATION", 60*time.Second),
		ScaleWorkers:  intOr("SCALE_WORKERS", 16),
		ScaleCorpus:   intOr("SCALE_CORPUS", 1_000_000),
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
