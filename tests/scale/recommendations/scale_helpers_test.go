// Package recommendations_scale contains scale tests for the recommendations
// service end-to-end (FastAPI + LangGraph), the underlying Ollama LLM, the
// Elasticsearch retrieve path, and the Iceberg parquet read path that feeds
// the offline analytics view.
//
// All tests in this package log metrics via t.Logf in a tabular form so
// downstream parsers can grep the same row across runs:
//
//	[recommend] p50=120ms p95=450ms p99=820ms qps=15.3 n=100
//
// Knobs (all optional):
//
//	SCALE_RECO_USERS       — concurrent users for E2E_Concurrent       (default 8)
//	SCALE_RECO_DURATION    — wall time for sustained-load tests        (default 60s)
//	SCALE_ES_DOCS          — synthetic ES corpus size                  (default 50000)
//	SCALE_ICEBERG_EVENTS   — watch events to produce for Iceberg test  (default 1000)
//
// All tests skip cleanly when the dependency is unreachable.
package recommendations_scale

import (
	"math"
	"os"
	"sort"
	"strconv"
	"time"
)

// envIntOr parses a positive int from env or returns fallback.
func envIntOr(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

func envDurationOr(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return fallback
}

// percentile returns the p-th percentile (0–100) of samples using
// nearest-rank. samples may be mutated (sorted in place).
func percentile(samples []time.Duration, p float64) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	rank := int(math.Ceil(p/100*float64(len(samples)))) - 1
	if rank < 0 {
		rank = 0
	}
	if rank >= len(samples) {
		rank = len(samples) - 1
	}
	return samples[rank]
}

type latencyStats struct {
	N   int
	P50 time.Duration
	P95 time.Duration
	P99 time.Duration
	Max time.Duration
	Sum time.Duration
}

func summarize(samples []time.Duration) latencyStats {
	if len(samples) == 0 {
		return latencyStats{}
	}
	var sum, max time.Duration
	for _, d := range samples {
		sum += d
		if d > max {
			max = d
		}
	}
	cp := make([]time.Duration, len(samples))
	copy(cp, samples)
	return latencyStats{
		N:   len(samples),
		P50: percentile(cp, 50),
		P95: percentile(cp, 95),
		P99: percentile(cp, 99),
		Max: max,
		Sum: sum,
	}
}
