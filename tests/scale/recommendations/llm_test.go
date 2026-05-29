package recommendations_scale

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/yourusername/videostreamingplatform-e2e/config"
)

// ollamaGenerateRequest is a minimal /api/generate body.
type ollamaGenerateRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
	Stream bool   `json:"stream"`
}

type ollamaGenerateResponse struct {
	Model              string `json:"model"`
	Response           string `json:"response"`
	Done               bool   `json:"done"`
	TotalDuration      int64  `json:"total_duration"`       // nanoseconds
	LoadDuration       int64  `json:"load_duration"`        // nanoseconds
	PromptEvalCount    int    `json:"prompt_eval_count"`
	PromptEvalDuration int64  `json:"prompt_eval_duration"` // nanoseconds
	EvalCount          int    `json:"eval_count"`
	EvalDuration       int64  `json:"eval_duration"`        // nanoseconds
}

const rankPrompt = `You are a recommender ranker.
Given the user history and candidate videos, return a JSON list of objects
[{"video_id":"<id>","score":<0-1>,"reason":"<short>"}] for the top 5.

User watched: action-thriller-101, sci-fi-202, documentary-303

Candidates:
- cand-001: "The Final Hour" (action thriller)
- cand-002: "Galaxy Drift" (sci-fi adventure)
- cand-003: "Cooking with Maya" (cooking show)
- cand-004: "Deep Space Recon" (sci-fi documentary)
- cand-005: "Mountain Trails" (nature documentary)
- cand-006: "Speed Heist" (action)
- cand-007: "Comedy Hour" (stand-up)

Respond ONLY with the JSON array.`

func ollamaAvailable(baseURL string) (string, bool) {
	resp, err := http.Get(baseURL + "/api/tags")
	if err != nil {
		return "", false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", false
	}
	var body struct {
		Models []struct {
			Name string `json:"name"`
		} `json:"models"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", false
	}
	if len(body.Models) == 0 {
		return "", false
	}
	// Prefer llama3.1 if present (configured model), otherwise first available.
	for _, m := range body.Models {
		if m.Name == "llama3.1:latest" || m.Name == "llama3.1" {
			return m.Name, true
		}
	}
	return body.Models[0].Name, true
}

// callOllama runs one /api/generate. Returns total latency and tokens/sec
// computed from server-reported eval_duration.
func callOllama(ctx context.Context, baseURL, model, prompt string) (time.Duration, *ollamaGenerateResponse, error) {
	body, _ := json.Marshal(ollamaGenerateRequest{Model: model, Prompt: prompt, Stream: false})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/api/generate", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 5 * time.Minute}
	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return time.Since(start), nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return time.Since(start), nil, fmt.Errorf("ollama status %d: %s", resp.StatusCode, raw)
	}
	var g ollamaGenerateResponse
	if err := json.NewDecoder(resp.Body).Decode(&g); err != nil {
		return time.Since(start), nil, err
	}
	return time.Since(start), &g, nil
}

func requireOllama(t *testing.T) (string, string) {
	t.Helper()
	cfg := config.Load()
	model, ok := ollamaAvailable(cfg.OllamaBaseURL)
	if !ok {
		t.Skipf("ollama unreachable or no models loaded at %s — pull a model first (e.g. `ollama pull llama3.1`)", cfg.OllamaBaseURL)
	}
	return cfg.OllamaBaseURL, model
}

func TestOllama_RankPrompt_Latency(t *testing.T) {
	baseURL, model := requireOllama(t)

	n := 30
	if testing.Short() {
		n = 5
	}

	// One warmup call to pay the model-load cost (we measure that
	// separately in TestOllama_ModelLoad_ColdStart).
	warmupCtx, cancelWarmup := context.WithTimeout(context.Background(), 5*time.Minute)
	if _, _, err := callOllama(warmupCtx, baseURL, model, "warmup"); err != nil {
		cancelWarmup()
		t.Skipf("warmup failed (model %s probably still loading or not present): %v", model, err)
	}
	cancelWarmup()

	lats := make([]time.Duration, 0, n)
	tokensTotal := 0
	evalDurationTotal := int64(0)

	for i := 0; i < n; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		d, resp, err := callOllama(ctx, baseURL, model, rankPrompt)
		cancel()
		if err != nil {
			t.Logf("call %d failed: %v", i, err)
			continue
		}
		lats = append(lats, d)
		if resp != nil {
			tokensTotal += resp.EvalCount
			evalDurationTotal += resp.EvalDuration
		}
	}
	if len(lats) == 0 {
		t.Fatalf("no successful ollama calls")
	}
	s := summarize(lats)
	var tokPerSec float64
	if evalDurationTotal > 0 {
		tokPerSec = float64(tokensTotal) / (float64(evalDurationTotal) / 1e9)
	}
	t.Logf("[ollama_rank] model=%s n=%d p50=%s p95=%s p99=%s max=%s tokens=%d tok/sec=%.1f",
		model, s.N, s.P50, s.P95, s.P99, s.Max, tokensTotal, tokPerSec)
}

func TestOllama_ModelLoad_ColdStart(t *testing.T) {
	// We deliberately do NOT bounce the ollama container — it's shared
	// with the recommendations API. Instead we send a request after first
	// asking Ollama to unload via /api/generate with keep_alive=0, then a
	// fresh /api/generate, and compare to a warm call.
	baseURL, model := requireOllama(t)

	// Issue a keep_alive=0 generate to force unload.
	unloadBody, _ := json.Marshal(map[string]any{
		"model":      model,
		"keep_alive": 0,
		"prompt":     "",
	})
	req, _ := http.NewRequest(http.MethodPost, baseURL+"/api/generate", bytes.NewReader(unloadBody))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Skipf("unload request failed: %v — skipping cold-start probe", err)
	}
	if resp != nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	// Give the runner a moment to release the model.
	time.Sleep(2 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	cold, coldResp, err := callOllama(ctx, baseURL, model, rankPrompt)
	cancel()
	if err != nil {
		t.Skipf("cold call failed: %v", err)
	}

	// Subsequent (warm) call.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Minute)
	warm, warmResp, err := callOllama(ctx2, baseURL, model, rankPrompt)
	cancel2()
	if err != nil {
		t.Fatalf("warm call: %v", err)
	}

	coldLoad := time.Duration(0)
	if coldResp != nil {
		coldLoad = time.Duration(coldResp.LoadDuration)
	}
	warmLoad := time.Duration(0)
	if warmResp != nil {
		warmLoad = time.Duration(warmResp.LoadDuration)
	}
	t.Logf("[ollama_cold] model=%s cold_total=%s cold_load=%s warm_total=%s warm_load=%s delta=%s",
		model, cold, coldLoad, warm, warmLoad, cold-warm)
}

func TestOllama_Concurrent_4Streams(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrent ollama test in -short mode")
	}
	baseURL, model := requireOllama(t)

	// Warmup.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	if _, _, err := callOllama(ctx, baseURL, model, "warmup"); err != nil {
		cancel()
		t.Skipf("warmup failed: %v", err)
	}
	cancel()

	const streams = 4
	const perStream = 5
	type result struct {
		latency time.Duration
		tokens  int
		eval    int64
		err     error
	}
	results := make([]result, streams*perStream)
	var wg sync.WaitGroup
	start := time.Now()
	for s := 0; s < streams; s++ {
		for q := 0; q < perStream; q++ {
			idx := s*perStream + q
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
				defer cancel()
				d, r, err := callOllama(ctx, baseURL, model, rankPrompt)
				if err != nil {
					results[i] = result{err: err}
					return
				}
				results[i] = result{latency: d, tokens: r.EvalCount, eval: r.EvalDuration}
			}(idx)
		}
	}
	wg.Wait()
	wall := time.Since(start)

	var lats []time.Duration
	totalTok := 0
	totalEval := int64(0)
	errs := 0
	for _, r := range results {
		if r.err != nil {
			errs++
			continue
		}
		lats = append(lats, r.latency)
		totalTok += r.tokens
		totalEval += r.eval
	}
	if len(lats) == 0 {
		t.Fatalf("no successful concurrent calls (errors=%d)", errs)
	}
	s := summarize(lats)
	aggTokPerSec := float64(totalTok) / wall.Seconds()
	t.Logf("[ollama_concurrent] model=%s streams=%d total=%d errs=%d wall=%s p50=%s p95=%s p99=%s agg_tok/sec=%.1f",
		model, streams, len(lats), errs, wall, s.P50, s.P95, s.P99, aggTokPerSec)
}
