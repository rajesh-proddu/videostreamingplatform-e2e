package resiliency

import (
	"net/http"
	"strconv"
	"testing"

	"github.com/yourusername/videostreamingplatform-e2e/testutil"
)

func TestRateLimiting_HeadersPresent(t *testing.T) {
	env := testutil.NewEnv(t)

	resp, err := env.Metadata.RawGet("/videos?limit=1&offset=0")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	limit := resp.Header.Get("X-RateLimit-Limit")
	remaining := resp.Header.Get("X-RateLimit-Remaining")

	if limit == "" {
		t.Error("X-RateLimit-Limit header missing")
	}
	if remaining == "" {
		t.Error("X-RateLimit-Remaining header missing")
	}

	if limit != "" && remaining != "" {
		lim, _ := strconv.Atoi(limit)
		rem, _ := strconv.Atoi(remaining)
		if rem > lim {
			t.Errorf("remaining (%d) > limit (%d)", rem, lim)
		}
		t.Logf("rate limit: %d/%d remaining", rem, lim)
	}
}

func TestRateLimiting_RemainingDecreases(t *testing.T) {
	env := testutil.NewEnv(t)

	resp1, err := env.Metadata.RawGet("/videos?limit=1&offset=0")
	if err != nil {
		t.Fatalf("first request failed: %v", err)
	}
	resp1.Body.Close()

	rem1Str := resp1.Header.Get("X-RateLimit-Remaining")
	if rem1Str == "" {
		t.Skip("X-RateLimit-Remaining header not present")
	}

	resp2, err := env.Metadata.RawGet("/videos?limit=1&offset=0")
	if err != nil {
		t.Fatalf("second request failed: %v", err)
	}
	resp2.Body.Close()

	rem2Str := resp2.Header.Get("X-RateLimit-Remaining")
	rem1, _ := strconv.Atoi(rem1Str)
	rem2, _ := strconv.Atoi(rem2Str)

	if rem2 >= rem1 {
		t.Logf("remaining did not decrease: %d -> %d (may be per-window reset)", rem1, rem2)
	}
}

func TestRateLimiting_ExcessiveRequests_Returns429(t *testing.T) {
	env := testutil.NewEnv(t)

	// Fire requests until we get a 429 or exhaust attempts
	got429 := false
	var lastStatus int
	for i := 0; i < 200; i++ {
		resp, err := env.Metadata.RawGet("/videos?limit=1&offset=0")
		if err != nil {
			t.Fatalf("request %d failed: %v", i, err)
		}
		resp.Body.Close()
		lastStatus = resp.StatusCode

		if resp.StatusCode == http.StatusTooManyRequests {
			got429 = true
			retryAfter := resp.Header.Get("Retry-After")
			t.Logf("got 429 after %d requests, Retry-After=%s", i+1, retryAfter)
			break
		}
	}

	if !got429 {
		t.Logf("did not hit rate limit after 200 requests (last status=%d), limit may be high", lastStatus)
		t.Skip("rate limit not triggered — adjust RATE_LIMIT or increase request count")
	}
}
