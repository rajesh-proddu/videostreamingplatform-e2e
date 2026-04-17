package scale

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/yourusername/videostreamingplatform-e2e/client"
	"github.com/yourusername/videostreamingplatform-e2e/testutil"
)

func TestBulkVideoCreation(t *testing.T) {
	env := testutil.NewEnv(t)
	count := env.Cfg.BulkCount

	var ids []string
	start := time.Now()

	for i := 0; i < count; i++ {
		v, _, err := env.Metadata.CreateVideo(&client.CreateVideoRequest{
			Title:     fmt.Sprintf("bulk-%d-%s", i, testutil.UniqueTitle("b")),
			SizeBytes: 256,
		})
		if err != nil {
			t.Fatalf("create %d/%d failed: %v", i+1, count, err)
		}
		ids = append(ids, v.ID)
	}

	elapsed := time.Since(start)
	t.Logf("created %d videos in %v (%.1f/s)", count, elapsed, float64(count)/elapsed.Seconds())

	// Verify all exist via list
	list, _, err := env.Metadata.ListVideos(count+10, 0)
	if err != nil {
		t.Fatalf("ListVideos failed: %v", err)
	}
	if len(list.Videos) < count {
		t.Errorf("list returned %d videos, expected at least %d", len(list.Videos), count)
	}

	// Cleanup
	for _, id := range ids {
		resp, _ := env.Metadata.DeleteVideo(id)
		if resp != nil {
			resp.Body.Close()
		}
	}
}

func TestConcurrentVideoCreation(t *testing.T) {
	env := testutil.NewEnv(t)
	concurrency := env.Cfg.ConcurrentUsers

	var (
		mu      sync.Mutex
		ids     []string
		errors  int
		wg      sync.WaitGroup
	)

	start := time.Now()
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func(n int) {
			defer wg.Done()
			v, _, err := env.Metadata.CreateVideo(&client.CreateVideoRequest{
				Title:     fmt.Sprintf("concurrent-%d-%s", n, testutil.UniqueTitle("c")),
				SizeBytes: 256,
			})
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errors++
				return
			}
			ids = append(ids, v.ID)
		}(i)
	}
	wg.Wait()

	elapsed := time.Since(start)
	t.Logf("concurrent creates: %d success, %d errors in %v", len(ids), errors, elapsed)

	if errors > concurrency/2 {
		t.Errorf("too many errors: %d/%d failed", errors, concurrency)
	}

	// Cleanup
	for _, id := range ids {
		resp, _ := env.Metadata.DeleteVideo(id)
		if resp != nil {
			resp.Body.Close()
		}
	}
}

func TestBulkDeletion(t *testing.T) {
	env := testutil.NewEnv(t)
	count := 20

	// Create videos
	var ids []string
	for i := 0; i < count; i++ {
		v := env.CreateTestVideo(t, fmt.Sprintf("bulk-del-%d-%s", i, testutil.UniqueTitle("d")), 128)
		ids = append(ids, v.ID)
	}

	// Delete all
	start := time.Now()
	for _, id := range ids {
		resp, err := env.Metadata.DeleteVideo(id)
		if err != nil {
			t.Errorf("delete %s failed: %v", id, err)
		}
		if resp != nil {
			resp.Body.Close()
		}
	}

	elapsed := time.Since(start)
	t.Logf("deleted %d videos in %v (%.1f/s)", count, elapsed, float64(count)/elapsed.Seconds())

	// Verify all gone
	for _, id := range ids {
		_, resp, _ := env.Metadata.GetVideo(id)
		if resp != nil && resp.StatusCode == 200 {
			t.Errorf("video %s still exists after deletion", id)
		}
	}
}
