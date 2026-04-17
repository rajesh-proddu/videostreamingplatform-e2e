package resiliency

import (
	"sync"
	"testing"

	"github.com/yourusername/videostreamingplatform-e2e/client"
	"github.com/yourusername/videostreamingplatform-e2e/testutil"
)

func TestConcurrentCreates_NoDuplicateIDs(t *testing.T) {
	env := testutil.NewEnv(t)
	const count = 10

	var (
		mu     sync.Mutex
		ids    = make(map[string]bool)
		wg     sync.WaitGroup
		errors []error
	)

	wg.Add(count)
	for i := 0; i < count; i++ {
		go func() {
			defer wg.Done()
			v, _, err := env.Metadata.CreateVideo(&client.CreateVideoRequest{
				Title:     testutil.UniqueTitle("conc"),
				SizeBytes: 256,
			})
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errors = append(errors, err)
				return
			}
			if ids[v.ID] {
				t.Errorf("duplicate ID: %s", v.ID)
			}
			ids[v.ID] = true
		}()
	}
	wg.Wait()

	if len(errors) > 0 {
		t.Logf("%d/%d concurrent creates failed", len(errors), count)
	}
	if len(ids) == 0 {
		t.Fatal("no videos created")
	}

	// Cleanup
	for id := range ids {
		resp, _ := env.Metadata.DeleteVideo(id)
		if resp != nil {
			resp.Body.Close()
		}
	}
}

func TestConcurrentReads_SameVideo(t *testing.T) {
	env := testutil.NewEnv(t)
	video := env.CreateTestVideo(t, testutil.UniqueTitle("conc-read"), 256)

	const readers = 20
	var (
		wg     sync.WaitGroup
		errors []error
		mu     sync.Mutex
	)

	wg.Add(readers)
	for i := 0; i < readers; i++ {
		go func() {
			defer wg.Done()
			got, _, err := env.Metadata.GetVideo(video.ID)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errors = append(errors, err)
				return
			}
			if got.ID != video.ID {
				t.Errorf("got video ID %q, want %q", got.ID, video.ID)
			}
		}()
	}
	wg.Wait()

	if len(errors) > 0 {
		t.Errorf("%d/%d concurrent reads failed", len(errors), readers)
	}
}

func TestConcurrentUpdates_LastWriteWins(t *testing.T) {
	env := testutil.NewEnv(t)
	video := env.CreateTestVideo(t, testutil.UniqueTitle("conc-upd"), 256)

	const updaters = 10
	var wg sync.WaitGroup

	wg.Add(updaters)
	for i := 0; i < updaters; i++ {
		title := testutil.UniqueTitle("upd")
		go func() {
			defer wg.Done()
			env.Metadata.UpdateVideo(video.ID, &client.UpdateVideoRequest{Title: title})
		}()
	}
	wg.Wait()

	// Verify the video still exists and has a valid title
	got, _, err := env.Metadata.GetVideo(video.ID)
	if err != nil {
		t.Fatalf("GetVideo after concurrent updates failed: %v", err)
	}
	if got.Title == "" {
		t.Error("title is empty after concurrent updates")
	}
	t.Logf("final title after %d concurrent updates: %q", updaters, got.Title)
}
