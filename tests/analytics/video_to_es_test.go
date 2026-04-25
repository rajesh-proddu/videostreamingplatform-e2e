package analytics

import (
	"strings"
	"sync"
	"testing"

	"github.com/yourusername/videostreamingplatform-e2e/client"
	"github.com/yourusername/videostreamingplatform-e2e/testutil"
)

func TestES_VideoCreate_IndexesDocument(t *testing.T) {
	env := testutil.NewEnv(t)
	env.RequireES(t)

	title := testutil.UniqueTitle("es-create")
	v := env.CreateTestVideo(t, title, 1024)

	doc, err := env.ES.WaitForDoc(v.ID, true, env.Cfg.AnalyticsWaitTime)
	if err != nil {
		t.Fatalf("doc not indexed: %v", err)
	}
	if got, _ := doc.Source["title"].(string); got != title {
		t.Fatalf("title in ES = %q, want %q", got, title)
	}
}

func TestES_VideoUpdate_ReplacesDocument(t *testing.T) {
	env := testutil.NewEnv(t)
	env.RequireES(t)

	v := env.CreateTestVideo(t, testutil.UniqueTitle("es-upd-orig"), 1024)
	if _, err := env.ES.WaitForDoc(v.ID, true, env.Cfg.AnalyticsWaitTime); err != nil {
		t.Fatalf("initial index: %v", err)
	}

	newTitle := testutil.UniqueTitle("es-upd-new")
	if _, _, err := env.Metadata.UpdateVideo(v.ID, &client.UpdateVideoRequest{Title: newTitle}); err != nil {
		t.Fatalf("UpdateVideo: %v", err)
	}

	deadline := env.Cfg.AnalyticsWaitTime
	doc, err := env.ES.WaitForDoc(v.ID, true, deadline)
	if err != nil {
		t.Fatalf("post-update lookup: %v", err)
	}
	got, _ := doc.Source["title"].(string)
	for tries := 0; tries < 10 && got != newTitle; tries++ {
		doc, _ = env.ES.WaitForDoc(v.ID, true, deadline)
		got, _ = doc.Source["title"].(string)
	}
	if got != newTitle {
		t.Fatalf("title in ES = %q, want %q", got, newTitle)
	}
}

func TestES_VideoDelete_RemovesDocument(t *testing.T) {
	env := testutil.NewEnv(t)
	env.RequireES(t)

	v := env.CreateTestVideo(t, testutil.UniqueTitle("es-del"), 1024)
	if _, err := env.ES.WaitForDoc(v.ID, true, env.Cfg.AnalyticsWaitTime); err != nil {
		t.Fatalf("initial index: %v", err)
	}

	resp, err := env.Metadata.DeleteVideo(v.ID)
	if err != nil {
		t.Fatalf("DeleteVideo: %v", err)
	}
	resp.Body.Close()

	if _, err := env.ES.WaitForDoc(v.ID, false, env.Cfg.AnalyticsWaitTime); err != nil {
		t.Fatalf("doc still in ES: %v", err)
	}
}

func TestES_RapidCreateBurst_AllIndexed(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping burst test in -short mode")
	}
	env := testutil.NewEnv(t)
	env.RequireES(t)

	const n = 20
	ids := make([]string, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v := env.CreateTestVideo(t, testutil.UniqueTitle("burst"), 256)
			ids[i] = v.ID
		}(i)
	}
	wg.Wait()

	missing := []string{}
	for _, id := range ids {
		doc, err := env.ES.WaitForDoc(id, true, env.Cfg.AnalyticsWaitTime)
		if err != nil || !doc.Found {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("%d/%d videos missing in ES: %s", len(missing), n, strings.Join(missing, ","))
	}
}
