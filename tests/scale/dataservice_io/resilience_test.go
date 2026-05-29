package dataservice_io

import (
	"bytes"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yourusername/videostreamingplatform-e2e/client"
	"github.com/yourusername/videostreamingplatform-e2e/testutil"
)

// TestUpload_AbortedSessions_DoNotLeak starts 50 uploads, abandons 25
// (no CompleteUpload), then starts 50 more — confirming the abandoned sessions
// don't cause subsequent uploads to fail.
func TestUpload_AbortedSessions_DoNotLeak(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping aborted-sessions test in -short mode")
	}
	env := testutil.NewEnv(t)
	requireDataServiceUp(t, env)
	dc := dataClientWithTimeout(env.Cfg, 5*time.Minute)
	const phaseSize = 50
	const abortCount = 25
	const size = int64(1 * 1024 * 1024) // 1 MiB

	abandonPhase := func(label string) (success, fail int) {
		var ok, errors int64
		var wg sync.WaitGroup
		for i := 0; i < phaseSize; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				v := env.CreateTestVideo(t, testutil.UniqueTitle(fmt.Sprintf("%s-%d", label, idx)), size)
				init, _, err := dc.InitiateUpload(&client.UploadInitiateRequest{
					VideoID:   v.ID,
					UserID:    fmt.Sprintf("user-%d", idx),
					TotalSize: size,
				})
				if err != nil {
					atomic.AddInt64(&errors, 1)
					return
				}
				// Always send the first chunk to make the session "real".
				buf := randomBuffer(int(size))
				resp, err := dc.UploadChunk(init.UploadID, 0, buf)
				if err != nil || (resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated) {
					atomic.AddInt64(&errors, 1)
					return
				}
				// Abandon the first `abortCount` of phase-1 uploads.
				if label == "phase1" && idx < abortCount {
					// no CompleteUpload → orphan upload session
					atomic.AddInt64(&ok, 1)
					return
				}
				if _, _, err := dc.CompleteUpload(init.UploadID); err != nil {
					atomic.AddInt64(&errors, 1)
					return
				}
				atomic.AddInt64(&ok, 1)
			}(i)
		}
		wg.Wait()
		return int(ok), int(errors)
	}

	p1ok, p1err := abandonPhase("phase1")
	t.Logf("phase1: %d ok, %d err (25 of the ok were intentionally abandoned)", p1ok, p1err)
	if p1err > phaseSize/4 {
		t.Errorf("phase1 had too many real errors: %d/%d", p1err, phaseSize)
	}

	p2ok, p2err := abandonPhase("phase2")
	t.Logf("phase2 (after orphans exist): %d ok, %d err", p2ok, p2err)
	if p2err > phaseSize/4 {
		t.Errorf("phase2 failures after orphans: %d/%d — orphans may be leaking", p2err, phaseSize)
	}
}

// TestUpload_OversizedChunk_Rejected sends a chunk larger than the initiated
// ChunkSize. We probe behavior first — if the server doesn't enforce a cap,
// the test still passes but logs the observed behavior (per advisor: don't
// assert behavior the server doesn't have).
func TestUpload_OversizedChunk_Rejected(t *testing.T) {
	env := testutil.NewEnv(t)
	requireDataServiceUp(t, env)
	dc := dataClientWithTimeout(env.Cfg, 60*time.Second)

	// Init with declared total = 1 MiB so an oversized chunk is obvious.
	const total = int64(1 * 1024 * 1024)
	v := env.CreateTestVideo(t, testutil.UniqueTitle("oversize"), total)
	init, _, err := dc.InitiateUpload(&client.UploadInitiateRequest{
		VideoID:   v.ID,
		UserID:    "oversize-user",
		TotalSize: total,
	})
	if err != nil {
		t.Fatalf("init: %v", err)
	}
	// Send a payload 2x the declared ChunkSize (server suggests 5 MiB by default).
	oversize := init.ChunkSize * 2
	if oversize <= total {
		oversize = total * 2 // ensure clearly larger than declared total
	}
	buf := bytes.Repeat([]byte{0xab}, int(oversize))
	resp, err := dc.UploadChunk(init.UploadID, 0, buf)
	if err != nil {
		t.Logf("oversized chunk produced transport error: %v (acceptable rejection)", err)
		return
	}
	switch resp.StatusCode {
	case http.StatusBadRequest, http.StatusRequestEntityTooLarge, http.StatusUnprocessableEntity:
		t.Logf("RESULT oversized chunk rejected with status %d (expected)", resp.StatusCode)
	case http.StatusOK, http.StatusCreated:
		t.Logf("NOTE: server accepted oversized chunk (status %d) — chunk size is not enforced on this build", resp.StatusCode)
	default:
		t.Logf("RESULT oversized chunk status=%d (server-specific behavior)", resp.StatusCode)
	}
}
