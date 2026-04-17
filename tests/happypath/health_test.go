package happypath

import (
	"net/http"
	"testing"

	"github.com/yourusername/videostreamingplatform-e2e/testutil"
)

func TestHealthEndpoints(t *testing.T) {
	env := testutil.NewEnv(t)

	t.Run("metadata_service_healthy", func(t *testing.T) {
		status, err := env.Metadata.Health()
		if err != nil {
			t.Fatalf("metadata health request failed: %v", err)
		}
		if status != http.StatusOK {
			t.Errorf("metadata health status = %d, want 200", status)
		}
	})

	t.Run("data_service_healthy", func(t *testing.T) {
		status, err := env.Data.Health()
		if err != nil {
			t.Fatalf("data health request failed: %v", err)
		}
		if status != http.StatusOK {
			t.Errorf("data health status = %d, want 200", status)
		}
	})

	t.Run("metadata_metrics_endpoint", func(t *testing.T) {
		resp, err := env.Metadata.RawGet("/metrics")
		if err != nil {
			t.Fatalf("metrics request failed: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("metrics status = %d, want 200", resp.StatusCode)
		}
	})
}
