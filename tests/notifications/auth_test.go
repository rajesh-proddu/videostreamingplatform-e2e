package notifications

import (
	"net/http"
	"testing"

	"github.com/yourusername/videostreamingplatform-e2e/testutil"
)

// TestWebSocketRejectsUnauthenticated asserts the WS gateway refuses the upgrade
// without a valid JWT — the same shared-secret HS256 token the rest of the
// platform uses gates the realtime stream.
func TestWebSocketRejectsUnauthenticated(t *testing.T) {
	env := testutil.NewEnv(t)
	env.RequireNotifications(t)

	for _, tc := range []struct {
		name  string
		token string
	}{
		{"empty token", ""},
		{"garbage token", "not.a.jwt"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, err := env.Notify.ConnectStatus(tc.token)
			if err != nil {
				t.Fatalf("ws handshake attempt: %v", err)
			}
			if code != http.StatusUnauthorized {
				t.Errorf("status = %d, want %d", code, http.StatusUnauthorized)
			}
		})
	}
}

// TestRESTRejectsUnauthenticated asserts the REST notification center is gated by
// the same JWT.
func TestRESTRejectsUnauthenticated(t *testing.T) {
	env := testutil.NewEnv(t)
	env.RequireNotifications(t)

	if _, err := env.Notify.List("not.a.jwt"); err == nil {
		t.Errorf("expected error listing notifications with an invalid token")
	}
}
