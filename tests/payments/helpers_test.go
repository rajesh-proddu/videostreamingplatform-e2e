// Package payments holds black-box e2e tests for the userservice payment plane:
// auth → subscribe → hosted checkout → webhook activation, plus the cross-service
// download paywall that the entitlement claim unlocks. All tests assume
// PAYMENT_PROVIDER=mock (the local/e2e default) and skip when the user service is
// unreachable.
package payments

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/yourusername/videostreamingplatform-e2e/client"
	"github.com/yourusername/videostreamingplatform-e2e/testutil"
)

const testPassword = "e2e-pass-123"

// registerAndLogin creates a fresh account and returns its credentials and a
// just-issued (unentitled) token pair.
func registerAndLogin(t *testing.T, env *testutil.Env) (email string, pair client.TokenPair) {
	t.Helper()
	email = fmt.Sprintf("e2e-pay-%d@example.com", time.Now().UnixNano())
	if err := env.User.Register(email, testPassword); err != nil {
		t.Fatalf("register: %v", err)
	}
	pair, err := env.User.Login(email, testPassword)
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	return email, pair
}

// tokenClaims base64-decodes a JWT's payload segment (no signature check — this
// is a black-box assertion on the entitlement the server stamped into the token).
func tokenClaims(t *testing.T, token string) map[string]any {
	t.Helper()
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("malformed jwt: %d segments", len(parts))
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("decode jwt payload: %v", err)
	}
	var claims map[string]any
	if err := json.Unmarshal(raw, &claims); err != nil {
		t.Fatalf("unmarshal jwt payload: %v", err)
	}
	return claims
}

func entitled(t *testing.T, token string) bool {
	t.Helper()
	v, _ := tokenClaims(t, token)["entitled"].(bool)
	return v
}

// uploadTestVideo registers metadata and uploads a payload via the data service,
// returning the video id. Used by the cross-service paywall tests.
func uploadTestVideo(t *testing.T, env *testutil.Env, payload []byte) string {
	t.Helper()
	video := env.CreateTestVideo(t, testutil.UniqueTitle("paywall"), int64(len(payload)))
	init, _, err := env.Data.InitiateUpload(&client.UploadInitiateRequest{
		VideoID:   video.ID,
		UserID:    "e2e-user",
		TotalSize: int64(len(payload)),
	})
	if err != nil {
		t.Fatalf("InitiateUpload: %v", err)
	}
	resp, err := env.Data.UploadChunk(init.UploadID, 0, payload)
	if err != nil {
		t.Fatalf("UploadChunk: %v", err)
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		t.Fatalf("UploadChunk status = %d", resp.StatusCode)
	}
	if _, _, err := env.Data.CompleteUpload(init.UploadID); err != nil {
		t.Fatalf("CompleteUpload: %v", err)
	}
	return video.ID
}
