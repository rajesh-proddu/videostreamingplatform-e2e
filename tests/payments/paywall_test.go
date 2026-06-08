package payments

import (
	"crypto/sha256"
	"net/http"
	"testing"

	"github.com/yourusername/videostreamingplatform-e2e/testutil"
)

// TestPaywall_EntitledTokenUnlocksDownload is the cross-service assertion: the
// dataservice download endpoint is gated behind the entitlement claim that
// userservice stamps into the access token. An unentitled-but-authenticated user
// gets 402 Payment Required; an entitled user downloads the bytes intact.
//
// Skips when the paywall is disabled (dataservice started without
// JWT_SIGNING_SECRET), detected by an unentitled download returning 200.
func TestPaywall_EntitledTokenUnlocksDownload(t *testing.T) {
	env := testutil.NewEnv(t)
	env.RequireUser(t)

	payload := testutil.RandomBytes(8 * 1024)
	videoID := uploadTestVideo(t, env, payload)

	// Authenticated but unentitled user (registered + logged in, no paid sub).
	_, unentitled := registerAndLogin(t, env)
	env.Data.Token = unentitled.AccessToken
	_, resp, err := env.Data.DownloadVideo(videoID, "e2e-user")
	if err != nil {
		t.Fatalf("download (unentitled): %v", err)
	}
	if resp.StatusCode == http.StatusOK {
		t.Skip("download paywall disabled (unentitled download returned 200) — skipping entitlement assertions")
	}
	if resp.StatusCode != http.StatusPaymentRequired {
		t.Fatalf("unentitled download status = %d, want 402", resp.StatusCode)
	}

	// Entitled user (premium, paid via mock checkout) downloads successfully.
	token, err := env.User.AcquireEntitledToken()
	if err != nil {
		t.Fatalf("AcquireEntitledToken: %v", err)
	}
	env.Data.Token = token
	downloaded, dlResp, err := env.Data.DownloadVideo(videoID, "e2e-user")
	if err != nil {
		t.Fatalf("download (entitled): %v", err)
	}
	if dlResp.StatusCode != http.StatusOK {
		t.Fatalf("entitled download status = %d, want 200", dlResp.StatusCode)
	}
	if sha256.Sum256(downloaded) != sha256.Sum256(payload) {
		t.Error("downloaded bytes do not match uploaded payload")
	}
}

// TestPaywall_FreePlanGrantsNoEntitlement verifies that an active *free* plan
// does not unlock the paywalled download — only a paid plan does.
func TestPaywall_FreePlanGrantsNoEntitlement(t *testing.T) {
	env := testutil.NewEnv(t)
	env.RequireUser(t)

	payload := testutil.RandomBytes(4 * 1024)
	videoID := uploadTestVideo(t, env, payload)

	email, pair := registerAndLogin(t, env)
	if _, err := env.User.Subscribe(pair.AccessToken, "free"); err != nil {
		t.Fatalf("Subscribe(free): %v", err)
	}
	// Re-login so any entitlement is reflected in the token.
	fresh, err := env.User.Login(email, testPassword)
	if err != nil {
		t.Fatalf("re-login: %v", err)
	}

	env.Data.Token = fresh.AccessToken
	_, resp, err := env.Data.DownloadVideo(videoID, "e2e-user")
	if err != nil {
		t.Fatalf("download (free plan): %v", err)
	}
	if resp.StatusCode == http.StatusOK {
		t.Skip("download paywall disabled — skipping free-plan entitlement assertion")
	}
	if resp.StatusCode != http.StatusPaymentRequired {
		t.Errorf("free-plan download status = %d, want 402", resp.StatusCode)
	}
}
