// Package notifications holds black-box e2e tests for the realtime notifications
// platform. They drive original platform endpoints (subscribe/checkout via the
// user service, video create via the metadata service) and assert that the
// resulting domain events flow through Kafka into the notifications service and
// are delivered to connected clients over WebSockets — plus the durable
// reconnect backlog and REST notification center that back the live stream.
//
// Chain under test:
//
//	userservice POST /subscriptions + /mock/checkout ─┐
//	metadataservice POST /videos ─────────────────────┤ (Kafka: subscription-events / video-events)
//	                                                   ▼
//	         notifications notif-rules ──► notification-events ──► ch-web
//	                                                   │ MySQL inbox + Redis publish
//	                                                   ▼
//	                              WS gateway ──► e2e client (GET /ws?token=)
//
// All tests skip when the notifications or user service is unreachable, and
// assume PAYMENT_PROVIDER=mock (the local/e2e default).
package notifications

import (
	"context"
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

// registerAndLogin creates a fresh account and returns its access token plus the
// user id (the JWT subject), which is also the notification routing key.
func registerAndLogin(t *testing.T, env *testutil.Env) (token, userID string) {
	t.Helper()
	email := fmt.Sprintf("e2e-notif-%d@example.com", time.Now().UnixNano())
	if err := env.User.Register(email, testPassword); err != nil {
		t.Fatalf("register: %v", err)
	}
	pair, err := env.User.Login(email, testPassword)
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	return pair.AccessToken, subjectOf(t, pair.AccessToken)
}

// subjectOf base64-decodes a JWT's payload and returns the `sub` claim (the user
// id). No signature check — this is a black-box read of what the server stamped.
func subjectOf(t *testing.T, token string) string {
	t.Helper()
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("malformed jwt: %d segments", len(parts))
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("decode jwt payload: %v", err)
	}
	var claims struct {
		Sub string `json:"sub"`
	}
	if err := json.Unmarshal(raw, &claims); err != nil {
		t.Fatalf("unmarshal jwt payload: %v", err)
	}
	if claims.Sub == "" {
		t.Fatalf("jwt has no sub claim")
	}
	return claims.Sub
}

// subscribeAndPay drives the real subscription endpoints: subscribe to premium,
// then complete the hosted (mock) checkout, which the webhook turns into an
// activated subscription. Activation emits subscription.activated on Kafka.
func subscribeAndPay(t *testing.T, env *testutil.Env, token string) {
	t.Helper()
	sub, err := env.User.Subscribe(token, "premium")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	ref, err := client.RefFromPaymentURL(sub.PaymentURL)
	if err != nil {
		t.Fatalf("payment ref: %v", err)
	}
	if code, err := env.User.MockCheckout(ref, "paid"); err != nil {
		t.Fatalf("mock checkout: %v", err)
	} else if code != http.StatusOK {
		t.Fatalf("mock checkout: status %d", code)
	}
}

// waitForRESTType polls the REST notification center until a notification of the
// given type appears or the timeout elapses, returning it.
func waitForRESTType(t *testing.T, env *testutil.Env, token, typ string, timeout time.Duration) client.Notification {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		items, err := env.Notify.List(token)
		if err != nil {
			t.Fatalf("list notifications: %v", err)
		}
		for _, n := range items {
			if n.Type == typ {
				return n
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("notification type %s not in REST center within %s", typ, timeout)
	return client.Notification{}
}

// connect opens a WebSocket stream and registers cleanup.
func connect(t *testing.T, env *testutil.Env, token string) *client.NotificationStream {
	t.Helper()
	stream, err := env.Notify.Connect(token)
	if err != nil {
		t.Fatalf("ws connect: %v", err)
	}
	t.Cleanup(func() { _ = stream.Close() })
	return stream
}

func ctxTimeout(t *testing.T, d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}
