package notifications

import (
	"testing"
	"time"

	"github.com/yourusername/videostreamingplatform-e2e/testutil"
)

// TestSubscriptionActivatedReachesWebSocket is the headline flow: a user connects
// to the WebSocket, then drives the real subscription endpoints (subscribe →
// hosted checkout). Activation emits subscription.activated on Kafka, which the
// notifications rule engine maps to a SUBSCRIPTION_ACTIVATED notification that is
// delivered live over the socket — and is also durably queryable via REST.
func TestSubscriptionActivatedReachesWebSocket(t *testing.T) {
	env := testutil.NewEnv(t)
	env.RequireUser(t)
	env.RequireNotifications(t)

	token, userID := registerAndLogin(t, env)

	// Connect before subscribing so the activation arrives as a live frame
	// (the inbox is empty for a fresh user, so there is no backlog noise).
	stream := connect(t, env, token)

	subscribeAndPay(t, env, token)

	n, err := stream.WaitForType(ctxTimeout(t, env.Cfg.NotificationWaitTime),
		"SUBSCRIPTION_ACTIVATED", env.Cfg.NotificationWaitTime)
	if err != nil {
		t.Fatalf("waiting for SUBSCRIPTION_ACTIVATED over websocket: %v", err)
	}
	if n.UserID == nil || *n.UserID != userID {
		t.Fatalf("notification user_id = %v, want %s", n.UserID, userID)
	}
	if n.Title == "" {
		t.Errorf("expected a non-empty title on the activation notification")
	}

	// The same notification must be durably visible through the REST center.
	got := waitForRESTType(t, env, token, "SUBSCRIPTION_ACTIVATED", env.Cfg.NotificationWaitTime)
	if got.ReadAt != nil {
		t.Errorf("freshly delivered notification should be unread, got read_at=%v", got.ReadAt)
	}
}

// TestNotificationBacklogReplayAndMarkRead exercises the durable read model: a
// notification produced while the user is offline is replayed from the MySQL
// inbox when the user (re)connects, and marking it read removes it from the
// reconnect backlog.
func TestNotificationBacklogReplayAndMarkRead(t *testing.T) {
	env := testutil.NewEnv(t)
	env.RequireUser(t)
	env.RequireNotifications(t)

	token, userID := registerAndLogin(t, env)

	// Produce the notification with no socket connected, then wait for it to land
	// durably in the inbox — this is the "missed while offline" case.
	subscribeAndPay(t, env, token)
	waitForRESTType(t, env, token, "SUBSCRIPTION_ACTIVATED", env.Cfg.NotificationWaitTime)

	// Reconnect: the unread notification replays from the inbox as backlog.
	stream := connect(t, env, token)
	n, err := stream.WaitForType(ctxTimeout(t, env.Cfg.NotificationWaitTime),
		"SUBSCRIPTION_ACTIVATED", env.Cfg.NotificationWaitTime)
	if err != nil {
		t.Fatalf("reconnect backlog replay: %v", err)
	}
	if n.UserID == nil || *n.UserID != userID {
		t.Fatalf("backlog notification user_id = %v, want %s", n.UserID, userID)
	}
	_ = stream.Close()

	// Mark all read, then reconnect again — a read notification is no longer in
	// the (unread) reconnect backlog.
	if err := env.Notify.MarkRead(token, nil); err != nil {
		t.Fatalf("mark read: %v", err)
	}
	stream2 := connect(t, env, token)
	if n, err := stream2.WaitForType(ctxTimeout(t, 5*time.Second),
		"SUBSCRIPTION_ACTIVATED", 5*time.Second); err == nil {
		t.Fatalf("read notification should not replay on reconnect, but got %s", n.ID)
	}
}
