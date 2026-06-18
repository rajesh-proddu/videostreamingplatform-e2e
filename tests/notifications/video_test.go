package notifications

import (
	"strings"
	"testing"

	"github.com/yourusername/videostreamingplatform-e2e/client"
	"github.com/yourusername/videostreamingplatform-e2e/testutil"
)

// TestVideoPublishedBroadcastReachesWebSocket drives the metadata service's real
// create-video endpoint and asserts the resulting video.created event flows
// through Kafka into the notifications service and is delivered to a connected
// client as a VIDEO_PUBLISHED broadcast (user_id = nil). This is the fully
// end-to-end chain with no event injection.
func TestVideoPublishedBroadcastReachesWebSocket(t *testing.T) {
	env := testutil.NewEnv(t)
	env.RequireUser(t)
	env.RequireNotifications(t)

	// Any valid JWT can hold a socket; broadcasts go to every connected client.
	token, _ := registerAndLogin(t, env)
	stream := connect(t, env, token)

	title := testutil.UniqueTitle("notif-video")
	video := env.CreateTestVideo(t, title, 1024)

	// Match our own video's broadcast (title carries through the rule) so a
	// concurrently-created video in a shared environment can't satisfy this.
	n, err := stream.WaitFor(ctxTimeout(t, env.Cfg.NotificationWaitTime), env.Cfg.NotificationWaitTime,
		func(n client.Notification) bool {
			return n.Type == "VIDEO_PUBLISHED" && strings.Contains(n.Title, title)
		})
	if err != nil {
		t.Fatalf("waiting for VIDEO_PUBLISHED broadcast for %q (video %s): %v", title, video.ID, err)
	}
	if n.UserID != nil {
		t.Errorf("VIDEO_PUBLISHED should be a broadcast (user_id nil), got %q", *n.UserID)
	}
}
