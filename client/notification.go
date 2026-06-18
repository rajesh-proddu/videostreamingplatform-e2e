package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// NotificationClient wraps the notifications service: the WebSocket stream
// (GET /ws?token=) plus the REST notification center (GET /api/v1/notifications,
// POST /api/v1/notifications/read). The realtime frame and the REST/backlog rows
// are the same JSON shape (the service's store.Notification).
type NotificationClient struct {
	BaseURL    string // http(s) base, e.g. http://127.0.0.1:8083
	HTTPClient *http.Client
}

func NewNotificationClient(baseURL string, timeout time.Duration) *NotificationClient {
	return &NotificationClient{
		BaseURL:    baseURL,
		HTTPClient: &http.Client{Timeout: timeout},
	}
}

// Notification mirrors the notifications service's store.Notification wire shape.
// A nil UserID means a broadcast (delivered to all users).
type Notification struct {
	ID        string     `json:"id"`
	UserID    *string    `json:"user_id,omitempty"`
	Type      string     `json:"type"`
	Title     string     `json:"title,omitempty"`
	Body      string     `json:"body,omitempty"`
	Metadata  string     `json:"metadata,omitempty"`
	CreatedAt time.Time  `json:"created_at"`
	ReadAt    *time.Time `json:"read_at,omitempty"`
}

func (c *NotificationClient) Health() (int, error) {
	resp, err := c.HTTPClient.Get(c.BaseURL + "/health")
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	return resp.StatusCode, nil
}

// Connect opens an authenticated WebSocket stream. The returned NotificationStream
// reads frames in the background; the caller must Close it (e.g. via t.Cleanup).
func (c *NotificationClient) Connect(token string) (*NotificationStream, error) {
	wsURL, err := c.wsURL(token)
	if err != nil {
		return nil, err
	}
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		code := 0
		if resp != nil {
			code = resp.StatusCode
		}
		return nil, fmt.Errorf("ws dial %s: %w (status %d)", wsURL, err, code)
	}
	s := &NotificationStream{
		conn: conn,
		recv: make(chan Notification, 64),
		done: make(chan struct{}),
	}
	go s.readLoop()
	return s, nil
}

// ConnectStatus attempts a WebSocket handshake and returns the HTTP status code
// on failure (without establishing a stream). Used to assert auth rejection.
func (c *NotificationClient) ConnectStatus(token string) (int, error) {
	wsURL, err := c.wsURL(token)
	if err != nil {
		return 0, err
	}
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err == nil {
		conn.Close()
		return resp.StatusCode, nil
	}
	if resp != nil {
		return resp.StatusCode, nil
	}
	return 0, err
}

// wsURL builds ws(s)://host/ws?token=… from the http(s) base URL.
func (c *NotificationClient) wsURL(token string) (string, error) {
	u, err := url.Parse(c.BaseURL)
	if err != nil {
		return "", fmt.Errorf("parse notification base url: %w", err)
	}
	switch u.Scheme {
	case "https":
		u.Scheme = "wss"
	default:
		u.Scheme = "ws"
	}
	u.Path = "/ws"
	u.RawQuery = url.Values{"token": {token}}.Encode()
	return u.String(), nil
}

// List returns the user's recent notifications (REST notification center).
func (c *NotificationClient) List(token string) ([]Notification, error) {
	req, _ := http.NewRequest(http.MethodGet, c.BaseURL+"/api/v1/notifications", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		rb, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("list notifications: status %d: %s", resp.StatusCode, rb)
	}
	var out struct {
		Notifications []Notification `json:"notifications"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out.Notifications, nil
}

// MarkRead marks the given notification ids read; an empty slice marks all of the
// user's unread notifications.
func (c *NotificationClient) MarkRead(token string, ids []string) error {
	body, _ := json.Marshal(map[string][]string{"ids": ids})
	req, _ := http.NewRequest(http.MethodPost, c.BaseURL+"/api/v1/notifications/read", strings.NewReader(string(body)))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		rb, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("mark read: status %d: %s", resp.StatusCode, rb)
	}
	return nil
}

// NotificationStream is a live WebSocket stream of notification frames.
type NotificationStream struct {
	conn      *websocket.Conn
	recv      chan Notification
	done      chan struct{}
	closeOnce sync.Once
}

func (s *NotificationStream) readLoop() {
	defer close(s.recv)
	for {
		_, data, err := s.conn.ReadMessage()
		if err != nil {
			return // connection closed or errored
		}
		var n Notification
		if json.Unmarshal(data, &n) != nil {
			continue
		}
		select {
		case s.recv <- n:
		case <-s.done:
			return
		}
	}
}

// WaitForType waits up to timeout for a frame of the given type, returning it.
// Frames of other types (e.g. a backlog broadcast) are skipped.
func (s *NotificationStream) WaitForType(ctx context.Context, typ string, timeout time.Duration) (*Notification, error) {
	n, err := s.WaitFor(ctx, timeout, func(n Notification) bool { return n.Type == typ })
	if err != nil {
		return nil, fmt.Errorf("%w (type %s)", err, typ)
	}
	return n, nil
}

// WaitFor waits up to timeout for a frame satisfying match, returning it.
// Non-matching frames are skipped.
func (s *NotificationStream) WaitFor(ctx context.Context, timeout time.Duration, match func(Notification) bool) (*Notification, error) {
	deadline := time.After(timeout)
	for {
		select {
		case n, ok := <-s.recv:
			if !ok {
				return nil, fmt.Errorf("stream closed while waiting for notification")
			}
			if match(n) {
				return &n, nil
			}
		case <-deadline:
			return nil, fmt.Errorf("timed out after %s waiting for notification", timeout)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Close terminates the stream and its background reader. Idempotent: a test may
// close explicitly and again via t.Cleanup.
func (s *NotificationStream) Close() error {
	s.closeOnce.Do(func() { close(s.done) })
	return s.conn.Close()
}
