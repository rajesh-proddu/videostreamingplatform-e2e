package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// UserClient wraps the user service: auth + subscriptions. It is used by e2e
// tests to obtain an entitled access token so gated endpoints (download) succeed.
type UserClient struct {
	BaseURL    string
	HTTPClient *http.Client
}

func NewUserClient(baseURL string, timeout time.Duration) *UserClient {
	return &UserClient{
		BaseURL:    baseURL,
		HTTPClient: &http.Client{Timeout: timeout},
	}
}

func (c *UserClient) Health() (int, error) {
	resp, err := c.HTTPClient.Get(c.BaseURL + "/health")
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	return resp.StatusCode, nil
}

// AcquireEntitledToken registers a fresh user, subscribes to premium, simulates
// payment via the mock provider's hosted checkout, and returns an access token
// carrying the active entitlement. Assumes PAYMENT_PROVIDER=mock (the e2e default).
func (c *UserClient) AcquireEntitledToken() (string, error) {
	email := fmt.Sprintf("e2e-%d@example.com", time.Now().UnixNano())
	const pw = "e2e-pass-123"

	if err := c.post("/auth/register", "", map[string]string{"email": email, "password": pw}, nil); err != nil {
		return "", fmt.Errorf("register: %w", err)
	}
	token, err := c.login(email, pw)
	if err != nil {
		return "", err
	}

	var sub struct {
		PaymentURL string `json:"payment_url"`
	}
	if err := c.post("/subscriptions", token, map[string]string{"plan": "premium"}, &sub); err != nil {
		return "", fmt.Errorf("subscribe: %w", err)
	}
	ref, err := refFromURL(sub.PaymentURL)
	if err != nil {
		return "", err
	}

	// Simulate the hosted payment → webhook activates the subscription.
	resp, err := c.HTTPClient.Get(fmt.Sprintf("%s/mock/checkout?ref=%s", c.BaseURL, ref))
	if err != nil {
		return "", fmt.Errorf("mock checkout: %w", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("mock checkout: status %d", resp.StatusCode)
	}

	// Re-login for a token carrying the active entitlement.
	return c.login(email, pw)
}

func (c *UserClient) login(email, pw string) (string, error) {
	var out struct {
		AccessToken string `json:"access_token"`
	}
	if err := c.post("/auth/login", "", map[string]string{"email": email, "password": pw}, &out); err != nil {
		return "", fmt.Errorf("login: %w", err)
	}
	return out.AccessToken, nil
}

func (c *UserClient) post(path, token string, body, out any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, c.BaseURL+path, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		rb, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%s: status %d: %s", path, resp.StatusCode, rb)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

func refFromURL(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("parse payment url: %w", err)
	}
	ref := u.Query().Get("ref")
	if ref == "" {
		return "", fmt.Errorf("no ref in payment url %q (mock provider expected)", raw)
	}
	return ref, nil
}
