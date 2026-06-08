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

// UserClient wraps the user service: auth, subscriptions, and payments. It is
// used by e2e tests both as a helper (obtain an entitled token so gated
// endpoints succeed) and directly (the payments suite drives subscribe →
// checkout → webhook flows).
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

// TokenPair mirrors the auth endpoints' token response.
type TokenPair struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
}

// SubscribeResult mirrors userservice bl.SubscribeResult.
type SubscribeResult struct {
	SubscriptionID string `json:"subscription_id"`
	PaymentURL     string `json:"payment_url"` // empty when activated immediately (free plan)
	Status         string `json:"status"`
	AlreadyActive  bool   `json:"already_active"`
}

// CurrentSubscription mirrors GET /subscriptions/me.
type CurrentSubscription struct {
	Active       bool `json:"active"`
	Subscription *struct {
		ID               string     `json:"id"`
		Status           string     `json:"status"`
		CurrentPeriodEnd *time.Time `json:"current_period_end"`
	} `json:"subscription"`
}

func (c *UserClient) Health() (int, error) {
	resp, err := c.HTTPClient.Get(c.BaseURL + "/health")
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	return resp.StatusCode, nil
}

// Register creates a new account.
func (c *UserClient) Register(email, password string) error {
	return c.post("/auth/register", "", map[string]string{"email": email, "password": password}, nil)
}

// Login authenticates and returns the token pair. The access token carries the
// caller's current entitlement, so re-login after a payment yields an entitled
// token.
func (c *UserClient) Login(email, password string) (TokenPair, error) {
	var out TokenPair
	if err := c.post("/auth/login", "", map[string]string{"email": email, "password": password}, &out); err != nil {
		return TokenPair{}, fmt.Errorf("login: %w", err)
	}
	return out, nil
}

// Refresh exchanges a refresh token for a fresh token pair.
func (c *UserClient) Refresh(refreshToken string) (TokenPair, error) {
	var out TokenPair
	if err := c.post("/auth/refresh", "", map[string]string{"refresh_token": refreshToken}, &out); err != nil {
		return TokenPair{}, fmt.Errorf("refresh: %w", err)
	}
	return out, nil
}

// Subscribe starts a subscription to plan for the token's user.
func (c *UserClient) Subscribe(token, plan string) (SubscribeResult, error) {
	var out SubscribeResult
	if err := c.post("/subscriptions", token, map[string]string{"plan": plan}, &out); err != nil {
		return SubscribeResult{}, fmt.Errorf("subscribe: %w", err)
	}
	return out, nil
}

// CurrentSubscription returns the token user's current subscription state.
func (c *UserClient) CurrentSubscription(token string) (CurrentSubscription, error) {
	var out CurrentSubscription
	if err := c.get("/subscriptions/me", token, &out); err != nil {
		return CurrentSubscription{}, fmt.Errorf("subscriptions/me: %w", err)
	}
	return out, nil
}

// MockCheckout simulates the hosted payment page (PAYMENT_PROVIDER=mock).
// outcome is "paid" (default) or "failed". Returns the HTTP status code.
func (c *UserClient) MockCheckout(ref, outcome string) (int, error) {
	u := fmt.Sprintf("%s/mock/checkout?ref=%s", c.BaseURL, url.QueryEscape(ref))
	if outcome != "" {
		u += "&outcome=" + url.QueryEscape(outcome)
	}
	resp, err := c.HTTPClient.Get(u)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	return resp.StatusCode, nil
}

// PostWebhook posts a raw webhook body with the given signature / event-id
// headers. Used to assert signature rejection without knowing the shared secret.
// Returns the HTTP status code.
func (c *UserClient) PostWebhook(body []byte, signature, eventID string) (int, error) {
	req, err := http.NewRequest(http.MethodPost, c.BaseURL+"/webhooks/payment", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Razorpay-Signature", signature)
	if eventID != "" {
		req.Header.Set("X-Razorpay-Event-Id", eventID)
	}
	resp, err := c.HTTPClient.Do(req)
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

	if err := c.Register(email, pw); err != nil {
		return "", fmt.Errorf("register: %w", err)
	}
	pair, err := c.Login(email, pw)
	if err != nil {
		return "", err
	}

	sub, err := c.Subscribe(pair.AccessToken, "premium")
	if err != nil {
		return "", err
	}
	ref, err := RefFromPaymentURL(sub.PaymentURL)
	if err != nil {
		return "", err
	}

	// Simulate the hosted payment → webhook activates the subscription.
	if code, err := c.MockCheckout(ref, "paid"); err != nil {
		return "", fmt.Errorf("mock checkout: %w", err)
	} else if code != http.StatusOK {
		return "", fmt.Errorf("mock checkout: status %d", code)
	}

	// Re-login for a token carrying the active entitlement.
	pair, err = c.Login(email, pw)
	if err != nil {
		return "", err
	}
	return pair.AccessToken, nil
}

func (c *UserClient) get(path, token string, out any) error {
	req, err := http.NewRequest(http.MethodGet, c.BaseURL+path, nil)
	if err != nil {
		return err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return c.do(req, path, out)
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
	return c.do(req, path, out)
}

func (c *UserClient) do(req *http.Request, path string, out any) error {
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

// RefFromPaymentURL extracts the mock provider's reference id (the `ref` query
// param) from a hosted payment URL.
func RefFromPaymentURL(raw string) (string, error) {
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
