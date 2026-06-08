package payments

import (
	"testing"
	"time"

	"github.com/yourusername/videostreamingplatform-e2e/client"
	"github.com/yourusername/videostreamingplatform-e2e/testutil"
)

// TestSubscription_PremiumLifecycle walks the full paid path: a fresh user is not
// entitled, subscribing to premium yields a PENDING_PAYMENT subscription with a
// hosted payment URL (no entitlement yet), and only the captured webhook (fired
// by the mock checkout) flips the subscription to ACTIVE and the re-issued token
// to entitled.
func TestSubscription_PremiumLifecycle(t *testing.T) {
	env := testutil.NewEnv(t)
	env.RequireUser(t)

	email, pair := registerAndLogin(t, env)
	if entitled(t, pair.AccessToken) {
		t.Fatal("fresh user token should not be entitled")
	}

	me, err := env.User.CurrentSubscription(pair.AccessToken)
	if err != nil {
		t.Fatalf("CurrentSubscription (before): %v", err)
	}
	if me.Active {
		t.Fatal("fresh user should have no active subscription")
	}

	sub, err := env.User.Subscribe(pair.AccessToken, "premium")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if sub.Status != "PENDING_PAYMENT" {
		t.Errorf("subscribe status = %q, want PENDING_PAYMENT", sub.Status)
	}
	if sub.PaymentURL == "" {
		t.Fatal("premium subscribe should return a hosted payment URL")
	}
	if sub.AlreadyActive {
		t.Error("brand-new subscription should not report already_active")
	}

	// Still no entitlement until the payment is captured.
	if me, _ := env.User.CurrentSubscription(pair.AccessToken); me.Active {
		t.Fatal("subscription should not be active before payment capture")
	}

	ref, err := client.RefFromPaymentURL(sub.PaymentURL)
	if err != nil {
		t.Fatalf("RefFromPaymentURL: %v", err)
	}
	if code, err := env.User.MockCheckout(ref, "paid"); err != nil || code != 200 {
		t.Fatalf("MockCheckout(paid): code=%d err=%v", code, err)
	}

	me, err = env.User.CurrentSubscription(pair.AccessToken)
	if err != nil {
		t.Fatalf("CurrentSubscription (after): %v", err)
	}
	if !me.Active || me.Subscription == nil || me.Subscription.Status != "ACTIVE" {
		t.Fatalf("subscription not active after capture: %+v", me)
	}
	if me.Subscription.CurrentPeriodEnd == nil {
		t.Fatal("active subscription should have a current_period_end")
	}
	// Premium grants a 30-day window; allow a generous tolerance for clock skew.
	if d := time.Until(*me.Subscription.CurrentPeriodEnd); d < 29*24*time.Hour || d > 31*24*time.Hour {
		t.Errorf("current_period_end %v is not ~30 days out (delta %v)", me.Subscription.CurrentPeriodEnd, d)
	}

	// A freshly minted token now carries the paid entitlement.
	entitledPair, err := env.User.Login(email, testPassword)
	if err != nil {
		t.Fatalf("re-login: %v", err)
	}
	if !entitled(t, entitledPair.AccessToken) {
		t.Error("token issued after payment should be entitled")
	}
	if plan, _ := tokenClaims(t, entitledPair.AccessToken)["plan"].(string); plan != "premium" {
		t.Errorf("token plan claim = %q, want premium", plan)
	}
}

// TestSubscription_IdempotentSubscribe verifies the "one open subscription per
// user+plan" guard: subscribing twice before payment reuses the same pending
// link, and subscribing again after activation reports already_active rather than
// creating a duplicate.
func TestSubscription_IdempotentSubscribe(t *testing.T) {
	env := testutil.NewEnv(t)
	env.RequireUser(t)

	_, pair := registerAndLogin(t, env)
	token := pair.AccessToken

	first, err := env.User.Subscribe(token, "premium")
	if err != nil {
		t.Fatalf("Subscribe (first): %v", err)
	}
	second, err := env.User.Subscribe(token, "premium")
	if err != nil {
		t.Fatalf("Subscribe (second): %v", err)
	}
	if second.SubscriptionID != first.SubscriptionID {
		t.Errorf("repeated subscribe created a new subscription: %s != %s", second.SubscriptionID, first.SubscriptionID)
	}
	if second.PaymentURL != first.PaymentURL {
		t.Errorf("repeated pending subscribe returned a different payment URL")
	}

	ref, err := client.RefFromPaymentURL(first.PaymentURL)
	if err != nil {
		t.Fatalf("RefFromPaymentURL: %v", err)
	}
	if code, err := env.User.MockCheckout(ref, "paid"); err != nil || code != 200 {
		t.Fatalf("MockCheckout(paid): code=%d err=%v", code, err)
	}

	third, err := env.User.Subscribe(token, "premium")
	if err != nil {
		t.Fatalf("Subscribe (after active): %v", err)
	}
	if !third.AlreadyActive {
		t.Errorf("subscribe on an active plan should report already_active=true, got %+v", third)
	}
	if third.SubscriptionID != first.SubscriptionID {
		t.Errorf("active subscribe returned a different subscription id")
	}
}

// TestSubscription_FreePlanActivatesImmediately verifies a zero-price plan is
// activated without a payment step, yet grants no *paid* entitlement — the
// distinction the download paywall relies on.
func TestSubscription_FreePlanActivatesImmediately(t *testing.T) {
	env := testutil.NewEnv(t)
	env.RequireUser(t)

	email, pair := registerAndLogin(t, env)

	sub, err := env.User.Subscribe(pair.AccessToken, "free")
	if err != nil {
		t.Fatalf("Subscribe(free): %v", err)
	}
	if sub.PaymentURL != "" {
		t.Errorf("free plan should not require payment, got URL %q", sub.PaymentURL)
	}
	if sub.Status != "ACTIVE" {
		t.Errorf("free plan status = %q, want ACTIVE", sub.Status)
	}

	me, err := env.User.CurrentSubscription(pair.AccessToken)
	if err != nil {
		t.Fatalf("CurrentSubscription: %v", err)
	}
	if !me.Active {
		t.Error("free subscription should be active")
	}

	// Free plan is active but unentitled: a re-issued token must not be entitled.
	fresh, err := env.User.Login(email, testPassword)
	if err != nil {
		t.Fatalf("re-login: %v", err)
	}
	if entitled(t, fresh.AccessToken) {
		t.Error("free (zero-price) plan must not grant a paid entitlement")
	}
}
