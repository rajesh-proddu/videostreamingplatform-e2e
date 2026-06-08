package payments

import (
	"net/http"
	"testing"

	"github.com/yourusername/videostreamingplatform-e2e/client"
	"github.com/yourusername/videostreamingplatform-e2e/testutil"
)

// TestWebhook_InvalidSignature_Returns400 confirms the webhook endpoint rejects a
// body whose HMAC signature does not match. We send a well-formed Razorpay-shaped
// envelope so the rejection is unambiguously a signature failure (400), not a
// parse error — the handler verifies the signature over the raw bytes before
// parsing.
func TestWebhook_InvalidSignature_Returns400(t *testing.T) {
	env := testutil.NewEnv(t)
	env.RequireUser(t)

	body := []byte(`{"entity":"event","event":"payment_link.paid","payload":{"payment_link":{"entity":{"id":"plink_x","status":"paid","reference_id":"ref_x","amount":29900,"amount_paid":29900}},"payment":{"entity":{"id":"pay_x","status":"captured","order_id":"order_x","amount":29900}}},"created_at":0}`)

	code, err := env.User.PostWebhook(body, "deadbeefnotavalidsignature", "evt-invalid-sig")
	if err != nil {
		t.Fatalf("PostWebhook: %v", err)
	}
	if code != http.StatusBadRequest {
		t.Errorf("webhook with bad signature: status = %d, want 400", code)
	}
}

// TestPayment_FailedCheckout_NoEntitlement verifies a failed payment leaves the
// subscription PENDING_PAYMENT and grants no entitlement.
func TestPayment_FailedCheckout_NoEntitlement(t *testing.T) {
	env := testutil.NewEnv(t)
	env.RequireUser(t)

	email, pair := registerAndLogin(t, env)

	sub, err := env.User.Subscribe(pair.AccessToken, "premium")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	ref, err := client.RefFromPaymentURL(sub.PaymentURL)
	if err != nil {
		t.Fatalf("RefFromPaymentURL: %v", err)
	}

	// The mock checkout page returns 200 even on a failed outcome (it renders the
	// "payment failed" page); the failure is reflected in subscription state.
	if code, err := env.User.MockCheckout(ref, "failed"); err != nil || code != 200 {
		t.Fatalf("MockCheckout(failed): code=%d err=%v", code, err)
	}

	me, err := env.User.CurrentSubscription(pair.AccessToken)
	if err != nil {
		t.Fatalf("CurrentSubscription: %v", err)
	}
	if me.Active {
		t.Error("subscription should not be active after a failed payment")
	}

	fresh, err := env.User.Login(email, testPassword)
	if err != nil {
		t.Fatalf("re-login: %v", err)
	}
	if entitled(t, fresh.AccessToken) {
		t.Error("token should not be entitled after a failed payment")
	}
}
