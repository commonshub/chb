package cmd

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestBuildReminderPlan(t *testing.T) {
	members := []Member{
		{ID: "a", FirstName: "Ann", Status: "active"},
		{ID: "b", FirstName: "Bea", Status: "grace", StatusReason: "payment_failed", StatusSince: "2026-10-01", GraceEndsAt: "2026-10-16", OdooPartnerID: 7},
		{ID: "c", FirstName: "Cy", Status: "lapsed", StatusSince: "2026-09-20", StripeCustomerID: "cus_1"},
		{ID: "d", FirstName: "Di", Status: "lapsed", StatusSince: "2026-08-01"}, // more than 30 days ago
		{ID: "e", FirstName: "Ed", Status: "grace", StatusReason: "paused", StatusSince: "2026-10-03"},
	}
	plan := buildReminderPlan(members, "2026-10-06")
	keys := map[string]string{}
	for _, p := range plan {
		keys[p.Key] = p.Reason
	}
	want := map[string]string{"odoo:7:payment_failed:2026-10-01": "payment_failed", "stripe:cus_1:ended:2026-09-20": "ended", "e:paused:2026-10-03": "paused"}
	if len(keys) != len(want) {
		t.Fatalf("plan %v, want %v", keys, want)
	}
	for k, r := range want {
		if keys[k] != r {
			t.Errorf("missing %s (%s): %v", k, r, keys)
		}
	}
}

func TestRenewTokenAndRender(t *testing.T) {
	e := ReminderPlanEntry{Reason: "payment_failed", FirstName: `Zoë <b>&`, OdooPartnerID: 42, StripeCustomerID: "cus_9", GraceEndsAt: "2026-10-21", PaymentReference: "+++123/4567/89012+++"}
	now := time.Unix(1790000000, 0)
	tok := renewToken("s3cret", e, now)
	parts := strings.Split(tok, ".")
	if len(parts) != 2 {
		t.Fatalf("token %q", tok)
	}
	mac := hmac.New(sha256.New, []byte("s3cret"))
	mac.Write([]byte(parts[0]))
	if base64.RawURLEncoding.EncodeToString(mac.Sum(nil)) != parts[1] {
		t.Fatal("HMAC over the base64url JSON string")
	}
	raw, _ := base64.RawURLEncoding.DecodeString(parts[0])
	var p map[string]interface{}
	_ = json.Unmarshal(raw, &p)
	if p["n"] != `Zoë <b>&` || p["c"] != "cus_9" || p["p"].(float64) != 42 || int64(p["x"].(float64)) != now.Add(renewLinkValidity).Unix() || p["o"] != nil {
		t.Fatalf("payload %v", p)
	}
	m, err := renderReminder(e, "z@example.org", reminderRenewBase+tok)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(m.HTML, "<b>&") || !strings.Contains(m.HTML, "Zoë &lt;b&gt;&amp;") {
		t.Error("HTML values must be escaped")
	}
	if !strings.Contains(m.Text, "Wednesday 21 October") || !strings.Contains(m.Text, "+++123/4567/89012+++") {
		t.Errorf("text: %s", m.Text)
	}
	for _, reason := range []string{"payment_failed", "paused", "ended"} {
		for _, org := range []bool{false, true} {
			e.Reason, e.Organisation = reason, org
			if _, err := renderReminder(e, "z@example.org", "https://x"); err != nil {
				t.Errorf("%s org=%v: %v", reason, org, err)
			}
		}
	}
}

func TestRemindSendsOnce(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("DATA_DIR", filepath.Join(tmp, "data"))
	t.Setenv("APP_DATA_DIR", filepath.Join(tmp, "app"))
	t.Setenv("RESEND_API_KEY", "k")
	t.Setenv("RENEW_LINK_SECRET", "s")
	plan := ReminderPlanFile{Reminders: []ReminderPlanEntry{{Key: "x:payment_failed:2026-10-01", Reason: "payment_failed", FirstName: "X", StripeCustomerID: "cus_x"}}}
	if err := writePrivateJSON(reminderPlanPath(DataDir()), plan); err != nil {
		t.Fatal(err)
	}
	sends := 0
	oldSend := sendReminderEmail
	sendReminderEmail = func(apiKey, idem, to string, m reminderEmail) (string, error) { sends++; return "msg_1", nil }
	defer func() { sendReminderEmail = oldSend }()
	// Preview first (no --yes), then mark the entry sent in the ledger:
	// a --yes run must skip it (the live address lookup is not exercised).
	if err := MembersRemind(nil); err != nil {
		t.Fatal(err)
	}
	if sends != 0 {
		t.Fatal("preview must not send")
	}
	ledger := loadReminderLedger(DataDir())
	ledger["x:payment_failed:2026-10-01"] = ReminderLedgerEntry{SentAt: "now"}
	_ = writePrivateJSON(reminderLedgerPath(DataDir()), ledger)
	if err := MembersRemind([]string{"--yes"}); err != nil {
		t.Fatal(err)
	}
	if sends != 0 {
		t.Fatal("an already-sent reminder must not be sent again")
	}
}
