package cmd

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

func TestSplitNameStripsEmailTokens(t *testing.T) {
	cases := []struct {
		in             string
		wantFirst      string
		wantLast       string
	}{
		{"Judith Saragossi", "Judith", "Saragossi"},
		{"judithsaragossi@gmail.com", "Member", ""},
		{"Judith judithsaragossi@gmail.com", "Judith", ""},
		{"   judithsaragossi@gmail.com   ", "Member", ""},
		{"", "Member", ""},
		{"Jean-Luc Picard", "Jean-Luc", "Picard"},
	}
	for _, c := range cases {
		name := c.in
		f, l := splitName(&name)
		if f != c.wantFirst || l != c.wantLast {
			t.Errorf("splitName(%q) = (%q, %q); want (%q, %q)", c.in, f, l, c.wantFirst, c.wantLast)
		}
	}
	f, l := splitName(nil)
	if f != "Member" || l != "" {
		t.Errorf("splitName(nil) = (%q, %q); want (Member, \"\")", f, l)
	}
}

func TestScrubNameFieldsRewritesAtInNameValues(t *testing.T) {
	in := []byte(`{
  "firstName": "alice@example.com",
  "lastName":  "bob.jones@example.com",
  "name":      "Alice Wonderland",
  "nested":    {"firstName": "no@t allowed"},
  "list":      [{"firstName": "only@email.com", "amount": 10}]
}`)
	out, scrubbed := scrubNameFields(in)
	if len(scrubbed) != 4 {
		t.Fatalf("expected 4 scrubs, got %d: %+v", len(scrubbed), scrubbed)
	}
	var parsed map[string]interface{}
	if err := json.Unmarshal(out, &parsed); err != nil {
		t.Fatalf("result is not valid JSON: %v", err)
	}
	if parsed["firstName"] != "Member" {
		t.Errorf("expected firstName to be Member, got %v", parsed["firstName"])
	}
	if parsed["lastName"] != "" {
		t.Errorf("expected lastName cleared, got %v", parsed["lastName"])
	}
	if parsed["name"] != "Alice Wonderland" {
		t.Errorf("expected name unchanged, got %v", parsed["name"])
	}
	if s := string(out); strings.Contains(s, "@") {
		t.Errorf("output still contains '@': %s", s)
	}
}

func TestValidatePublicJSONDetectsNameAtAndEmail(t *testing.T) {
	in := []byte(`{"firstName":"x@y.com","contact":"mail me at bob@example.com","event":"evt@events.lu.ma"}`)
	hard, soft := validatePublicJSON(in)
	if len(hard) == 0 {
		t.Fatalf("expected hard violation for firstName with @")
	}
	if len(soft) == 0 {
		t.Fatalf("expected soft violation for email in contact")
	}
}

func TestPathHasPrivateSegment(t *testing.T) {
	cases := []struct {
		path string
		want bool
	}{
		{"/data/2025/01/finance/stripe/private/customers.json", true},
		{"/data/2025/01/finance/stripe/subscriptions.json", false},
		{filepath.Join("data", "private", "x.json"), true},
		{"private/x.json", true},
		{"xprivate/x.json", false},
	}
	for _, c := range cases {
		if got := pathHasPrivateSegment(c.path); got != c.want {
			t.Errorf("pathHasPrivateSegment(%q) = %v; want %v", c.path, got, c.want)
		}
	}
}

func TestSoftAllowlistSuppressesLumaAndGoogleCalendarIDs(t *testing.T) {
	payload := []byte(`{
  "events": [
    {
      "id": "evt-abc@events.lu.ma",
      "coverImageLocal": "evt-abc@events.lu.ma.jpg",
      "extra": "real-email-here hello@example.com"
    },
    {
      "id": "1234@google.com",
      "coverImageLocal": "1234@google.com.png"
    }
  ]
}`)
	_, soft := validatePublicJSON(payload)
	// The only soft violation should be the "extra" field with a real email.
	if len(soft) != 1 {
		t.Fatalf("expected 1 soft violation, got %d: %+v", len(soft), soft)
	}
	if !strings.Contains(soft[0].Field, "extra") {
		t.Errorf("expected violation on 'extra', got %q", soft[0].Field)
	}
}

func TestSoftAllowlistDoesNotSuppressRealEmailsInAllowlistedField(t *testing.T) {
	// A real email in id (not matching the allowlist pattern) should still warn.
	payload := []byte(`{"id": "actual@person.com"}`)
	_, soft := validatePublicJSON(payload)
	if len(soft) != 1 {
		t.Fatalf("expected 1 soft violation for real email in id, got %d", len(soft))
	}
}

func TestEnforcePIIPolicyScrubsOnlyOutsidePrivate(t *testing.T) {
	data := []byte(`{"firstName":"alice@example.com"}`)
	cleaned := enforcePIIPolicy("/tmp/data/2025/01/generated/members.json", data)
	if strings.Contains(string(cleaned), "@") {
		t.Errorf("expected '@' scrubbed in public path, got %s", cleaned)
	}
	privCleaned := enforcePIIPolicy("/tmp/data/2025/01/finance/stripe/private/customers.json", data)
	if !strings.Contains(string(privCleaned), "@") {
		t.Errorf("expected private path to be untouched, got %s", privCleaned)
	}
}
