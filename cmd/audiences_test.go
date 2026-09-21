package cmd

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
)

func TestAudienceOfPath(t *testing.T) {
	cases := map[string]string{
		"2026/09/public/transactions.json":          "public",
		"2026/09/members/door.json":                 "members",
		"2026/09/stewards/transactions.json":        "stewards",
		"latest/members/transactions.json":          "members",
		"2026/public/events.csv":                    "public",
		"2026/09/generated/transactions.json":       "",
		"2026/09/providers/stripe/public.json":      "", // "public" not directly under the month
		"2026/09/generated/private/enrichment.json": "",
	}
	for path, want := range cases {
		a, ok := audienceOfPath(path)
		if (want == "") != !ok || (ok && string(a) != want) {
			t.Errorf("audienceOfPath(%q) = %q,%v want %q", path, a, ok, want)
		}
	}
}

func TestIBANChecksum(t *testing.T) {
	if !ibanChecksumValid("GB82WEST12345698765432") || !ibanChecksumValid("BE68539007547034") {
		t.Error("valid IBANs must verify")
	}
	if ibanChecksumValid("GB82WEST12345698765433") || ibanChecksumValid("BE00539007547034") {
		t.Error("a wrong check digit must not verify")
	}
	// Shapes that look like IBANs but aren't must not trip the policy.
	if got := findIBANs([]byte(`{"hash":"AB12CDEF0123456789012345","sku":"FR76ABCDEFGHIJKLMNOPQ"}`)); len(got) != 0 {
		t.Errorf("false positives: %v", got)
	}
}

func TestEnforceAudiencePolicy(t *testing.T) {
	ownIBANsForTest = map[string]bool{"BE46734072238636": true}
	t.Cleanup(func() { ownIBANsForTest = nil })

	withEmail := []byte(`{"transactions":[{"metadata":{"memo":"paid by someone@example.com, thanks"}}]}`)
	withIBAN := []byte(`{"enrichments":{"x":{"iban":"BE68539007547034"}}}`)
	withOwnIBAN := []byte(`{"accounts":[{"id":"iban:be46734072238636","iban":"BE46734072238636"}]}`)
	withName := []byte(`{"transactions":[{"counterparty":"Jane Doe","metadata":{"memo":"rent"}}]}`)
	nameHasAt := []byte(`{"author":{"displayName":"jane@example.com","username":"jane"}}`)
	ids := []byte(`{"id":"4lk13p4bfk66fhaj4humbbthvu@google.com","event":"evt-abc@events.lu.ma","contact":"hello@commonshub.brussels"}`)

	for _, a := range []Audience{AudiencePublic, AudienceMembers} {
		out, err := enforceAudiencePolicy(a, "transactions.json", withEmail)
		if err != nil || strings.Contains(string(out), "someone@") || !strings.Contains(string(out), emailMask) {
			t.Errorf("%s: an email in free text is masked, not refused: out=%s err=%v", a, out, err)
		}
		if _, err := enforceAudiencePolicy(a, "x.json", withIBAN); !errors.Is(err, ErrAudiencePolicy) {
			t.Errorf("%s must refuse a third-party IBAN, got %v", a, err)
		}
		if _, err := enforceAudiencePolicy(a, "x.json", withOwnIBAN); err != nil {
			t.Errorf("%s: our own account IBAN is not personal data: %v", a, err)
		}
		out, err = enforceAudiencePolicy(a, "x.json", nameHasAt)
		if err != nil || strings.Contains(string(out), "jane@") {
			t.Errorf("%s: an email-shaped displayName is scrubbed: out=%s err=%v", a, out, err)
		}
		out, err = enforceAudiencePolicy(a, "x.json", ids)
		if err != nil || string(out) != string(ids) {
			t.Errorf("%s: calendar ids, Luma ids and role mailboxes are kept verbatim: out=%s err=%v", a, out, err)
		}
	}
	if out, err := enforceAudiencePolicy(AudienceMembers, "x.json", withName); err != nil || !strings.Contains(string(out), "Jane Doe") {
		t.Errorf("members may carry names: out=%s err=%v", out, err)
	}
	if out, err := enforceAudiencePolicy(AudienceStewards, "x.json", withIBAN); err != nil || string(out) != string(withIBAN) {
		t.Errorf("stewards is passthrough: out=%s err=%v", out, err)
	}
	// Non-JSON payloads: emails masked, third-party IBANs refused.
	if out, err := enforceAudiencePolicy(AudiencePublic, "rooms.md", []byte("Contact hello@commonshub.brussels or jane@example.com\n")); err != nil || strings.Contains(string(out), "jane@") || !strings.Contains(string(out), "hello@commonshub.brussels") {
		t.Errorf("markdown: out=%s err=%v", out, err)
	}
	if _, err := enforceAudiencePolicy(AudiencePublic, "events.csv", []byte("Host,IBAN\nJane,BE68539007547034\n")); !errors.Is(err, ErrAudiencePolicy) {
		t.Errorf("csv with a third-party IBAN must be refused for public, got %v", err)
	}
}

func TestWriteAudienceFileModesAndMirror(t *testing.T) {
	dataDir := t.TempDir()
	t.Setenv("DATA_DIR", dataDir)

	if err := writeAudienceFile(dataDir, "2026", "09", AudienceStewards, "transactions.json", []byte(`{"iban":"BE68539007547034"}`)); err != nil {
		t.Fatal(err)
	}
	if err := writeAudienceFile(dataDir, "2026", "09", AudienceMembers, "door.json", []byte(`{"openers":[{"name":"Jane"}]}`)); err != nil {
		t.Fatal(err)
	}
	if err := writeAudienceFile(dataDir, "2026", "09", AudiencePublic, "door.json", []byte(`{"totalOpens":3}`)); err != nil {
		t.Fatal(err)
	}
	// A public write with an IBAN is refused and leaves no file behind.
	err := writeAudienceFile(dataDir, "2026", "09", AudiencePublic, "leak.json", []byte(`{"iban":"BE68539007547034"}`))
	if !errors.Is(err, ErrAudiencePolicy) {
		t.Fatalf("want policy error, got %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(dataDir, "2026", "09", "public", "leak.json")); statErr == nil {
		t.Error("refused write must not leave a file")
	}

	mode := func(p string) os.FileMode {
		info, err := os.Stat(filepath.Join(dataDir, p))
		if err != nil {
			t.Fatalf("%s: %v", p, err)
		}
		return info.Mode().Perm()
	}
	if m := mode("2026/09/stewards"); m != 0o700 {
		t.Errorf("stewards dir = %o, want 700", m)
	}
	if m := mode("2026/09/stewards/transactions.json"); m != 0o600 {
		t.Errorf("stewards file = %o, want 600", m)
	}
	if m := mode("2026/09/members"); m != 0o750 {
		t.Errorf("members dir = %o, want 750", m)
	}
	if m := mode("2026/09/members/door.json"); m != 0o640 {
		t.Errorf("members file = %o, want 640", m)
	}
	if m := mode("2026/09/public"); m != 0o755 {
		t.Errorf("public dir = %o, want 755", m)
	}
	// latest/<tier> mirror exists with the same modes.
	if m := mode("latest/stewards"); m != 0o700 {
		t.Errorf("latest/stewards dir = %o, want 700", m)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "latest", "public", "door.json")); err != nil {
		t.Errorf("latest mirror missing: %v", err)
	}
	// The data-dir normaliser must keep tier modes, not reset them to 0755/0644.
	normalizedDataDirs = syncMapReset()
	normalizeDataDir(dataDir)
	if m := mode("2026/09/stewards/transactions.json"); m != 0o600 {
		t.Errorf("normaliser reset stewards file to %o", m)
	}
	if m := mode("2026/09/members"); m != 0o750 {
		t.Errorf("normaliser reset members dir to %o", m)
	}
}

func TestTransactionForAudience(t *testing.T) {
	tx := TransactionEntry{
		ID:               "iban:be46734072238636:tx:1",
		Provider:         "kbcbrussels",
		Counterparty:     "Jane Doe",
		TxHash:           "row-hash",
		StripeCustomerID: "cus_123",
		Amount:           121,
		Category:         "coworking",
		Metadata: map[string]interface{}{
			"memo":                 "coworking september",
			"fullDescription":      "VIREMENT DE JANE DOE",
			"reference":            "+++000/0044/88369+++",
			"iban":                 "BE68539007547034",
			"bic":                  "GEBABEBB",
			"stripe_receipt_email": "jane@example.com",
			"custom_Display name for the donors list": "Jane",
			"description": "coworking",
		},
	}

	st := transactionForAudience(tx, AudienceStewards)
	if st.Counterparty != "Jane Doe" || st.Metadata["iban"] != "BE68539007547034" || st.StripeCustomerID != "cus_123" {
		t.Errorf("stewards must keep everything: %+v", st)
	}

	emailCp := tx
	emailCp.Counterparty = "someone@example.com"
	if got := transactionForAudience(emailCp, AudienceMembers).Counterparty; got != "" {
		t.Errorf("members: an email standing in for a name must go, got %q", got)
	}

	me := transactionForAudience(tx, AudienceMembers)
	if me.Counterparty != "Jane Doe" || me.Metadata["fullDescription"] != "VIREMENT DE JANE DOE" || me.Metadata["memo"] != "coworking september" {
		t.Errorf("members keeps names and narration: %+v", me)
	}
	for _, k := range []string{"iban", "bic", "stripe_receipt_email"} {
		if _, ok := me.Metadata[k]; ok {
			t.Errorf("members must not carry %s", k)
		}
	}
	if me.StripeCustomerID != "" || me.TxHash != "" {
		t.Errorf("members must not carry provider ids: %+v", me)
	}

	pu := transactionForAudience(tx, AudiencePublic)
	if pu.Counterparty != "" {
		t.Error("public must not name the counterparty")
	}
	for _, k := range []string{"iban", "bic", "stripe_receipt_email", "fullDescription", "reference", "memo", "custom_Display name for the donors list"} {
		if _, ok := pu.Metadata[k]; ok {
			t.Errorf("public must not carry %s", k)
		}
	}
	if pu.Metadata["description"] != "coworking" || pu.Amount != 121 || pu.Category != "coworking" {
		t.Errorf("public keeps amounts, categories and the plain description: %+v", pu)
	}
	// The projection must not mutate the source.
	if _, ok := tx.Metadata["iban"]; !ok {
		t.Error("projection mutated the original metadata map")
	}
	// And the public projection passes the public policy.
	data, _ := json.Marshal(TransactionsFile{Transactions: []TransactionEntry{pu}})
	if _, err := enforceAudiencePolicy(AudiencePublic, "transactions.json", data); err != nil {
		t.Errorf("public projection rejected by public policy: %v", err)
	}
}

func TestDoorFileForAudience(t *testing.T) {
	full := DoorMonthFile{
		Month: "2026-08", TokenOpens: 2, TotalOpens: 7,
		Openers: []DoorOpener{
			{ID: "1", Username: "xdamman", Name: "Xavier", Days: 2, Opens: 4, Dates: []string{"2026-08-01", "2026-08-03"}},
			{Name: "Guest", Days: 1, Opens: 1, Dates: []string{"2026-08-03"}, Via: []string{"event"}},
		},
	}
	pub, ok := doorFileForAudience(full, AudiencePublic).(DoorPublicFile)
	if !ok || pub.Openers != 2 || pub.OpenDays != 2 || pub.TotalOpens != 7 || pub.TokenOpens != 2 {
		t.Errorf("public = %+v", pub)
	}
	mem := doorFileForAudience(full, AudienceMembers).(DoorMonthFile)
	if mem.Openers[0].Name != "Xavier" || mem.Openers[0].Days != 2 || mem.Openers[0].Dates != nil {
		t.Errorf("members keeps identity and counts, drops dates: %+v", mem.Openers[0])
	}
	if len(full.Openers[0].Dates) != 2 {
		t.Error("projection mutated the full file")
	}
	ste := doorFileForAudience(full, AudienceStewards).(DoorMonthFile)
	if len(ste.Openers[0].Dates) != 2 {
		t.Error("stewards keeps dates")
	}
}

func syncMapReset() sync.Map { return sync.Map{} }

func TestTierPathScope(t *testing.T) {
	cases := []struct {
		path, y, m string
		ok         bool
	}{
		{"d/2026/09/stewards/events.json", "2026", "09", true},
		{"d/2026/stewards/events.json", "2026", "", true},
		{"d/latest/stewards/events.json", "latest", "", true},
		{"d/2026/09/generated/events.json", "", "", false},
	}
	for _, c := range cases {
		y, m, ok := tierPathScope("d", c.path)
		if y != c.y || m != c.m || ok != c.ok {
			t.Errorf("tierPathScope(%s) = %q,%q,%v want %q,%q,%v", c.path, y, m, ok, c.y, c.m, c.ok)
		}
	}
}

func TestMembersTierGroupOwnership(t *testing.T) {
	dataDir := t.TempDir()
	t.Setenv("DATA_DIR", dataDir)
	// Use the caller's own primary gid: chown to it always succeeds, and it
	// proves the members/ dir and file get the configured group while the
	// other tiers are left alone.
	gid := os.Getgid()
	membersGIDForTest = &gid
	t.Cleanup(func() { membersGIDForTest = nil })

	if err := writeAudienceFile(dataDir, "2026", "09", AudienceMembers, "door.json", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if err := writeAudienceFile(dataDir, "2026", "09", AudiencePublic, "door.json", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"2026/09/members", "2026/09/members/door.json", "latest/members/door.json"} {
		info, err := os.Stat(filepath.Join(dataDir, p))
		if err != nil {
			t.Fatalf("%s: %v", p, err)
		}
		if got := int(info.Sys().(*syscall.Stat_t).Gid); got != gid {
			t.Errorf("%s gid = %d, want %d", p, got, gid)
		}
	}
	if membersGroupGIDFromEnv("") != -1 {
		t.Error("unset CHB_MEMBERS_GROUP must mean owner-only")
	}
	if membersGroupGIDFromEnv("1500") != 1500 {
		t.Error("numeric CHB_MEMBERS_GROUP is used as the gid")
	}
}
