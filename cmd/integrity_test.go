package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func seedMonth(t *testing.T, dataDir, year, month, cachedAt, orderKey string) {
	p := filepath.Join(dataDir, year, month, "providers")
	writeFile(t, filepath.Join(p, "stripe", "balance-transactions.json"),
		`{"cachedAt":"`+cachedAt+`","accountId":"acct_1","transactions":[{"id":"txn_1","net":100},{"id":"txn_2","net":50}]}`)
	writeFile(t, filepath.Join(p, "discord", "123", "messages.json"), `{"channelId":"123","cachedAt":"`+cachedAt+`","messages":[{"id":"1"},{"id":"2"},{"id":"3"}]}`)
	writeFile(t, filepath.Join(p, "discord", "images", "a.jpg"), "jpgbytes")
	writeFile(t, filepath.Join(p, "odoo", "commonshub", "journals", "44.json"), `{"fetchedAt":"`+cachedAt+`","journalId":44,"count":2,"lines":[{"id":1},{"id":2}]}`)
	writeFile(t, filepath.Join(p, "odoo", "commonshub", "invoices.json"), `{"fetchedAt":"`+cachedAt+`","invoices":[{"id":9}]}`)
	writeFile(t, filepath.Join(p, "odoo", "commonshub", "pending", "transactions.json"), `{"generatedAt":"`+cachedAt+`","entries":{"`+orderKey+`":{}}}`)
	writeFile(t, filepath.Join(p, "ics", "coworking.ics"), "BEGIN:VCALENDAR\nBEGIN:VEVENT\nEND:VEVENT\nBEGIN:VEVENT\nEND:VEVENT\nEND:VCALENDAR\n")
}

func TestCanonicalJSONIgnoresFetchTimestampsAndKeyOrder(t *testing.T) {
	a, _ := canonicalJSON([]byte(`{"transactions":[{"net":1,"id":"x"}],"cachedAt":"2026-01-01T00:00:00Z","b":2,"a":1}`))
	b, _ := canonicalJSON([]byte(`{"a":1,"b":2,"cachedAt":"2026-09-09T09:09:09Z","transactions":[{"id":"x","net":1}]}`))
	if string(a) != string(b) {
		t.Errorf("canonical forms differ:\n%s\n%s", a, b)
	}
	if strings.Contains(string(a), "cachedAt") {
		t.Error("volatile key survived")
	}
	// Large integers keep their exact text.
	c, _ := canonicalJSON([]byte(`{"id":1306678821751230514}`))
	if string(c) != `{"id":1306678821751230514}` {
		t.Errorf("number precision lost: %s", c)
	}
	if _, ok := canonicalJSON([]byte("BEGIN:VCALENDAR")); ok {
		t.Error("non-JSON must report ok=false")
	}
}

func TestMonthIntegrityIsInstanceIndependent(t *testing.T) {
	a := t.TempDir()
	b := t.TempDir()
	seedMonth(t, a, "2026", "08", "2026-09-01T10:00:00Z", "iban:x:tx:1")
	seedMonth(t, b, "2026", "08", "2026-09-15T22:30:00Z", "iban:y:tx:2") // different fetch times, different outbox
	ma, err := computeMonthIntegrity(a, "2026", "08")
	if err != nil {
		t.Fatal(err)
	}
	mb, err := computeMonthIntegrity(b, "2026", "08")
	if err != nil {
		t.Fatal(err)
	}
	if ma.Hash != mb.Hash {
		t.Errorf("same data, different hashes: %s vs %s", ma.Hash, mb.Hash)
	}
	if ma.Providers != 4 {
		t.Errorf("providers = %d, want 4 (discord, ics, odoo/commonshub, stripe)", ma.Providers)
	}
	by := map[string]ProviderIntegrity{}
	for _, e := range ma.Entries {
		by[e.Provider] = e
	}
	if s := by["stripe"].Summary; s != "2 transactions" {
		t.Errorf("stripe summary = %q", s)
	}
	if s := by["discord"].Summary; s != "1 channels, 3 messages, 1 attachments" {
		t.Errorf("discord summary = %q", s)
	}
	if s := by["odoo/commonshub"].Summary; s != "1 journals, 2 lines, 1 invoices" {
		t.Errorf("odoo summary = %q", s)
	}
	if s := by["ics"].Summary; s != "1 calendars, 2 events" {
		t.Errorf("ics summary = %q", s)
	}
	if by["odoo/commonshub"].Files != 2 {
		t.Errorf("pending/ must be excluded from the odoo unit: files = %d", by["odoo/commonshub"].Files)
	}

	// A real data change changes the provider hash and the month hash.
	writeFile(t, filepath.Join(b, "2026", "08", "providers", "stripe", "balance-transactions.json"),
		`{"cachedAt":"x","accountId":"acct_1","transactions":[{"id":"txn_1","net":100}]}`)
	mb2, _ := computeMonthIntegrity(b, "2026", "08")
	if mb2.Hash == mb.Hash {
		t.Error("removing a transaction must change the month hash")
	}
	for _, e := range mb2.Entries {
		if e.Provider == "discord" && e.Hash != by["discord"].Hash {
			t.Error("an untouched provider keeps its hash")
		}
	}
}

func TestGenerateIntegrityOnlyCompletedAndStaleMonths(t *testing.T) {
	dataDir := t.TempDir()
	t.Setenv("DATA_DIR", dataDir)
	seedMonth(t, dataDir, "2024", "03", "2024-04-01T00:00:00Z", "k")
	current := nowBrusselsYearMonth()
	seedMonth(t, dataDir, current[:4], current[5:], "x", "k") // the running month: never hashed

	n, err := generateIntegrity(dataDir, "", false)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("hashed %d months, want 1 (only the completed one)", n)
	}
	for _, tier := range []string{"public", "members", "stewards"} {
		if _, err := os.Stat(filepath.Join(dataDir, "2024", "03", tier, integrityFile)); err != nil {
			t.Errorf("%s/integrity.json missing", tier)
		}
	}
	if _, err := os.Stat(filepath.Join(dataDir, current[:4], current[5:], "public", integrityFile)); err == nil {
		t.Error("the current month must not get a manifest")
	}
	idx, err := os.ReadFile(filepath.Join(dataDir, "latest", "public", integrityFile))
	if err != nil {
		t.Fatal("index missing")
	}
	var index IntegrityIndexFile
	if json.Unmarshal(idx, &index) != nil || len(index.Months) != 1 || index.Months[0].Month != "2024-03" {
		t.Errorf("index = %s", idx)
	}

	// Second run: nothing stale, nothing rehashed.
	if n, _ := generateIntegrity(dataDir, "", false); n != 0 {
		t.Errorf("unchanged data rehashed %d months", n)
	}
	// A provider file newer than the manifest makes the month stale.
	p := filepath.Join(dataDir, "2024", "03", "providers", "stripe", "balance-transactions.json")
	writeFile(t, p, `{"transactions":[]}`)
	future := os.Getpid() // any positive delta; set mtime well after the manifest
	_ = future
	if err := os.Chtimes(p, timeNowPlus(2), timeNowPlus(2)); err != nil {
		t.Fatal(err)
	}
	if n, _ := generateIntegrity(dataDir, "", false); n != 1 {
		t.Errorf("a newer provider file must trigger a rehash, got %d", n)
	}
}
