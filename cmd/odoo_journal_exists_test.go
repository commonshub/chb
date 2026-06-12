package cmd

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

// TestMissingOdooJournals verifies the pre-push existence check used by
// `chb sync` / `chb push`: journals returned by Odoo are reported as present
// (and their names cached), linked IDs absent from the response are flagged
// as missing so the push loop can skip them with a repair hint.
func TestMissingOdooJournals(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("APP_DATA_DIR", filepath.Join(tmp, "app"))
	t.Setenv("DATA_DIR", filepath.Join(tmp, "data"))

	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":[{"id":47,"name":"KBC"},{"id":53,"name":"Stripe"}]}`))
	}))
	defer server.Close()

	creds := &OdooCredentials{URL: server.URL, DB: "testdb", Login: "x", Password: "y"}
	missing, err := missingOdooJournals(creds, 2, []int{47, 53, 99})
	if err != nil {
		t.Fatalf("missingOdooJournals: %v", err)
	}
	if len(missing) != 1 || !missing[99] {
		t.Fatalf("missing = %v, want only 99", missing)
	}
	if calls != 1 {
		t.Fatalf("expected one batched RPC, got %d", calls)
	}
	if got := OdooJournalName(47); got != "KBC" {
		t.Fatalf("cached name for #47 = %q, want KBC", got)
	}

	// Empty input short-circuits without a network call.
	m, err := missingOdooJournals(creds, 2, nil)
	if err != nil || len(m) != 0 {
		t.Fatalf("empty input: missing=%v err=%v", m, err)
	}
	if calls != 1 {
		t.Fatalf("empty input hit the network (calls=%d)", calls)
	}
}
