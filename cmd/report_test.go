package cmd

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReportMonthlyUsesGeneratedReportJSON(t *testing.T) {
	dataDir := t.TempDir()
	t.Setenv("DATA_DIR", dataDir)
	writeJSONFixture(t, filepath.Join(dataDir, "2026", "04", "generated", "summary.json"), `{
	  "year": "2026",
	  "month": "04",
	  "summary": {"contributors": 3, "images": 5, "transactions": 2, "events": 1, "bookings": 4},
	  "currencies": [{"currency": "EUR", "transactions": 2, "in": 20, "out": 4, "fees": 1, "net": 15}],
	  "accounts": [{
	    "source": "stripe",
	    "accountSlug": "acct_test",
	    "accountName": "Stripe",
	    "currency": "EUR",
	    "counts": {"credits": 2},
	    "amounts": {"in": 20, "out": 0, "fees": 1, "net": 19},
	    "balance": {"delta": 19, "computed": false, "verified": false}
	  }],
	  "tokens": [{
	    "slug": "cht",
	    "name": "Commons Hub Token",
	    "symbol": "CHT",
	    "chain": "celo",
	    "mints": 1,
	    "burns": 1,
	    "transfers": 0,
	    "transactions": 2,
	    "minted": 42,
	    "burnt": 3,
	    "totalSupply": 1000,
	    "tokenHolders": 12,
	    "activeTokenHolders": 4,
	    "computedFromHistory": true
	  }],
	  "sources": [{
	    "source": "discord",
	    "records": 12,
	    "attachments": 4,
	    "summary": {"channels": 2, "images": 5}
	  }, {
	    "source": "ics",
	    "records": 4,
	    "summary": {"calendars": 1, "events": 1, "bookings": 4, "byCalendar": {"ostrom": {"events": 1, "bookings": 4}}}
	  }],
	  "notes": ["opening and ending balances omitted"]
	}`)

	out := captureStdout(t, func() {
		if err := Report([]string{"2026/04"}); err != nil {
			t.Fatalf("Report: %v", err)
		}
	})

	for _, want := range []string{
		"Report for April 2026",
		"👥 Contributors  3",
		"🪙 Tokens",
		"CHT      minted 42 CHT  burnt 3 CHT  txs 2  supply 1000 CHT  holders 12  active 4",
		"💶 Currencies",
		"EUR      total in 20.00 EUR  out 4.00 EUR  net 15.00 EUR",
		"🏦 Accounts",
		"Stripe  in 20.00 EUR  out 0.00 EUR  net 19.00 EUR  start n/a  end n/a",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("report output missing %q:\n%s", want, out)
		}
	}
}

func TestReportMonthlyAcceptsYYYYMM(t *testing.T) {
	dataDir := t.TempDir()
	t.Setenv("DATA_DIR", dataDir)
	writeJSONFixture(t, filepath.Join(dataDir, "2026", "04", "generated", "summary.json"), `{
	  "year": "2026",
	  "month": "04",
	  "summary": {"contributors": 1},
	  "accounts": [],
	  "sources": []
	}`)

	out := captureStdout(t, func() {
		if err := Report([]string{"202604"}); err != nil {
			t.Fatalf("Report: %v", err)
		}
	})

	if !strings.Contains(out, "Report for April 2026") {
		t.Fatalf("report output missing monthly title:\n%s", out)
	}
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	orig := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = w
	defer func() {
		os.Stdout = orig
	}()

	fn()

	if err := w.Close(); err != nil {
		t.Fatalf("close stdout pipe: %v", err)
	}
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read stdout pipe: %v", err)
	}
	return string(data)
}
