package cmd

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestLumaProcessorAddsEventURLTagAndCachesRedirect(t *testing.T) {
	dataDir := t.TempDir()
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Path != "/event/evt-abc" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		http.Redirect(w, r, "/my-real-event", http.StatusFound)
	}))
	defer server.Close()

	plugin := newLumaStripeProcessor()
	plugin.baseURL = server.URL + "/event/"
	ctx := newProcessorContext(dataDir, "2026", "04")
	ctx.HTTPClient = server.Client()

	if err := plugin.WarmUp(ctx); err != nil {
		t.Fatal(err)
	}
	tx := TransactionEntry{
		ID:    "stripe:txn_123",
		Event: "evt-abc",
	}
	if err := plugin.ProcessTransaction(ctx, &tx); err != nil {
		t.Fatal(err)
	}
	if !transactionHasTag(tx, []string{"eventUrl", server.URL + "/my-real-event"}) {
		t.Fatalf("eventUrl tag missing from %#v", tx.Tags)
	}
	if err := plugin.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	cachePath := filepath.Join(dataDir, "2026", "04", "processors", "luma-stripe", "event-urls.json")
	data, err := os.ReadFile(cachePath)
	if err != nil {
		t.Fatal(err)
	}
	var cache lumaStripeEventURLCache
	if err := json.Unmarshal(data, &cache); err != nil {
		t.Fatal(err)
	}
	if cache.EventURLs["evt-abc"] != server.URL+"/my-real-event" {
		t.Fatalf("unexpected cache: %#v", cache.EventURLs)
	}

	tx2 := TransactionEntry{ID: "stripe:txn_456", Event: "evt-abc"}
	if err := plugin.ProcessTransaction(ctx, &tx2); err != nil {
		t.Fatal(err)
	}
	if requests != 1 {
		t.Fatalf("expected cached second lookup, got %d requests", requests)
	}
}

func TestLumaProcessorSkipsExistingEventURLTag(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		t.Fatal("server should not be called when eventUrl tag exists")
	}))
	defer server.Close()

	plugin := newLumaStripeProcessor()
	plugin.baseURL = server.URL + "/event/"
	ctx := newProcessorContext(t.TempDir(), "2026", "04")
	ctx.HTTPClient = server.Client()
	if err := plugin.WarmUp(ctx); err != nil {
		t.Fatal(err)
	}

	tx := TransactionEntry{
		ID:    "stripe:txn_123",
		Event: "evt-abc",
		Tags:  [][]string{{"eventUrl", "https://luma.com/my-real-event"}},
	}
	if err := plugin.ProcessTransaction(ctx, &tx); err != nil {
		t.Fatal(err)
	}
	if requests != 0 {
		t.Fatalf("expected no request, got %d", requests)
	}
}

func TestLumaProcessorMapsExistingEventURLToCalendarEvent(t *testing.T) {
	dataDir := t.TempDir()
	writeTestLumaEventsFile(t, dataDir, "2026", "04", `{
	  "month": "2026-04",
	  "generatedAt": "2026-04-01T00:00:00Z",
	  "events": [{
	    "id": "calendar-uid-1",
	    "name": "Test Event",
	    "startAt": "2026-04-20T10:00:00Z",
	    "url": "https://luma.com/my-real-event",
	    "source": "calendar",
	    "metadata": {}
	  }]
	}`)

	plugin := newLumaStripeProcessor()
	ctx := newProcessorContext(dataDir, "2026", "04")
	if err := plugin.WarmUp(ctx); err != nil {
		t.Fatal(err)
	}

	tx := TransactionEntry{
		ID:    "stripe:txn_123",
		Event: "evt-abc",
		Tags:  [][]string{{"eventUrl", "https://lu.ma/my-real-event"}},
	}
	if err := plugin.ProcessTransaction(ctx, &tx); err != nil {
		t.Fatal(err)
	}
	if tx.Event != "calendar-uid-1" {
		t.Fatalf("Event = %q, want calendar-uid-1", tx.Event)
	}
	if !transactionHasTag(tx, []string{"event", "calendar-uid-1"}) {
		t.Fatalf("canonical event tag missing from %#v", tx.Tags)
	}
	if !transactionHasTag(tx, []string{"lumaEvent", "evt-abc"}) {
		t.Fatalf("luma source event tag missing from %#v", tx.Tags)
	}
	if !transactionHasTag(tx, []string{"eventName", "Test Event"}) {
		t.Fatalf("eventName tag missing from %#v", tx.Tags)
	}
	if !transactionHasTag(tx, []string{"i", "https://lu.ma/my-real-event"}) {
		t.Fatalf("NIP-73 i tag missing from %#v", tx.Tags)
	}
	if transactionHasTag(tx, []string{"I", "https://lu.ma/my-real-event"}) || transactionHasTag(tx, []string{"K", "web"}) {
		t.Fatalf("root-scope NIP-22 tags should not be generated for transactions: %#v", tx.Tags)
	}
	if !transactionHasTag(tx, []string{"k", "web"}) {
		t.Fatalf("NIP-73 kind tags missing from %#v", tx.Tags)
	}
}

func TestLumaProcessorMapsResolvedEventURLToCalendarEvent(t *testing.T) {
	dataDir := t.TempDir()
	writeTestLumaEventsFile(t, dataDir, "2026", "04", `{
	  "month": "2026-04",
	  "generatedAt": "2026-04-01T00:00:00Z",
	  "events": [{
	    "id": "calendar-uid-2",
	    "name": "Test Event",
	    "startAt": "2026-04-20T10:00:00Z",
	    "url": "https://luma.com/my-real-event",
	    "source": "calendar",
	    "metadata": {}
	  }]
	}`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/event/evt-abc" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		http.Redirect(w, r, "https://luma.com/my-real-event", http.StatusFound)
	}))
	defer server.Close()

	plugin := newLumaStripeProcessor()
	plugin.baseURL = server.URL + "/event/"
	ctx := newProcessorContext(dataDir, "2026", "04")
	ctx.HTTPClient = server.Client()
	if err := plugin.WarmUp(ctx); err != nil {
		t.Fatal(err)
	}

	tx := TransactionEntry{ID: "stripe:txn_123", Event: "evt-abc"}
	if err := plugin.ProcessTransaction(ctx, &tx); err != nil {
		t.Fatal(err)
	}
	if tx.Event != "calendar-uid-2" {
		t.Fatalf("Event = %q, want calendar-uid-2", tx.Event)
	}
	if !transactionHasTag(tx, []string{"eventUrl", "https://luma.com/my-real-event"}) {
		t.Fatalf("eventUrl tag missing from %#v", tx.Tags)
	}
}

func TestLumaProcessorInfersEventFromCachedURLTitleAndCollective(t *testing.T) {
	dataDir := t.TempDir()
	writeTestLumaEventsFile(t, dataDir, "2026", "04", `{
	  "month": "2026-04",
	  "generatedAt": "2026-04-01T00:00:00Z",
	  "events": [{
	    "id": "evt-Ga7MtsEoQmF3FDR",
	    "name": "Block 26",
	    "startAt": "2026-04-20T10:00:00Z",
	    "url": "https://luma.com/block26",
	    "source": "calendar",
	    "metadata": {}
	  }]
	}`)
	writeTestLumaEventURLCache(t, dataDir, "latest", "", `{
	  "eventUrls": {
	    "evt-Ga7MtsEoQmF3FDR": "https://luma.com/block26"
	  }
	}`)

	plugin := newLumaStripeProcessor()
	ctx := newProcessorContext(dataDir, "2026", "04")
	if err := plugin.WarmUp(ctx); err != nil {
		t.Fatal(err)
	}

	for _, tt := range []struct {
		name string
		tx   TransactionEntry
	}{
		{
			name: "charge",
			tx: TransactionEntry{
				ID:         "stripe:txn_charge",
				Provider:   "stripe",
				Type:       "CREDIT",
				Category:   "tickets",
				Collective: "block26",
				Metadata: map[string]interface{}{
					"description": "Block 26",
					"category":    "charge",
				},
			},
		},
		{
			name: "refund",
			tx: TransactionEntry{
				ID:         "stripe:txn_refund",
				Provider:   "stripe",
				Type:       "DEBIT",
				Category:   "tickets",
				Collective: "block26",
				Metadata: map[string]interface{}{
					"description": "Block 26",
					"category":    "refund",
				},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tx := tt.tx
			if err := plugin.ProcessTransaction(ctx, &tx); err != nil {
				t.Fatal(err)
			}
			if tx.Application != "Luma" {
				t.Fatalf("Application = %q, want Luma", tx.Application)
			}
			if tx.Event != "evt-Ga7MtsEoQmF3FDR" {
				t.Fatalf("Event = %q, want evt-Ga7MtsEoQmF3FDR", tx.Event)
			}
			if got := stringMetadata(tx.Metadata, "application"); got != "Luma" {
				t.Fatalf("metadata.application = %q, want Luma", got)
			}
			if got := stringMetadata(tx.Metadata, "eventId"); got != "evt-Ga7MtsEoQmF3FDR" {
				t.Fatalf("metadata.eventId = %q", got)
			}
			if got := stringMetadata(tx.Metadata, "eventUrl"); got != "https://luma.com/block26" {
				t.Fatalf("metadata.eventUrl = %q", got)
			}
			if got := stringMetadata(tx.Metadata, "eventName"); got != "Block 26" {
				t.Fatalf("metadata.eventName = %q", got)
			}
			for _, tag := range [][]string{
				{"application", "luma"},
				{"eventId", "evt-Ga7MtsEoQmF3FDR"},
				{"eventUrl", "https://luma.com/block26"},
				{"category", "tickets"},
				{"collective", "block26"},
			} {
				if !transactionHasTag(tx, tag) {
					t.Fatalf("missing tag %v in %#v", tag, tx.Tags)
				}
			}
		})
	}
}

func TestLumaProcessorDoesNotInferStripeFeesOrTaxes(t *testing.T) {
	dataDir := t.TempDir()
	writeTestLumaEventsFile(t, dataDir, "2026", "04", `{
	  "month": "2026-04",
	  "generatedAt": "2026-04-01T00:00:00Z",
	  "events": [{
	    "id": "evt-Ga7MtsEoQmF3FDR",
	    "name": "Block 26",
	    "startAt": "2026-04-20T10:00:00Z",
	    "url": "https://luma.com/block26",
	    "source": "calendar",
	    "metadata": {}
	  }]
	}`)
	writeTestLumaEventURLCache(t, dataDir, "2026", "04", `{
	  "eventUrls": {
	    "evt-Ga7MtsEoQmF3FDR": "https://luma.com/block26"
	  }
	}`)

	plugin := newLumaStripeProcessor()
	ctx := newProcessorContext(dataDir, "2026", "04")
	if err := plugin.WarmUp(ctx); err != nil {
		t.Fatal(err)
	}

	for _, tx := range []TransactionEntry{
		{
			ID:       "stripe:fee",
			Provider: "stripe",
			Metadata: map[string]interface{}{
				"description": "Billing - Usage Fee",
				"category":    "fee",
			},
		},
		{
			ID:       "stripe:tax",
			Provider: "stripe",
			Metadata: map[string]interface{}{
				"description": "Automatic Taxes",
				"category":    "tax",
			},
		},
	} {
		if err := plugin.ProcessTransaction(ctx, &tx); err != nil {
			t.Fatal(err)
		}
		if tx.Application != "" || tx.Event != "" {
			t.Fatalf("fee/tax transaction was marked as Luma: %#v", tx)
		}
	}
}

func TestGenerateTransactionsMergesStripeChargeDataEvenWithCustomerData(t *testing.T) {
	dataDir := t.TempDir()
	sourceDir := filepath.Join(dataDir, "2026", "04", "sources", "stripe")
	if err := os.MkdirAll(sourceDir, 0755); err != nil {
		t.Fatal(err)
	}
	writeTestLumaEventsFile(t, dataDir, "2026", "04", `{
	  "month": "2026-04",
	  "generatedAt": "2026-04-01T00:00:00Z",
	  "events": [{
	    "id": "evt-Ga7MtsEoQmF3FDR",
	    "name": "Block 26",
	    "startAt": "2026-04-20T10:00:00Z",
	    "url": "https://luma.com/block26",
	    "source": "calendar",
	    "metadata": {}
	  }]
	}`)
	writeTestLumaEventURLCache(t, dataDir, "2026", "04", `{
	  "eventUrls": {
	    "evt-Ga7MtsEoQmF3FDR": "https://luma.com/block26"
	  }
	}`)

	stripeCache := `{
	  "accountId": "acct_test",
	  "currency": "eur",
	  "transactions": [{
	    "id": "txn_1",
	    "amount": 2500,
	    "net": 2400,
	    "fee": 100,
	    "currency": "eur",
	    "description": "Block 26",
	    "created": 1776427200,
	    "reporting_category": "charge",
	    "type": "charge",
	    "customerName": "Alice Example",
	    "customerEmail": "alice@example.org",
	    "source": {"id": "ch_1"}
	  }]
	}`
	if err := os.WriteFile(filepath.Join(sourceDir, "balance-transactions.json"), []byte(stripeCache), 0644); err != nil {
		t.Fatal(err)
	}
	charges := `{
	  "fetchedAt": "2026-04-01T00:00:00Z",
	  "charges": {
	    "ch_1": {
	      "id": "ch_1",
	      "applicationName": "Luma",
	      "metadata": {
	        "event_api_id": "evt-Ga7MtsEoQmF3FDR",
	        "event_name": "Block 26",
	        "collective": "block26"
	      }
	    }
	  }
	}`
	if err := os.WriteFile(filepath.Join(sourceDir, "charges.json"), []byte(charges), 0600); err != nil {
		t.Fatal(err)
	}

	if count := generateTransactionsGo(dataDir, "2026", "04", nil); count != 1 {
		t.Fatalf("generated %d transactions, want 1", count)
	}
	data, err := os.ReadFile(filepath.Join(dataDir, "2026", "04", "generated", "transactions.json"))
	if err != nil {
		t.Fatal(err)
	}
	var out TransactionsFile
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Transactions) != 1 {
		t.Fatalf("transactions len = %d, want 1", len(out.Transactions))
	}
	tx := out.Transactions[0]
	if tx.Application != "Luma" || tx.Event != "evt-Ga7MtsEoQmF3FDR" {
		t.Fatalf("missing Luma app/event: %#v", tx)
	}
	if tx.NetAmount != 24 || tx.GrossAmount != 25 || tx.Fee != 1 {
		t.Fatalf("unexpected amounts: net %.2f gross %.2f fee %.2f", tx.NetAmount, tx.GrossAmount, tx.Fee)
	}
	if got := stringMetadata(tx.Metadata, "eventUrl"); got != "https://luma.com/block26" {
		t.Fatalf("metadata.eventUrl = %q", got)
	}
	if !transactionHasTag(tx, []string{"application", "luma"}) || !transactionHasTag(tx, []string{"eventId", "evt-Ga7MtsEoQmF3FDR"}) {
		t.Fatalf("missing Luma tags: %#v", tx.Tags)
	}
}

func writeTestLumaEventsFile(t *testing.T, dataDir, year, month, payload string) {
	t.Helper()
	dir := filepath.Join(dataDir, year, month, "generated")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "events.json"), []byte(payload), 0644); err != nil {
		t.Fatal(err)
	}
}

func writeTestLumaEventURLCache(t *testing.T, dataDir, year, month, payload string) {
	t.Helper()
	parts := []string{dataDir, year}
	if month != "" {
		parts = append(parts, month)
	}
	parts = append(parts, "processors", "luma-stripe")
	dir := filepath.Join(parts...)
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "event-urls.json"), []byte(payload), 0644); err != nil {
		t.Fatal(err)
	}
}

func TestEventTicketTransactionAmountUsesStripeGrossAmount(t *testing.T) {
	charge := TransactionEntry{Provider: "stripe", Type: "CREDIT", GrossAmount: 25, NormalizedAmount: 24, Amount: 24}
	refund := TransactionEntry{Provider: "stripe", Type: "DEBIT", GrossAmount: 10, NormalizedAmount: -9.6, Amount: -9.6}
	if got := eventTicketTransactionAmount(charge); got != 25 {
		t.Fatalf("charge amount = %.2f, want 25.00", got)
	}
	if got := eventTicketTransactionAmount(refund); got != -10 {
		t.Fatalf("refund amount = %.2f, want -10.00", got)
	}
}
