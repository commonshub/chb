package cmd

import "testing"

func TestParseTransactionTagSpec(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"#ticket-sale", []string{"t", "ticket-sale"}},
		{"ticket_sale", []string{"t", "ticket-sale"}},
		{"#color:red", []string{"color", "red"}},
		{"#[url:https://example.com/a:b]", []string{"url", "https://example.com/a:b"}},
		{"#paymentLink:plink_123", []string{"paymentLink", "plink_123"}},
		{"#eventUrl:https://luma.com/event", []string{"eventUrl", "https://luma.com/event"}},
		{"#eventName:My Event", []string{"eventName", "My Event"}},
		{"#i:https://luma.com/event", []string{"i", "https://luma.com/event"}},
	}
	for _, c := range cases {
		got, ok := parseTransactionTagSpec(c.in)
		if !ok {
			t.Fatalf("parseTransactionTagSpec(%q) failed", c.in)
		}
		if len(got) != len(c.want) {
			t.Fatalf("parseTransactionTagSpec(%q) = %#v; want %#v", c.in, got, c.want)
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Fatalf("parseTransactionTagSpec(%q) = %#v; want %#v", c.in, got, c.want)
			}
		}
	}
}

func TestSyncTransactionTagsCanonicalizesFieldsAndMetadata(t *testing.T) {
	tx := TransactionEntry{
		Provider:   "stripe",
		Type:       "CREDIT",
		Category:   "Ticket Sales",
		Collective: "Open_Letter",
		Event:      "evt-2gc6B12TEyRNRqN",
		Metadata: map[string]interface{}{
			"application":         "Luma",
			"paymentLink":         "plink_1TGev1FAhaWeDyowQqEek3mT",
			"stripe_event_api_id": "evt-2gc6B12TEyRNRqN",
			"eventUrl":            "https://luma.com/my-event",
			"eventName":           "My Event",
			"product":             "Ticket",
			"color":               "Red",
			"email":               "person@example.com",
		},
	}

	syncTransactionTags(&tx)

	for _, query := range [][]string{
		{"category", "ticket-sales"},
		{"collective", "open-letter"},
		{"event", "evt-2gc6B12TEyRNRqN"},
		{"application", "luma"},
		{"paymentLink", "plink_1TGev1FAhaWeDyowQqEek3mT"},
		{"eventUrl", "https://luma.com/my-event"},
		{"eventName", "My Event"},
		{"t", "ticket"},
		{"color", "red"},
	} {
		if !transactionHasTag(tx, query) {
			t.Fatalf("expected tag %#v in %#v", query, tx.Tags)
		}
	}
	if transactionHasTag(tx, []string{"email", "person@example.com"}) {
		t.Fatalf("email metadata should not be converted to a public tag: %#v", tx.Tags)
	}
}

func TestSyncTransactionTagsEmitsSpreadAndDropsStaleSpread(t *testing.T) {
	tx := TransactionEntry{
		Provider: "stripe",
		Type:     "DEBIT",
		Tags: [][]string{
			{"spread", "2024-01", "100.00"}, // stale — should be dropped
			{"description", "kept"},
		},
		Spread: []SpreadEntry{
			{Month: "2025-01", Amount: "50.00"},
			{Month: "2025-02", Amount: "50.00"},
		},
	}
	syncTransactionTags(&tx)

	for _, query := range [][]string{
		{"spread", "2025-01", "50.00"},
		{"spread", "2025-02", "50.00"},
		{"description", "kept"},
	} {
		if !transactionHasTag(tx, query) {
			t.Fatalf("expected tag %#v in %#v", query, tx.Tags)
		}
	}
	for _, t2 := range tx.Tags {
		if len(t2) >= 2 && t2[0] == "spread" && t2[1] == "2024-01" {
			t.Fatalf("stale spread tag should have been dropped: %#v", tx.Tags)
		}
	}
}

func TestParseTxListFlagsAddsTagAliases(t *testing.T) {
	f, _, _, err := parseTxListFlags([]string{
		"--application", "luma",
		"--event", "evt-abc",
		"--payment-link", "plink_123",
		"--tag", "#color:red",
		"--tags", "ticket-sale,status:needs-review",
	})
	if err != nil {
		t.Fatal(err)
	}

	tx := TransactionEntry{
		Tags: [][]string{
			{"application", "luma"},
			{"event", "evt-abc"},
			{"paymentLink", "plink_123"},
			{"color", "red"},
			{"t", "ticket-sale"},
			{"status", "needs-review"},
		},
	}
	if !f.matches(tx) {
		t.Fatalf("expected filter %#v to match %#v", f.Tags, tx.Tags)
	}
}
