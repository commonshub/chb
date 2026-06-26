package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	stripesource "github.com/CommonsHub/chb/providers/stripe"
)

// TestStripeBTIsReversal locks in which BT types reverse a charge (and so must
// inherit that charge's account, booked negative) versus fee adjustments, which
// belong on the Stripe-fees account.
func TestStripeBTIsReversal(t *testing.T) {
	cases := []struct {
		name string
		bt   stripesource.Transaction
		want bool
	}{
		{"card refund", stripesource.Transaction{Type: "refund"}, true},
		{"bank payment_refund", stripesource.Transaction{Type: "payment_refund"}, true},
		{"chargeback du_ source", stripesource.Transaction{Type: "adjustment", Source: json.RawMessage(`"du_1TYdcv"`)}, true},
		{"chargeback by description", stripesource.Transaction{Type: "adjustment", Description: "Chargeback withdrawal for ch_x"}, true},
		{"fee credit adjustment is NOT a reversal", stripesource.Transaction{Type: "adjustment", ReportingCategory: "fee", Description: "€10,000 free Credit"}, false},
		{"plain charge", stripesource.Transaction{Type: "charge"}, false},
		{"payout", stripesource.Transaction{Type: "payout"}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := stripeBTIsReversal(c.bt); got != c.want {
				t.Errorf("stripeBTIsReversal = %v, want %v", got, c.want)
			}
		})
	}
}

// TestExtractStripeChargeIDFromText covers pulling the originating charge id out
// of a chargeback's free-text description (the only place it's recorded).
func TestExtractStripeChargeIDFromText(t *testing.T) {
	cases := map[string]string{
		"Chargeback withdrawal for ch_3TQ2VlFAhaWeDyow0YG2Fjqf": "ch_3TQ2VlFAhaWeDyow0YG2Fjqf",
		"Dispute for py_1AbcDEF, see case":                      "py_1AbcDEF",
		"REFUND FOR PAYMENT (Some Event)":                       "",
		"":                                                      "",
	}
	for text, want := range cases {
		if got := extractStripeChargeIDFromText(text); got != want {
			t.Errorf("extractStripeChargeIDFromText(%q) = %q, want %q", text, got, want)
		}
	}
}

// TestStripeRefundEventName covers stripping Stripe's "REFUND FOR …(event)"
// wrapper, used to disambiguate refunds that share an amount.
func TestStripeRefundEventName(t *testing.T) {
	cases := map[string]string{
		"REFUND FOR PAYMENT (Voice Initiation Workshop)": "Voice Initiation Workshop",
		"REFUND FOR CHARGE (Subscription update)":        "Subscription update",
		"REFUND FOR PAYMENT":                             "",
	}
	for desc, want := range cases {
		if got := stripeRefundEventName(desc); got != want {
			t.Errorf("stripeRefundEventName(%q) = %q, want %q", desc, got, want)
		}
	}
}

// TestStripeReversalAmountFallback proves a bank payment refund (pyr_) — which
// carries no charge link — inherits the account of the earlier charge it reverses
// by matching on amount, disambiguated by event name. This is the path that gives
// the 12 pyr_ refunds an account.
func TestStripeReversalAmountFallback(t *testing.T) {
	cat, mappings := stripeTestCategorizer()
	acc := &AccountConfig{Provider: "stripe", Slug: "stripe", AccountID: "acct_1Nn0FaFAhaWeDyow"}

	// Two earlier charges share the amount 2300 cents: a ticket and a donation.
	// The refund's event name must steer it to the ticket charge's account.
	idx := map[int64][]stripeChargeClass{
		2300: {
			{Created: 100, Category: "donation", Collective: "openletter", EventKey: normalizeLumaMatchText("Financial contribution to openletter")},
			{Created: 200, Category: "ticket", Collective: "commonshub", EventKey: normalizeLumaMatchText("Sabar dance night")},
		},
	}

	tests := []struct {
		name        string
		bt          stripesource.Transaction
		wantCat     string
		wantAccount string
	}{
		{
			name:        "event name steers to the ticket charge",
			bt:          stripesource.Transaction{ID: "txn_pyr_ticket", Type: "payment_refund", Currency: "EUR", Amount: -2300, Net: -2300, Source: json.RawMessage(`"pyr_1Rc26L"`), Created: 300, Description: "REFUND FOR PAYMENT (Sabar dance night)"},
			wantCat:     "ticket",
			wantAccount: "700150",
		},
		{
			name:        "no event match falls back to most recent preceding charge",
			bt:          stripesource.Transaction{ID: "txn_pyr_noevent", Type: "payment_refund", Currency: "EUR", Amount: -2300, Net: -2300, Source: json.RawMessage(`"pyr_1Abc"`), Created: 300, Description: "REFUND FOR PAYMENT"},
			wantCat:     "ticket", // Created:200 is the most recent before 300
			wantAccount: "700150",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bt := tc.bt
			amount := stripeStatementLineAmount(bt)
			if amount >= 0 {
				t.Fatalf("booked amount = %.2f, want negative (refund)", amount)
			}
			tx := stripeClassificationTransaction(acc, bt, amount)
			cat.Apply(&tx)
			if tx.Category != "" {
				t.Fatalf("precondition: no charge link should leave category empty, got %q", tx.Category)
			}
			stripeApplyReversalFallback(bt, &tx, idx)
			if tx.Category != tc.wantCat {
				t.Errorf("category = %q, want %q", tx.Category, tc.wantCat)
			}
			if got := stripeOdooAccountCode(bt, tx, mappings); got != tc.wantAccount {
				t.Errorf("account = %q, want %q", got, tc.wantAccount)
			}
		})
	}
}

// TestStripeTicketRefundFromEventName proves a ticket refund is classified as a
// ticket straight from the event name in its description, even with no resolvable
// origin charge — so it lands on the ticket account (700150), booked negative.
func TestStripeTicketRefundFromEventName(t *testing.T) {
	cat, mappings := stripeTestCategorizer()
	acc := &AccountConfig{Provider: "stripe", Slug: "stripe", AccountID: "acct_1Nn0FaFAhaWeDyow"}
	hints := []stripeLocalEventHint{{ID: "evt-123", Name: "Voice Initiation Workshop", URL: "https://lu.ma/voice", Collective: "commonshub"}}

	for _, desc := range []string{
		"REFUND FOR PAYMENT (Voice Initiation Workshop)",
		"REFUND FOR CHARGE (Voice Initiation Workshop)",
	} {
		t.Run(desc, func(t *testing.T) {
			bt := stripesource.Transaction{ID: "txn_pyr_ticket", Type: "payment_refund", Currency: "EUR", Amount: -3500, Net: -3500, Source: json.RawMessage(`"pyr_x"`), Description: desc}
			bt = enrichStripeReversalEvent(bt, hints)
			amount := stripeStatementLineAmount(bt)
			if amount >= 0 {
				t.Fatalf("booked amount = %.2f, want negative", amount)
			}
			tx := stripeClassificationTransaction(acc, bt, amount)
			cat.Apply(&tx)
			if tx.Category != "ticket" {
				t.Errorf("category = %q, want ticket", tx.Category)
			}
			if got := stripeOdooAccountCode(bt, tx, mappings); got != "700150" {
				t.Errorf("account = %q, want 700150", got)
			}
		})
	}

	// A reversal whose event isn't known stays unclassified (no false ticket).
	bt := stripesource.Transaction{Type: "refund", Description: "REFUND FOR CHARGE (Totally Unknown Event)"}
	bt = enrichStripeReversalEvent(bt, hints)
	if got := stringMetadata(bt.Metadata, "category"); got != "" {
		t.Errorf("unknown event: category = %q, want empty", got)
	}
}

// TestLiveStripeReversalsGetAccount validates the user's requirement against the
// ACTUAL local data + live rules/mappings: every refund and chargeback resolves
// to a non-empty Odoo account (via charge link or amount matching). The cmd test
// harness (main_test.go) pins config to an empty sandbox, so point it back at the
// real ~/.chb explicitly:
//
//	CHB_LIVE_APP_DIR=$HOME/.chb \
//	  go test ./cmd/ -run TestLiveStripeReversalsGetAccount -v
func TestLiveStripeReversalsGetAccount(t *testing.T) {
	appDir := os.Getenv("CHB_LIVE_APP_DIR")
	if appDir == "" {
		t.Skip("set CHB_LIVE_APP_DIR=$HOME/.chb to validate live reversals")
	}
	// Override the harness sandbox so real rules.json / odoo_mapping.json / data load.
	t.Setenv("APP_DATA_DIR", appDir)
	t.Setenv("DATA_DIR", filepath.Join(appDir, "data"))

	acc := &AccountConfig{Provider: "stripe", Slug: "stripe", AccountID: "acct_1Nn0FaFAhaWeDyow"}
	bts, err := stripesource.LoadTransactionsSince(DataDir(), acc.AccountID, 0)
	if err != nil {
		t.Fatalf("load BTs: %v", err)
	}
	chargeIndex := loadArchivedStripeCharges(DataDir())
	refundMap := loadArchivedStripeRefundMap(DataDir())
	eventHints := loadLocalStripeEventHints(DataDir())
	originIndex := loadStripeRefundOriginIndex(acc, chargeIndex, eventHints)
	mappings, _ := LoadOdooMappings()
	categorizer := NewCategorizer(nil)

	// originHasCategorized reports whether some earlier charge of this amount IS
	// categorized — i.e. whether an account COULD have been inherited. When none
	// is, the reversal is blocked by an uncategorized source charge (a separate,
	// larger gap), not by a matching bug.
	originHasCategorized := func(bt stripesource.Transaction) bool {
		amt := bt.Amount
		if amt < 0 {
			amt = -amt
		}
		for _, c := range originIndex[amt] {
			if c.Created < bt.Created && c.Category != "" {
				return true
			}
		}
		return false
	}

	reversals, withAccount, blocked := 0, 0, 0
	for _, raw := range bts {
		if !stripeBTIsReversal(raw) {
			continue
		}
		reversals++
		bt := enrichStripeBTForClassification(raw, chargeIndex, refundMap, eventHints)
		amount := stripeStatementLineAmount(bt)
		tx := stripeClassificationTransaction(acc, bt, amount)
		categorizer.Apply(&tx)
		stripeApplyReversalFallback(bt, &tx, originIndex)
		if stripeOdooAccountCode(bt, tx, mappings) != "" {
			withAccount++
			continue
		}
		date := time.Unix(raw.Created, 0).Format("2006-01-02")
		if originHasCategorized(bt) {
			// A categorized origin existed but wasn't inherited → matching bug.
			t.Errorf("reversal %s (%s, %s, %.2f) had a categorized origin but resolved to NO account — desc=%q",
				raw.ID, raw.Type, date, amount, raw.Description)
			continue
		}
		blocked++
		t.Logf("blocked (uncategorized source charge): %s %s %.2f %q", raw.ID, date, amount, raw.Description)
	}
	t.Logf("%d/%d reversals resolved; %d blocked by an uncategorized source charge", withAccount, reversals, blocked)
	if reversals == 0 {
		t.Skip("no reversals in local data")
	}
}

// TestStripeReversalNoOriginLeavesUncategorized documents that when nothing of the
// refund's amount precedes it, the fallback is a no-op (no false attribution).
func TestStripeReversalNoOriginLeavesUncategorized(t *testing.T) {
	bt := stripesource.Transaction{Type: "payment_refund", Amount: -9999, Created: 50, Description: "REFUND FOR PAYMENT (Unknown)"}
	tx := TransactionEntry{}
	stripeApplyReversalFallback(bt, &tx, map[int64][]stripeChargeClass{})
	if tx.Category != "" {
		t.Errorf("category = %q, want empty (no matching origin)", tx.Category)
	}
}
