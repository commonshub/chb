package cmd

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	stripesource "github.com/CommonsHub/chb/providers/stripe"
)

func TestStripeStatementLineAmountUsesGrossForCustomerTransactions(t *testing.T) {
	tests := []struct {
		name string
		bt   stripesource.Transaction
		want float64
	}{
		{
			name: "charge",
			bt:   stripesource.Transaction{Type: "charge", Amount: 2500, Fee: 100, Net: 2400},
			want: 25,
		},
		{
			name: "payment",
			bt:   stripesource.Transaction{Type: "payment", Amount: 4250, Fee: 150, Net: 4100},
			want: 42.5,
		},
		{
			name: "refund",
			bt:   stripesource.Transaction{Type: "refund", Amount: -1000, Fee: -40, Net: -960},
			want: -10,
		},
		{
			name: "payment refund",
			bt:   stripesource.Transaction{Type: "payment_refund", Amount: -1600, Fee: -60, Net: -1540},
			want: -16,
		},
		{
			name: "payout",
			bt:   stripesource.Transaction{Type: "payout", Amount: -5000, Fee: 0, Net: -5000},
			want: -50,
		},
		{
			name: "stripe fee",
			bt:   stripesource.Transaction{Type: "stripe_fee", Amount: -300, Fee: 0, Net: -300},
			want: -3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := stripeStatementLineAmount(tt.bt); got != tt.want {
				t.Fatalf("stripeStatementLineAmount() = %.2f, want %.2f", got, tt.want)
			}
		})
	}
}

func TestFilterStripeBTsByDateWindow(t *testing.T) {
	tz := BrusselsTZ()
	bts := []stripesource.Transaction{
		{ID: "before", Created: time.Date(2026, 4, 30, 23, 59, 0, 0, tz).Unix()},
		{ID: "first", Created: time.Date(2026, 5, 1, 0, 0, 0, 0, tz).Unix()},
		{ID: "second", Created: time.Date(2026, 5, 2, 12, 0, 0, 0, tz).Unix()},
		{ID: "after", Created: time.Date(2026, 6, 1, 0, 0, 0, 0, tz).Unix()},
	}

	got := filterStripeBTsByDateWindow(
		bts,
		time.Date(2026, 5, 1, 0, 0, 0, 0, tz),
		time.Date(2026, 5, 31, 23, 59, 59, 0, tz),
	)
	if len(got) != 2 || got[0].ID != "first" || got[1].ID != "second" {
		t.Fatalf("filtered IDs = %#v, want first and second", got)
	}
}

func TestFilterStripeBTsAfterOdooCursorRewindsToStartOfCursorDay(t *testing.T) {
	// A previous batch may have had interleaved successes and failures on
	// the same day — Odoo's id-desc cursor returns the *last successful*
	// import, and a naive `lastIdx+1` slice would silently skip the
	// failed siblings on resume. We rewind to the first BT on the cursor's
	// Brussels-day so the existingIDs dedup catches up.
	tz := BrusselsTZ()
	acc := &AccountConfig{AccountID: "acct_test"}
	bts := []stripesource.Transaction{
		{ID: "bt_prev_day", Created: time.Date(2026, 5, 18, 23, 30, 0, 0, tz).Unix()},
		{ID: "bt_day_a", Created: time.Date(2026, 5, 19, 9, 0, 0, 0, tz).Unix()},
		{ID: "bt_day_b", Created: time.Date(2026, 5, 19, 10, 0, 0, 0, tz).Unix()},
		{ID: "bt_day_cursor", Created: time.Date(2026, 5, 19, 11, 0, 0, 0, tz).Unix()},
		{ID: "bt_day_after", Created: time.Date(2026, 5, 19, 12, 0, 0, 0, tz).Unix()},
		{ID: "bt_next_day", Created: time.Date(2026, 5, 20, 8, 0, 0, 0, tz).Unix()},
	}
	cursor := odooImportCursor{
		Found:          true,
		UniqueImportID: stripeBTImportID(acc, bts[3]),
		Date:           "2026-05-19",
	}
	got, matched := filterStripeBTsAfterOdooCursor(acc, bts, cursor)
	if !matched {
		t.Fatalf("expected matched=true")
	}
	if len(got) != 5 {
		t.Fatalf("len(got) = %d, want 5 (all BTs on 2026-05-19 plus 2026-05-20)", len(got))
	}
	wantIDs := []string{"bt_day_a", "bt_day_b", "bt_day_cursor", "bt_day_after", "bt_next_day"}
	for i, w := range wantIDs {
		if got[i].ID != w {
			t.Fatalf("got[%d].ID = %q, want %q", i, got[i].ID, w)
		}
	}
}

func TestFilterStripeBTsAfterOdooCursorReturnsEmptyWhenCursorIsLast(t *testing.T) {
	tz := BrusselsTZ()
	acc := &AccountConfig{AccountID: "acct_test"}
	bts := []stripesource.Transaction{
		{ID: "bt_earlier", Created: time.Date(2026, 5, 18, 12, 0, 0, 0, tz).Unix()},
		{ID: "bt_cursor", Created: time.Date(2026, 5, 19, 11, 0, 0, 0, tz).Unix()},
	}
	cursor := odooImportCursor{
		Found:          true,
		UniqueImportID: stripeBTImportID(acc, bts[1]),
		Date:           "2026-05-19",
	}
	got, matched := filterStripeBTsAfterOdooCursor(acc, bts, cursor)
	if !matched {
		t.Fatalf("expected matched=true")
	}
	// Cursor is the only BT on its day, so we still rewind to it. The
	// existingIDs dedup will skip it. The result keeps the BT in the
	// slice — the loop body handles it correctly.
	if len(got) != 1 || got[0].ID != "bt_cursor" {
		t.Fatalf("got = %#v, want [bt_cursor]", got)
	}
}

func TestUpdateBTStatsUsesGrossCustomerAmounts(t *testing.T) {
	stats := &syncStats{}

	updateBTStats(stats, stripesource.Transaction{Type: "charge", Amount: 2500, Fee: 100, Net: 2400}, 25)
	updateBTStats(stats, stripesource.Transaction{Type: "refund", Amount: -1000, Fee: -40, Net: -960}, -10)

	if stats.Charges != 1 {
		t.Fatalf("Charges = %d, want 1", stats.Charges)
	}
	if stats.ChargesGross != 25 {
		t.Fatalf("ChargesGross = %.2f, want 25.00", stats.ChargesGross)
	}
	if stats.ChargeFees != 0.6 {
		t.Fatalf("ChargeFees = %.2f, want 0.60", stats.ChargeFees)
	}
	if stats.Refunds != 1 {
		t.Fatalf("Refunds = %d, want 1", stats.Refunds)
	}
	if stats.RefundsTotal != -10 {
		t.Fatalf("RefundsTotal = %.2f, want -10.00", stats.RefundsTotal)
	}
}

func TestUpdateBTStatsNetsPayoutCancellations(t *testing.T) {
	stats := &syncStats{}

	updateBTStats(stats, stripesource.Transaction{Type: "payout", Amount: -6000, Net: -6000}, -60)
	updateBTStats(stats, stripesource.Transaction{Type: "payout_cancel", Amount: 1000, Net: 1000}, 10)

	if stats.PayoutsTotal != -50 {
		t.Fatalf("PayoutsTotal = %.2f, want -50.00", stats.PayoutsTotal)
	}
}

func TestStripeImplicitChargeFeeCentsTracksCustomerTransactionFees(t *testing.T) {
	tests := []struct {
		name string
		bt   stripesource.Transaction
		want int64
		ok   bool
	}{
		{
			name: "charge fee",
			bt:   stripesource.Transaction{Type: "charge", Amount: 2500, Fee: 100, Net: 2400},
			want: 100,
			ok:   true,
		},
		{
			name: "refund returned fee",
			bt:   stripesource.Transaction{Type: "refund", Amount: -1000, Fee: -40, Net: -960},
			want: -40,
			ok:   true,
		},
		{
			name: "payout no fee line",
			bt:   stripesource.Transaction{Type: "payout", Amount: -2400, Fee: 0, Net: -2400},
			ok:   false,
		},
		{
			name: "stripe_fee is its own line, not implicit",
			bt:   stripesource.Transaction{Type: "stripe_fee", Amount: -14, Net: -14},
			ok:   false,
		},
		{
			name: "adjustment is its own line, not implicit",
			bt:   stripesource.Transaction{Type: "adjustment", Amount: -500, Net: -500},
			ok:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := stripeImplicitChargeFeeCents(tt.bt)
			if ok != tt.ok {
				t.Fatalf("ok = %v, want %v", ok, tt.ok)
			}
			if got != tt.want {
				t.Fatalf("fee cents = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestOpenStatementFeeImportIDIsStableAcrossRuns(t *testing.T) {
	// Regression: the rolling "Stripe fees for open statement" line must
	// have an importID that does not change between sync runs, so that
	// successive runs update the same line instead of accumulating
	// duplicates. Tying the key to the open statement's Odoo ID is the
	// invariant we rely on.
	const accountID = "acct_1ABC"
	const stmtID = 48
	first := openStatementFeeImportID(accountID, stmtID)
	second := openStatementFeeImportID(accountID, stmtID)
	if first != second {
		t.Fatalf("importID is non-deterministic: %q vs %q", first, second)
	}
	if got, want := first, "stripe:acct_1abc:open:48:fees"; got != want {
		t.Fatalf("importID = %q, want %q", got, want)
	}

	// Different statements (e.g. after a payout closes one and opens
	// another) must produce different IDs.
	if openStatementFeeImportID(accountID, stmtID) == openStatementFeeImportID(accountID, stmtID+1) {
		t.Fatalf("importID must differ across open statements")
	}
}

func TestOpenStatementFeeLineUpdateDoesNotChangeDate(t *testing.T) {
	acc := &AccountConfig{AccountID: "acct_1ABC"}
	vals := stripeOpenStatementFeeLineUpdateVals(acc, "stripe:acct_1abc:open:48:fees", -12.34, 1234, 3, "2026-05-01", "2026-05-18")
	if _, ok := vals["date"]; ok {
		t.Fatalf("existing open-statement fee line update must not include date: %#v", vals)
	}
	if got, want := vals["payment_ref"], "Stripe fees for open statement"; got != want {
		t.Fatalf("payment_ref = %v, want %q", got, want)
	}
}

func TestParseStripeAccountIDFromOpenFeeImportID(t *testing.T) {
	tests := []struct {
		name     string
		importID string
		want     string
	}{
		{"canonical", "stripe:acct_1abc:open:48:fees", "acct_1abc"},
		{"legacy bt range", "stripe:acct_1abc:open:bt_aaa:bt_zzz:fees", "acct_1abc"},
		{"non-stripe", "manual:acct_1abc:open:48:fees", ""},
		{"closed payout", "stripe:acct_1abc:po_123:fees", ""},
		{"too short", "stripe:acct_1abc:open", ""},
		{"empty", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseStripeAccountIDFromOpenFeeImportID(tt.importID); got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestStripeGrossCustomerRowsPlusAggregateFeeLineEqualNet(t *testing.T) {
	var feeCents int64
	var grossTotal float64
	var netTotal float64

	for _, bt := range []stripesource.Transaction{
		{Type: "charge", Amount: 2500, Fee: 100, Net: 2400},
		{Type: "refund", Amount: -1000, Fee: -40, Net: -960},
	} {
		grossTotal += stripeStatementLineAmount(bt)
		netTotal += centsToEuros(bt.Net)
		if cents, ok := stripeImplicitChargeFeeCents(bt); ok {
			feeCents += cents
		}
	}

	total := grossTotal + stripeAggregateFeeLineAmount(feeCents)
	if total != netTotal {
		t.Fatalf("gross+aggregate fee = %.2f, want net %.2f", total, netTotal)
	}
	if got := stripeAggregateFeeLineAmount(feeCents); got != -0.6 {
		t.Fatalf("aggregate fee line = %.2f, want -0.60", got)
	}
}

// Standalone fee/adjustment BTs (Billing - Usage Fee, Automatic Taxes,
// chargeback withdrawals, free Credits, …) must be pushed as their own
// statement lines, not folded into the aggregate fee line. Only the
// implicit per-charge processing fee — which has no standalone BT — is
// aggregated.
func TestStripeStandaloneFeeBTsAreNotAggregated(t *testing.T) {
	cases := []stripesource.Transaction{
		{Type: "stripe_fee", Amount: -14, Net: -14, Description: "Billing - Usage Fee"},
		{Type: "stripe_fee", Amount: -250, Net: -250, Description: "Automatic Taxes"},
		{Type: "adjustment", Amount: -3000, Net: -3000, Description: "Chargeback withdrawal"},
		{Type: "adjustment", Amount: 500, Net: 500, Description: "free Credit"},
	}
	for _, bt := range cases {
		if _, ok := stripeImplicitChargeFeeCents(bt); ok {
			t.Fatalf("standalone fee BT %q must not contribute to implicit per-charge fee aggregate", bt.Description)
		}
		if got := stripeStatementLineAmount(bt); got != centsToEuros(bt.Net) {
			t.Fatalf("standalone fee BT %q line amount = %.2f, want %.2f", bt.Description, got, centsToEuros(bt.Net))
		}
	}
}

func TestBTPaymentRefPayoutFallsBackToCreatedDate(t *testing.T) {
	created := time.Date(2024, 1, 4, 12, 0, 0, 0, time.UTC).Unix()
	bt := stripesource.Transaction{
		Type:              "payout",
		Created:           created,
		PayoutArrivalDate: 0,
		PayoutAutomatic:   false,
	}

	if got, want := btPaymentRef(bt), "Manual payout 2024-01-04"; got != want {
		t.Fatalf("btPaymentRef() = %q, want %q", got, want)
	}
}

func TestPayoutStatementLabelsFallBackToCreatedDate(t *testing.T) {
	created := time.Date(2024, 1, 4, 12, 0, 0, 0, time.UTC).Unix()
	bt := stripesource.Transaction{
		Type:              "payout",
		Created:           created,
		Net:               -100,
		Currency:          "eur",
		PayoutArrivalDate: 0,
	}

	name, _ := payoutStatementLabels(bt)
	if got, want := name, "2024-01-04 Stripe payout (1.00 EUR)"; got != want {
		t.Fatalf("payoutStatementLabels() name = %q, want %q", got, want)
	}
}

func TestStripePayoutClosesStatementFallbackForOldLocalArchives(t *testing.T) {
	if !stripePayoutClosesStatement(stripesource.Transaction{Type: "payout"}) {
		t.Fatalf("payout without expanded metadata should close statement")
	}
	if stripePayoutClosesStatement(stripesource.Transaction{Type: "payout", PayoutID: "po_manual"}) {
		t.Fatalf("payout with explicit automatic=false metadata should not close statement")
	}
	if !stripePayoutClosesStatement(stripesource.Transaction{Type: "payout", PayoutID: "po_auto", PayoutAutomatic: true}) {
		t.Fatalf("automatic payout should close statement")
	}
}

func TestAmbiguousLocalPartnerMatchUsesOldestAndSuggestsMerge(t *testing.T) {
	idx := &odooPartnerIndex{
		byEmail: map[string][]OdooPartner{
			"toon@example.com": {
				{ID: 42, Name: "Toon Vanagt", Email: "toon@example.com", Active: true},
				{ID: 7, Name: "Toon Vanagt", Email: "toon@example.com", Active: true},
			},
		},
		byName: map[string][]OdooPartner{},
	}
	stats := &syncStats{}
	id := resolveOdooPartnerFromLocalIndex(idx, "Toon Vanagt", "toon@example.com", map[string]int{}, stats)
	if id != 7 {
		t.Fatalf("partner id = %d, want oldest #7", id)
	}
	if stats.PartnersMatched != 1 || stats.PartnersSkipped != 1 {
		t.Fatalf("stats matched/skipped = %d/%d, want 1/1", stats.PartnersMatched, stats.PartnersSkipped)
	}
	if len(stats.Ambiguous) != 1 || !strings.Contains(stats.Ambiguous[0], "linked to oldest partner #7") {
		t.Fatalf("merge suggestion = %#v", stats.Ambiguous)
	}
}

func TestStripeOdooPaymentRefPrefersCategoryCollective(t *testing.T) {
	bt := stripesource.Transaction{
		CustomerName: "Jane Donor",
		Metadata: map[string]interface{}{
			"category":   "donation",
			"collective": "openletter",
		},
	}
	tx := stripeRuleTransaction(&AccountConfig{Slug: "stripe", AccountID: "acct_ABC"}, bt, 10)
	if got, want := stripeOdooPaymentRef(bt, tx), "donation openletter"; got != want {
		t.Fatalf("stripeOdooPaymentRef() = %q, want %q", got, want)
	}

	bt.Metadata = nil
	tx = stripeRuleTransaction(&AccountConfig{Slug: "stripe", AccountID: "acct_ABC"}, bt, 10)
	if got, want := stripeOdooPaymentRef(bt, tx), "Jane Donor"; got != want {
		t.Fatalf("stripeOdooPaymentRef() = %q, want %q", got, want)
	}
}

func TestStripeOdooPaymentRefIncludesTicketEventName(t *testing.T) {
	bt := stripesource.Transaction{
		Description: "Building IRL communities - why, how and with what",
		Metadata: map[string]interface{}{
			"application": "luma",
		},
	}
	tx := TransactionEntry{Category: "ticket"}

	if got, want := stripeOdooPaymentRef(bt, tx), "ticket Building IRL communities - why, how and with what"; got != want {
		t.Fatalf("stripeOdooPaymentRef() = %q, want %q", got, want)
	}

	bt.Metadata["eventName"] = "Canonical event title"
	if got, want := stripeOdooPaymentRef(bt, tx), "ticket Canonical event title"; got != want {
		t.Fatalf("stripeOdooPaymentRef() = %q, want %q", got, want)
	}
}

func TestStripeOdooAccountCodeUsesFeeAccount(t *testing.T) {
	rules := []OdooMapping{{
		Match: OdooMappingMatch{Category: "stripe_fee", Direction: "out"},
		Set:   OdooMappingResult{AccountCode: "657020"},
	}}
	tests := []struct {
		name string
		bt   stripesource.Transaction
	}{
		{
			name: "stripe fee type",
			bt:   stripesource.Transaction{Type: "stripe_fee"},
		},
		{
			name: "billing usage fee description",
			bt:   stripesource.Transaction{Description: "Billing - Usage Fee for Invoice"},
		},
		{
			name: "automatic taxes description",
			bt:   stripesource.Transaction{Description: "Automatic Taxes fee"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx := stripeRuleTransaction(&AccountConfig{Slug: "stripe", AccountID: "acct_ABC"}, tt.bt, -1)
			if got := stripeOdooAccountCode(tt.bt, tx, rules); got != "657020" {
				t.Fatalf("stripeOdooAccountCode() = %q, want 657020", got)
			}
		})
	}
}

func TestStripeFeePaymentRefUsesStripeDescription(t *testing.T) {
	bt := stripesource.Transaction{
		Type:        "stripe_fee",
		Description: "Automatic Taxes (2026-05-17): Automatic tax",
		Metadata:    map[string]interface{}{"category": "stripe_fee"},
	}
	tx := stripeRuleTransaction(&AccountConfig{Slug: "stripe", AccountID: "acct_ABC"}, bt, -1.23)
	if got := stripeOdooPaymentRef(bt, tx); got != bt.Description {
		t.Fatalf("stripeOdooPaymentRef() = %q, want %q", got, bt.Description)
	}
	narr := buildStripeOdooNarration(&AccountConfig{Slug: "stripe", AccountID: "acct_ABC"}, bt, tx, "stripe:acct_abc:txn_fee", -1.23)
	var meta map[string]interface{}
	if err := json.Unmarshal([]byte(narr), &meta); err != nil {
		t.Fatalf("narration json: %v", err)
	}
	if got := metaString(meta, "category"); got != "stripe_fee" {
		t.Fatalf("category = %q, want stripe_fee", got)
	}
	if got, _ := meta["stripeFee"].(bool); !got {
		t.Fatalf("stripeFee tag missing in narration: %#v", meta)
	}
}

func TestStripeFeeNarrationNeedsUpdateIgnoresUnrelatedMetadata(t *testing.T) {
	current := map[string]interface{}{
		"category":          "stripe_fee",
		"stripeFee":         true,
		"stripeDescription": "Automatic Taxes (2026-05-17): Automatic tax",
		"description":       "Automatic Taxes (2026-05-17): Automatic tax",
		"tags":              []interface{}{"stripe_fee"},
		"legacyOnly":        "kept",
	}
	desired := map[string]interface{}{
		"category":          "stripe_fee",
		"stripeFee":         true,
		"stripeDescription": "Automatic Taxes (2026-05-17): Automatic tax",
		"description":       "Automatic Taxes (2026-05-17): Automatic tax",
		"tags":              []string{"stripe_fee"},
		"newUnrelated":      "ignored",
	}
	if stripeFeeNarrationNeedsUpdate(current, desired) {
		t.Fatalf("fee narration should not need update for unrelated metadata differences")
	}
	delete(current, "stripeFee")
	if !stripeFeeNarrationNeedsUpdate(current, desired) {
		t.Fatalf("fee narration should need update when stripeFee tag is missing")
	}
}

func TestEnrichStripeBTFromLocalEventMarksTicket(t *testing.T) {
	bt := stripesource.Transaction{
		Type:        "payment",
		Description: "An Economy of Better for Europe - An evening with Dutch new economy thinkers",
		Metadata:    map[string]interface{}{},
	}
	bt = enrichStripeBTFromLocalEvent(bt, []stripeLocalEventHint{
		{
			ID:         "evt-xMTh8TLTnN960IY",
			Name:       "An Economy of Better for Europe",
			URL:        "https://luma.com/i1cy84sl",
			Collective: "i1cy84sl",
		},
	})

	if got := stringMetadata(bt.Metadata, "category"); got != "ticket" {
		t.Fatalf("category = %q, want ticket", got)
	}
	if got := stringMetadata(bt.Metadata, "application"); got != "luma" {
		t.Fatalf("application = %q, want luma", got)
	}
	if got := stringMetadata(bt.Metadata, "eventName"); got != "An Economy of Better for Europe" {
		t.Fatalf("eventName = %q", got)
	}
}

func TestStripeOdooAccountCodeUsesTicketRule(t *testing.T) {
	rules := []OdooMapping{{
		Match: OdooMappingMatch{Category: "ticket", Direction: "in"},
		Set:   OdooMappingResult{AccountCode: "700150"},
	}}
	tx := TransactionEntry{Category: "ticket", Type: "CREDIT"}

	if got := stripeOdooAccountCode(stripesource.Transaction{Type: "payment"}, tx, rules); got != "700150" {
		t.Fatalf("stripeOdooAccountCode() = %q, want 700150", got)
	}
}

func TestEnrichStripeBTFromChargeAddsPaymentLinkForRules(t *testing.T) {
	bt := stripesource.Transaction{
		ID:       "txn_123",
		Type:     "charge",
		Amount:   1000,
		Currency: "eur",
		Source:   json.RawMessage(`{"id":"ch_123"}`),
	}
	bt = enrichStripeBTFromCharge(bt, map[string]*stripesource.Charge{
		"ch_123": {
			ID:          "ch_123",
			CustomerID:  "cus_123",
			BillingName: "jane donor",
			PaymentLink: "plink_openletter",
		},
	})
	if got, want := bt.CustomerName, "Jane Donor"; got != want {
		t.Fatalf("CustomerName = %q, want %q", got, want)
	}
	if got, want := stringMetadata(bt.Metadata, "stripeCustomerId"), "cus_123"; got != want {
		t.Fatalf("stripeCustomerId metadata = %q, want %q", got, want)
	}

	ruleTx := stripeRuleTransaction(&AccountConfig{Slug: "stripe", AccountID: "acct_ABC"}, bt, 10)
	rule := Rule{
		Match: RuleMatch{
			Provider:    "stripe",
			Currency:    "EUR",
			PaymentLink: "plink_openletter",
		},
		Assign: RuleAssign{Category: "donation", Collective: "openletter"},
	}
	if !rule.MatchesTransaction(ruleTx) {
		t.Fatalf("paymentLink rule did not match enriched Stripe transaction: %#v", ruleTx.Metadata)
	}
	(&Categorizer{rules: []Rule{rule}}).Apply(&ruleTx)
	if got, want := stripeOdooPaymentRef(bt, ruleTx), "donation openletter"; got != want {
		t.Fatalf("stripeOdooPaymentRef() = %q, want %q", got, want)
	}
}

func TestNormalizeStripePartnerName(t *testing.T) {
	tests := []struct {
		name  string
		email string
		want  string
	}{
		{name: "judithsaragossi@gmail.com", want: "Judithsaragossi"},
		{name: "jane donor", want: "Jane Donor"},
		{name: "JANE DOE", want: "Jane Doe"},
		{name: "JEAN-CHRISTOPHE ALFONSE", want: "Jean-Christophe Alfonse"},
		{name: "", email: "judithsaragossi@gmail.com", want: "Judithsaragossi"},
	}
	for _, tt := range tests {
		if got := normalizeStripePartnerName(tt.name, tt.email); got != tt.want {
			t.Fatalf("normalizeStripePartnerName(%q, %q) = %q, want %q", tt.name, tt.email, got, tt.want)
		}
	}
}

func TestOdooPartnerCollectiveTagName(t *testing.T) {
	if got, want := odooPartnerCollectiveTagName("Open Letter"), "collective:open-letter"; got != want {
		t.Fatalf("odooPartnerCollectiveTagName() = %q, want %q", got, want)
	}
}

func TestBuildStripeOdooNarrationStoresStripeDetails(t *testing.T) {
	acc := &AccountConfig{AccountID: "acct_ABC"}
	bt := stripesource.Transaction{
		ID:                "txn_123",
		Created:           time.Date(2024, 1, 4, 12, 0, 0, 0, time.UTC).Unix(),
		Amount:            1000,
		Fee:               50,
		Net:               950,
		Currency:          "eur",
		Type:              "charge",
		ReportingCategory: "charge",
		CustomerName:      "Jane Donor",
		CustomerEmail:     "jane@example.com",
		ChargeID:          "ch_123",
		Metadata: map[string]interface{}{
			"category":   "donation",
			"collective": "openletter",
			"orderId":    "ord_123",
		},
	}

	var got map[string]interface{}
	tx := stripeRuleTransaction(acc, bt, 10)
	if err := json.Unmarshal([]byte(buildStripeOdooNarration(acc, bt, tx, "stripe:acct_abc:txn_123", 10)), &got); err != nil {
		t.Fatalf("narration is not JSON: %v", err)
	}
	if got["category"] != "donation" || got["collective"] != "openletter" {
		t.Fatalf("category/collective missing from narration: %#v", got)
	}
	if got["balanceTransaction"] != "txn_123" || got["chargeId"] != "ch_123" {
		t.Fatalf("Stripe IDs missing from narration: %#v", got)
	}
	meta, ok := got["stripeMetadata"].(map[string]interface{})
	if !ok || meta["orderId"] != "ord_123" {
		t.Fatalf("stripeMetadata missing from narration: %#v", got["stripeMetadata"])
	}
}

func TestParseStripeDryRunUploadCount(t *testing.T) {
	if got := parseStripeDryRunUploadCount("dry-run: 3556 tx would be uploaded"); got != 3556 {
		t.Fatalf("parseStripeDryRunUploadCount() = %d, want 3556", got)
	}
	if got := parseStripeDryRunUploadCount("already in sync"); got != 0 {
		t.Fatalf("parseStripeDryRunUploadCount() = %d, want 0", got)
	}
}

func TestBuildUniqueImportIDStripeHasNoSyntheticIndex(t *testing.T) {
	acc := &AccountConfig{Provider: "stripe", AccountID: "acct_ABC"}
	tx := TransactionEntry{TxHash: "txn_123"}

	if got, want := buildUniqueImportID(acc, tx), "stripe:acct_abc:txn_123"; got != want {
		t.Fatalf("buildUniqueImportID() = %q, want %q", got, want)
	}
}

func TestBuildStripeExistingFromCacheLines(t *testing.T) {
	lines := []OdooCacheLine{
		{ID: 10, UniqueImportID: "stripe:acct_x:txn_1", PaymentRef: "Coffee", Narration: "n1"},
		{ID: 11, UniqueImportID: "stripe:acct_x:txn_2", PaymentRef: "Tea"},
		{ID: 12, UniqueImportID: "", PaymentRef: "Solde de départ"}, // opening entry: no import id
	}
	ids, rows := buildStripeExistingFromCacheLines(lines)

	if !ids["stripe:acct_x:txn_1"] || !ids["stripe:acct_x:txn_2"] {
		t.Fatalf("dedup set missing import ids: %v", ids)
	}
	if len(ids) != 2 {
		t.Fatalf("import-id-less line must be skipped, got %d ids", len(ids))
	}
	row := rows["stripe:acct_x:txn_1"]
	if row == nil {
		t.Fatal("missing row for txn_1")
	}
	// Types must match what odooInt/odooString expect downstream.
	if odooInt(row["id"]) != 10 {
		t.Fatalf("row id = %v, want 10", row["id"])
	}
	if odooString(row["payment_ref"]) != "Coffee" {
		t.Fatalf("row payment_ref = %v, want Coffee", row["payment_ref"])
	}
	if odooString(row["narration"]) != "n1" {
		t.Fatalf("row narration = %v, want n1", row["narration"])
	}
}
