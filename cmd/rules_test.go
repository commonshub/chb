package cmd

import "testing"

func TestRuleMatchesAmountRoundedToCents(t *testing.T) {
	amount := 10.0
	rule := Rule{
		Match: RuleMatch{
			Provider:  "stripe",
			Currency:  "EUR",
			Amount:    &amount,
			Direction: "in",
		},
	}
	tx := TransactionEntry{
		Provider:         "stripe",
		Currency:         "EUR",
		Type:             "CREDIT",
		NormalizedAmount: 10.004,
	}
	if !rule.MatchesTransaction(tx) {
		t.Fatalf("rule should match amount rounded to cents")
	}

	tx.NormalizedAmount = 9.99
	if rule.MatchesTransaction(tx) {
		t.Fatalf("rule should not match a different amount")
	}
}

func TestRuleMatchesPaymentLink(t *testing.T) {
	rule := Rule{
		Match: RuleMatch{
			Provider:    "stripe",
			PaymentLink: "plink_openletter",
		},
	}
	tx := TransactionEntry{
		Provider: "stripe",
		Metadata: map[string]interface{}{
			"paymentLink": "plink_openletter",
		},
	}

	if !rule.MatchesTransaction(tx) {
		t.Fatalf("rule should match paymentLink metadata")
	}

	tx.Metadata["paymentLink"] = "plink_other"
	if rule.MatchesTransaction(tx) {
		t.Fatalf("rule should not match a different paymentLink")
	}
}

// TestRuleDescriptionDoesNotFallBackToCounterparty pins the new behavior:
// before this change, a `description` rule would also match against
// tx.Counterparty when metadata.description and metadata.memo were empty.
// Stripe-fee txs (where counterparty used to be the descriptive text) made
// the conflation obvious. After the fix, description matches against
// metadata.description / metadata.memo only; use the explicit
// `counterparty` field to match against the counterparty.
func TestRuleDescriptionDoesNotFallBackToCounterparty(t *testing.T) {
	descRule := Rule{
		Match: RuleMatch{Description: "*partena*"},
	}
	cpRule := Rule{
		Match: RuleMatch{Counterparty: "*partena*"},
	}
	tx := TransactionEntry{
		Counterparty: "PARTENA PROFESSIONAL",
		Type:         "DEBIT",
		Metadata:     map[string]interface{}{},
	}
	if descRule.MatchesTransaction(tx) {
		t.Fatalf("description rule should NOT match a tx where the keyword only lives in counterparty")
	}
	if !cpRule.MatchesTransaction(tx) {
		t.Fatalf("counterparty rule should match a tx with the keyword in counterparty")
	}

	// And the inverse: a description-only keyword shouldn't match a
	// counterparty rule.
	tx2 := TransactionEntry{
		Counterparty: "Stripe",
		Type:         "DEBIT",
		Metadata: map[string]interface{}{
			"description": "Automatic Taxes (2026-05-17): Automatic tax",
		},
	}
	descTaxRule := Rule{Match: RuleMatch{Description: "*Automatic Tax*"}}
	cpTaxRule := Rule{Match: RuleMatch{Counterparty: "*Automatic Tax*"}}
	if !descTaxRule.MatchesTransaction(tx2) {
		t.Fatalf("description rule should match metadata.description")
	}
	if cpTaxRule.MatchesTransaction(tx2) {
		t.Fatalf("counterparty rule should NOT match a tx where the keyword is only in metadata.description")
	}
}

// TestRuleTargetGating pins the invariant: a rule with no `target`
// defaults to "transaction" and never matches an invoice/bill;
// likewise a `target: "invoice"` rule never matches a transaction.
// Without this, mixing target types in one rules.json would cross-
// contaminate matchers.
func TestRuleTargetGating(t *testing.T) {
	tx := TransactionEntry{
		Counterparty: "Acme",
		Type:         "CREDIT",
		Metadata:     map[string]interface{}{},
	}
	mv := OdooOutgoingInvoicePublic{Title: "MEM/2026/00052", TotalAmount: 100}

	// A pure-transaction rule shouldn't fire on a move.
	txRule := Rule{Match: RuleMatch{Counterparty: "*Acme*"}, Assign: RuleAssign{Category: "x"}}
	if txRule.MatchesMove(mv, "Acme", moveKindInvoice) {
		t.Fatalf("transaction-target rule should NOT match an invoice")
	}
	if !txRule.MatchesTransaction(tx) {
		t.Fatalf("transaction rule should still match the transaction")
	}

	// An invoice rule should fire on a matching invoice but not on a tx.
	invRule := Rule{Target: "invoice", Match: RuleMatch{Title: "MEM/*"}, Assign: RuleAssign{Category: "membership"}}
	if !invRule.MatchesMove(mv, "Acme", moveKindInvoice) {
		t.Fatalf("invoice rule should match MEM/* invoice")
	}
	if invRule.MatchesTransaction(tx) {
		t.Fatalf("invoice-target rule should NOT match a transaction")
	}
	if invRule.MatchesMove(mv, "Acme", moveKindBill) {
		t.Fatalf("invoice-target rule should NOT match a bill")
	}
}

// TestApplyMoveRulesFillsBlanksOnly pins the merge semantics: rules
// only fill in collective/category that are still blank. A manually-
// set value on the move record wins over any matching rule (so the
// operator's [e] edit can override a default-assign rule).
func TestApplyMoveRulesFillsBlanksOnly(t *testing.T) {
	rules := []Rule{
		{Target: "invoice", Match: RuleMatch{Title: "MEM/*"}, Assign: RuleAssign{Category: "membership", Collective: "commonshub"}},
		{Target: "invoice", Match: RuleMatch{}, Assign: RuleAssign{Collective: "commonshub"}},
	}

	mv := OdooOutgoingInvoicePublic{Title: "MEM/2026/00052", Category: "individual"}
	ApplyMoveRules(&mv, "Acme", moveKindInvoice, rules)
	if mv.Category != "individual" {
		t.Fatalf("manual category should be preserved, got %q", mv.Category)
	}
	if mv.Collective != "commonshub" {
		t.Fatalf("blank collective should be filled by rule, got %q", mv.Collective)
	}

	// Non-MEM invoice still gets the default-collective from the
	// no-conditions catch-all but no category.
	mv2 := OdooOutgoingInvoicePublic{Title: "S00148"}
	ApplyMoveRules(&mv2, "Acme", moveKindInvoice, rules)
	if mv2.Collective != "commonshub" {
		t.Fatalf("default collective should apply, got %q", mv2.Collective)
	}
	if mv2.Category != "" {
		t.Fatalf("non-matching invoice should not get a category, got %q", mv2.Category)
	}
}

// TestRuleAmountRange pins the absolute-amount semantic of amount_min
// / amount_max: a rule with `amount_min: 500` matches a +€600
// incoming tx AND a -€600 outgoing tx (sign-independent), but not
// a +€400 tx. Combine with `direction: "in"` to scope.
func TestRuleAmountRange(t *testing.T) {
	min := 500.0
	rule := Rule{
		Match: RuleMatch{MinAmount: &min, Direction: "in"},
	}
	mk := func(amount float64, in bool) TransactionEntry {
		tx := TransactionEntry{
			Amount: amount,
			Type:   "CREDIT",
		}
		if !in {
			tx.Type = "DEBIT"
		}
		return tx
	}
	if !rule.MatchesTransaction(mk(600, true)) {
		t.Fatalf("amount_min=500, direction=in should match +600")
	}
	if rule.MatchesTransaction(mk(400, true)) {
		t.Fatalf("amount_min=500 should NOT match +400")
	}
	if rule.MatchesTransaction(mk(800, false)) {
		t.Fatalf("direction=in should NOT match an outgoing 800")
	}

	// Same range on a move (invoice): m.TotalAmount is absolute.
	imv := Rule{
		Target: "invoice",
		Match:  RuleMatch{Title: "*OSV/20*", MinAmount: &min},
		Assign: RuleAssign{Category: "sponsoring"},
	}
	hi := OdooOutgoingInvoicePublic{Title: "OSV/2024/0001", TotalAmount: 600}
	lo := OdooOutgoingInvoicePublic{Title: "OSV/2024/0002", TotalAmount: 400}
	if !imv.MatchesMove(hi, "", moveKindInvoice) {
		t.Fatalf("title+amount_min should match a 600 OSV invoice")
	}
	if imv.MatchesMove(lo, "", moveKindInvoice) {
		t.Fatalf("title+amount_min=500 should NOT match a 400 OSV invoice")
	}
}

// TestInvoiceRuleDescriptionMatchesLineItems pins that the
// `description` match field, when used on an invoice/bill target,
// matches against the concatenated line item titles (NOT against
// metadata.description, which is the transaction-target semantic).
// Same field name on RuleMatch, different evaluation per target —
// this lets operators author "match the text that describes this
// row" rules without remembering the underlying schema.
func TestInvoiceRuleDescriptionMatchesLineItems(t *testing.T) {
	rules := []Rule{
		{Target: "invoice", Match: RuleMatch{Description: "*room*"},
			Assign: RuleAssign{Category: "rental", Collective: "commonshub"}},
		{Target: "invoice", Match: RuleMatch{Description: "*cowork*"},
			Assign: RuleAssign{Category: "coworking", Collective: "commonshub"}},
	}

	withLineItem := func(title string) OdooOutgoingInvoicePublic {
		return OdooOutgoingInvoicePublic{
			LineItems: []OdooInvoiceLineItem{{Title: title}},
		}
	}

	cases := []struct {
		title    string
		wantCat  string
	}{
		{"Satoshi meeting room booking", "rental"},
		{"Ostrom Event Space", ""}, // contains neither "room" nor "cowork"
		{"Coworker 1 Month", "coworking"},
		{"Coworking Solarpunk subscription", "coworking"},
	}
	for _, tc := range cases {
		mv := withLineItem(tc.title)
		ApplyMoveRules(&mv, "", moveKindInvoice, rules)
		if mv.Category != tc.wantCat {
			t.Fatalf("line item %q: want category %q, got %q", tc.title, tc.wantCat, mv.Category)
		}
	}

	// Section / note rows should be skipped: a section titled "room"
	// shouldn't trigger the rental rule on its own.
	sectionOnly := OdooOutgoingInvoicePublic{
		LineItems: []OdooInvoiceLineItem{
			{DisplayType: "line_section", Title: "Meeting room category"},
		},
	}
	ApplyMoveRules(&sectionOnly, "", moveKindInvoice, rules)
	if sectionOnly.Category != "" {
		t.Fatalf("section row shouldn't trigger description rule, got category %q", sectionOnly.Category)
	}
}

// TestApplyMoveRulesMergesTags pins the additive semantics of
// RuleAssign.Tags: every matching rule's tags get merged onto the
// move, deduplicated, and never removed. Multiple rules can each
// contribute tags — unlike Category / Collective which are first-
// matching-rule-wins.
func TestApplyMoveRulesMergesTags(t *testing.T) {
	amount := 121.0
	rules := []Rule{
		{
			Target: "invoice",
			Match:  RuleMatch{Description: "Shifter*", Amount: &amount},
			Assign: RuleAssign{
				Category:   "coworking",
				Collective: "commonshub",
				Tags:       []string{"shifter", "vat:21%"},
			},
		},
		{
			Target: "invoice",
			Match:  RuleMatch{Description: "*cowork*"},
			Assign: RuleAssign{Tags: []string{"coworking-related"}},
		},
	}

	// Both rules match a "Shifter coworking" line at €121 → tags
	// from both should land, deduplicated.
	mv := OdooOutgoingInvoicePublic{
		TotalAmount: 121,
		LineItems:   []OdooInvoiceLineItem{{Title: "Shifter coworking monthly"}},
		Tags:        []string{"shifter"}, // pre-existing tag survives
	}
	ApplyMoveRules(&mv, "", moveKindInvoice, rules)
	if mv.Category != "coworking" {
		t.Fatalf("category = %q", mv.Category)
	}
	for _, want := range []string{"shifter", "vat:21%", "coworking-related"} {
		if !containsString(mv.Tags, want) {
			t.Fatalf("tags missing %q: %v", want, mv.Tags)
		}
	}
	// Dedupe: "shifter" was pre-existing AND added by the rule; it
	// should appear exactly once.
	count := 0
	for _, tag := range mv.Tags {
		if tag == "shifter" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected exactly one 'shifter' tag, got %d (tags=%v)", count, mv.Tags)
	}
}

// TestInvoiceRuleSubstringGlobCatchesReversal pins the lesson learned
// after v3.4.0 shipped with prefix-anchored title globs: a credit
// note / reversal whose title is "Reversal of: CHB/2025/00076,
// Membership cancelled" should still get the rental tag, not be
// left blank. The fix is on the rule side (use `*CHB/*` so the glob
// runs as a substring match), but this test pins the behaviour so a
// future "tidy the embedded defaults" PR can't accidentally re-add
// the anchor.
func TestInvoiceRuleSubstringGlobCatchesReversal(t *testing.T) {
	rules := []Rule{
		{Target: "invoice", Match: RuleMatch{Title: "*CHB/*"}, Assign: RuleAssign{Category: "rental"}},
	}
	cases := []struct {
		title    string
		wantCat  string
	}{
		{"CHB/2026/00299", "rental"},
		{"Reversal of: CHB/2025/00076, Membership cancelled", "rental"},
		{"Refund for CHB/2024/00100", "rental"},
		{"MEM/2026/00052", ""}, // not a CHB invoice — no category
	}
	for _, tc := range cases {
		mv := OdooOutgoingInvoicePublic{Title: tc.title}
		ApplyMoveRules(&mv, "Acme", moveKindInvoice, rules)
		if mv.Category != tc.wantCat {
			t.Fatalf("title %q: want category %q, got %q", tc.title, tc.wantCat, mv.Category)
		}
	}
}

func TestRuleMatchesProduct(t *testing.T) {
	r := Rule{Match: RuleMatch{Product: "*Open Letter*"}}
	yes := TransactionEntry{Metadata: map[string]interface{}{"product": "Support Open Letter"}}
	no := TransactionEntry{Metadata: map[string]interface{}{"product": "Donation to the Commons Hub Brussels"}}
	if !r.MatchesTransaction(yes) {
		t.Fatal("expected product glob to match 'Support Open Letter'")
	}
	if r.MatchesTransaction(no) {
		t.Fatal("expected product glob NOT to match Commons Hub product")
	}
	// productName fallback key also works.
	alt := TransactionEntry{Metadata: map[string]interface{}{"productName": "Support Open Letter campaign"}}
	if !r.MatchesTransaction(alt) {
		t.Fatal("expected productName key to match")
	}
}
