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
