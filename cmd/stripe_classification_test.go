package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	stripesource "github.com/CommonsHub/chb/providers/stripe"
)

// stripeTestCategorizer builds the categorizer + Odoo account mappings that
// these cases exercise, seeded inline so the test is hermetic (the test harness
// isolates settings into a temp dir, so the live config isn't readable). The
// rules mirror the production rules.json entries for these flows, in the same
// order (payment-link rules authoritative; product rules are the uncategorized
// fallback).
func stripeTestCategorizer() (*Categorizer, []OdooMapping) {
	const (
		openLetterPlink = "plink_1TGev1FAhaWeDyowQqEek3mT"
		commonsHubPlink = "plink_1R7tuJFAhaWeDyowGmrFTAYd"
	)
	amt := func(v float64) *float64 { return &v }
	rules := []Rule{
		// Payment-link rules win (authoritative, applied first).
		{Match: RuleMatch{Provider: "stripe", PaymentLink: commonsHubPlink, Currency: "EUR", Amount: amt(10), Direction: "in"}, Assign: RuleAssign{Category: "donation", Collective: "openletter"}},
		{Match: RuleMatch{Provider: "stripe", PaymentLink: openLetterPlink, Direction: "in"}, Assign: RuleAssign{Category: "donation", Collective: "openletter"}},
		// Membership subscription.
		{Match: RuleMatch{Provider: "stripe", Description: "Subscription update*", Currency: "EUR", Amount: amt(10), Direction: "in"}, Assign: RuleAssign{Category: "membership", Collective: "commonshub"}},
		// Income categories by description.
		{Match: RuleMatch{Description: "*room*"}, Assign: RuleAssign{Category: "rental", Collective: "commonshub"}},
		{Match: RuleMatch{Provider: "stripe", Description: "*Daily pass*", Direction: "in"}, Assign: RuleAssign{Category: "coworking", Collective: "commonshub"}},
		{Match: RuleMatch{Description: "*sponsor*"}, Assign: RuleAssign{Category: "sponsoring", Collective: "commonshub"}},
		{Match: RuleMatch{Description: "*catering*"}, Assign: RuleAssign{Category: "catering", Collective: "commonshub"}},
		{Match: RuleMatch{Description: "*grant*"}, Assign: RuleAssign{Category: "grant"}},
		// Fridge / drinks orders are INCOME (CP-ORDER references a fridge sale).
		{Match: RuleMatch{Description: "*CP-ORDER-*"}, Assign: RuleAssign{Category: "drinks", Collective: "commonshub"}},
		// System kinds.
		{Match: RuleMatch{Provider: "stripe", Kind: "fee"}, Assign: RuleAssign{Category: "stripe_fee"}},
		{Match: RuleMatch{Application: "luma"}, Assign: RuleAssign{Category: "ticket"}},
		// Payout → internal transfer (covers the Brussels Pay / Zinne debt-token
		// top-up payouts too — they are Stripe payouts).
		{Match: RuleMatch{Provider: "stripe", Kind: "payout"}, Assign: RuleAssign{Category: "internal_transfer", Type: "INTERNAL"}},
		// Product fallback (only when nothing above categorised it).
		{Match: RuleMatch{Provider: "stripe", Product: "*Commons Hub*", Direction: "in", Uncategorized: true}, Assign: RuleAssign{Category: "donation", Collective: "commonshub"}},
		{Match: RuleMatch{Provider: "stripe", Product: "*Open Letter*", Direction: "in", Uncategorized: true}, Assign: RuleAssign{Category: "donation", Collective: "openletter"}},
	}
	mappings := []OdooMapping{
		{Match: OdooMappingMatch{Category: "donation"}, Set: OdooMappingResult{AccountCode: "740040"}},
		{Match: OdooMappingMatch{Category: "membership", Direction: "in"}, Set: OdooMappingResult{AccountCode: "730000"}},
		{Match: OdooMappingMatch{Category: "ticket", Direction: "in"}, Set: OdooMappingResult{AccountCode: "700150"}},
		{Match: OdooMappingMatch{Category: "rental", Direction: "in"}, Set: OdooMappingResult{AccountCode: "700003"}},
		{Match: OdooMappingMatch{Category: "coworking", Direction: "in"}, Set: OdooMappingResult{AccountCode: "700003"}},
		{Match: OdooMappingMatch{Category: "drinks", Direction: "in"}, Set: OdooMappingResult{AccountCode: "700003"}},
		{Match: OdooMappingMatch{Category: "sponsoring", Direction: "in"}, Set: OdooMappingResult{AccountCode: "700110"}},
		{Match: OdooMappingMatch{Category: "catering", Direction: "in"}, Set: OdooMappingResult{AccountCode: "700002"}},
		{Match: OdooMappingMatch{Category: "grant", Direction: "in"}, Set: OdooMappingResult{AccountCode: "740041"}},
		{Match: OdooMappingMatch{Category: "stripe_fee", Direction: "out"}, Set: OdooMappingResult{AccountCode: "657020"}},
		{Match: OdooMappingMatch{Category: "internal_transfer"}, Set: OdooMappingResult{AccountCode: "580000"}},
	}
	return &Categorizer{rules: rules, categories: map[string]CategoryDef{}}, mappings
}

// TestStripeClassification locks in the category / collective / counterpart
// account (and, where relevant, the payment_ref) for a set of known Stripe
// transactions. Synthetic balance transactions / charges carry the same signals
// the live ones do (payment link, product, kind, description, customer id), run
// through the real enrichment + rule engine + mapping. No Odoo API calls.
func TestStripeClassification(t *testing.T) {
	cat, mappings := stripeTestCategorizer()
	acc := &AccountConfig{Provider: "stripe", Slug: "stripe", AccountID: "acct_1Nn0FaFAhaWeDyow"}

	const (
		openLetterPlink = "plink_1TGev1FAhaWeDyowQqEek3mT"
		commonsHubPlink = "plink_1R7tuJFAhaWeDyowGmrFTAYd"
	)

	cases := []struct {
		name         string
		bt           stripesource.Transaction
		charge       *stripesource.Charge
		wantCat      string
		wantColl     string // "" → don't assert
		wantAccount  string
		wantRef      string // "" → don't assert
		wantPlink    string // "" → don't assert metadata["paymentLink"]
		wantCustID   string // "" → don't assert metadata["stripeCustomerId"]
		wantType     string // "" → don't assert tx.Type
		refundID     string // refund id (re_…) in the BT source, for refund cases
		refundOf     string // original charge id the refund maps to
		wantNegative bool   // assert the booked statement amount is negative
	}{
		{
			name:    "payment-link openletter → donation/openletter/740040",
			bt:      stripesource.Transaction{ID: "txn_openletter", Type: "charge", Currency: "EUR", Amount: 1000, Net: 1000, ChargeID: "ch_openletter"},
			charge:  &stripesource.Charge{ID: "ch_openletter", PaymentLink: openLetterPlink},
			wantCat: "donation", wantColl: "openletter", wantAccount: "740040",
		},
		{
			// py_ payment object (SEPA/bank): ExtractChargeID must accept py_
			// so the BT links to its charge and inherits the payment link.
			name:    "txn_3Th3vf py_ payment links to openletter plink",
			bt:      stripesource.Transaction{ID: "txn_3Th3vfFAhaWeDyow0WUUFXP3", Type: "payment", Currency: "EUR", Amount: 1000, Net: 1000, Source: json.RawMessage(`"py_3Th3vfFAhaWeDyow0hLS7Zso"`)},
			charge:  &stripesource.Charge{ID: "py_3Th3vfFAhaWeDyow0hLS7Zso", PaymentLink: openLetterPlink},
			wantCat: "donation", wantColl: "openletter", wantAccount: "740040", wantPlink: openLetterPlink,
		},
		{
			// "€10,000 free Credit" fee credit: type adjustment / reporting_category fee.
			name:    "txn_1Th5UQ fee credit → stripe_fee/657020 keeps its description",
			bt:      stripesource.Transaction{ID: "txn_1Th5UQFAhaWeDyowaBJw7ipN", Type: "adjustment", ReportingCategory: "fee", Currency: "EUR", Amount: 164, Net: 164, Description: "€10,000 free Credit"},
			wantCat: "stripe_fee", wantAccount: "657020", wantRef: "€10,000 free Credit",
		},
		{
			name:    "txn_3ThCT0 subscription → membership/commonshub/730000",
			bt:      stripesource.Transaction{ID: "txn_3ThCT0FAhaWeDyow0lkw3S60", Type: "charge", Currency: "EUR", Amount: 1000, Net: 1000, Description: "Subscription update", ChargeID: "ch_3ThCT0FAhaWeDyow02g3rZTV"},
			wantCat: "membership", wantColl: "commonshub", wantAccount: "730000",
		},
		{
			name:    "txn_3TgJaS luma event → ticket/700150, payment_ref = event name",
			bt:      stripesource.Transaction{ID: "txn_3TgJaSFAhaWeDyow1m2cEhZD", Type: "payment", Currency: "EUR", Amount: 1210, Net: 1210, Description: "An Economy of Better for Europe", Source: json.RawMessage(`"py_3TgJaSFAhaWeDyow1tpF6vkN"`)},
			charge:  &stripesource.Charge{ID: "py_3TgJaSFAhaWeDyow1tpF6vkN", ApplicationName: "luma"},
			wantCat: "ticket", wantAccount: "700150", wantRef: "An Economy of Better for Europe",
		},
		{
			// €8 on the shared Commons-Hub link: the €10→openletter payment-link
			// rule does NOT match, so the product name is the fallback signal.
			name:    "ch_3Thwb5 commons-hub product → donation/commonshub/740040 + customer id",
			bt:      stripesource.Transaction{ID: "txn_ch3Thwb5", Type: "charge", Currency: "EUR", Amount: 800, Net: 800, ChargeID: "ch_3Thwb5FAhaWeDyow1icqQCYr"},
			charge:  &stripesource.Charge{ID: "ch_3Thwb5FAhaWeDyow1icqQCYr", PaymentLink: commonsHubPlink, ProductName: "Donation to the Commons Hub Brussels", CustomerID: "gcus_1QXSOSFAhaWeDyowXUztiDtM"},
			wantCat: "donation", wantColl: "commonshub", wantAccount: "740040", wantCustID: "gcus_1QXSOSFAhaWeDyowXUztiDtM",
		},
		{
			// Processing fee debit (type stripe_fee, negative): the most common fee.
			name:    "stripe_fee debit (processing fee) → stripe_fee/657020",
			bt:      stripesource.Transaction{ID: "txn_feeDebit", Type: "stripe_fee", ReportingCategory: "fee", Currency: "EUR", Amount: -50, Net: -50, Description: "Billing - Usage Fee (2026-06-24)"},
			wantCat: "stripe_fee", wantAccount: "657020", wantRef: "Billing - Usage Fee (2026-06-24)",
		},
		{
			name:    "room booking → rental/commonshub/700003",
			bt:      stripesource.Transaction{ID: "txn_rental", Type: "charge", Currency: "EUR", Amount: 5000, Net: 5000, Description: "Meeting room - 2nd floor", ChargeID: "ch_rental"},
			wantCat: "rental", wantColl: "commonshub", wantAccount: "700003",
		},
		{
			name:    "daily pass → coworking/commonshub/700003",
			bt:      stripesource.Transaction{ID: "txn_cowork", Type: "charge", Currency: "EUR", Amount: 1500, Net: 1500, Description: "Daily pass", ChargeID: "ch_cowork"},
			wantCat: "coworking", wantColl: "commonshub", wantAccount: "700003",
		},
		{
			name:    "sponsorship → sponsoring/commonshub/700110",
			bt:      stripesource.Transaction{ID: "txn_sponsor", Type: "charge", Currency: "EUR", Amount: 50000, Net: 50000, Description: "Sponsorship contribution", ChargeID: "ch_sponsor"},
			wantCat: "sponsoring", wantColl: "commonshub", wantAccount: "700110",
		},
		{
			// Payout → internal transfer. metadata.kind is what `chb generate`
			// stamps from reporting_category; the rule keys off it.
			name:    "automatic payout → internal_transfer/580000 (type INTERNAL)",
			bt:      stripesource.Transaction{ID: "txn_payout", Type: "payout", ReportingCategory: "payout", Currency: "EUR", Amount: -100000, Net: -100000, Metadata: map[string]interface{}{"kind": "payout"}},
			wantCat: "internal_transfer", wantAccount: "580000", wantType: "INTERNAL",
		},
		{
			// EURb / Zinne debt-token top-up is a Stripe payout too.
			name:    "zinne income payout → internal_transfer/580000",
			bt:      stripesource.Transaction{ID: "txn_1Qajw2FAhaWeDyowv2MawShT", Type: "payout", ReportingCategory: "payout", Currency: "EUR", Amount: -58783, Net: -58783, Description: "2024 zinne income", Metadata: map[string]interface{}{"kind": "payout"}},
			wantCat: "internal_transfer", wantAccount: "580000", wantType: "INTERNAL",
		},
		{
			name:    "CP-ORDER fridge sale → drinks/commonshub/700003 (income)",
			bt:      stripesource.Transaction{ID: "txn_cporder", Type: "charge", Currency: "EUR", Amount: 350, Net: 350, Description: "CP-ORDER-1234", ChargeID: "ch_cporder"},
			wantCat: "drinks", wantColl: "commonshub", wantAccount: "700003",
		},
		{
			name:    "research grant → grant/740041",
			bt:      stripesource.Transaction{ID: "txn_grant", Type: "charge", Currency: "EUR", Amount: 250000, Net: 250000, Description: "Research grant 2026", ChargeID: "ch_grant"},
			wantCat: "grant", wantAccount: "740041",
		},
		{
			name:    "catering income → catering/commonshub/700002",
			bt:      stripesource.Transaction{ID: "txn_catering", Type: "charge", Currency: "EUR", Amount: 12000, Net: 12000, Description: "Catering for workshop", ChargeID: "ch_catering"},
			wantCat: "catering", wantColl: "commonshub", wantAccount: "700002",
		},
		{
			// Refund of a donation: inherits the original charge's classification
			// (→ same account 740040) and is booked as a DEBIT (negative).
			name:     "refund of openletter donation → donation/740040, negative",
			bt:       stripesource.Transaction{ID: "txn_refund", Type: "refund", ReportingCategory: "refund", Currency: "EUR", Amount: -1000, Net: -1000, Source: json.RawMessage(`"re_1AbcRefund"`)},
			charge:   &stripesource.Charge{ID: "ch_origDonation", PaymentLink: openLetterPlink},
			refundOf: "ch_origDonation", refundID: "re_1AbcRefund",
			wantCat: "donation", wantColl: "openletter", wantAccount: "740040", wantNegative: true,
		},
		{
			// Chargeback (type adjustment, du_ source): the originating charge id
			// lives only in the description. It must resolve to the same account as
			// the disputed charge (openletter donation) and book negative.
			name:     "chargeback of openletter donation → donation/740040, negative",
			bt:       stripesource.Transaction{ID: "txn_chargeback", Type: "adjustment", Currency: "EUR", Amount: -500, Net: -500, Source: json.RawMessage(`"du_1TYdcvFAhaWeDyowThUJdScR"`), Description: "Chargeback withdrawal for ch_origDonation"},
			charge:   &stripesource.Charge{ID: "ch_origDonation", PaymentLink: openLetterPlink},
			wantCat:  "donation", wantColl: "openletter", wantAccount: "740040", wantNegative: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			bt := c.bt
			// Resolve the originating charge id exactly as the push does: customer
			// refunds via the re_→charge map, chargebacks via the ch_ in their
			// description.
			refundMap := map[string]string{}
			if c.refundID != "" && c.refundOf != "" {
				refundMap[c.refundID] = c.refundOf
			}
			if bt.ChargeID == "" {
				if cid := resolveStripeReversalChargeID(bt, refundMap); cid != "" {
					bt.ChargeID = cid
				}
			}
			if c.charge != nil {
				bt = enrichStripeBTFromCharge(bt, map[string]*stripesource.Charge{c.charge.ID: c.charge})
			}
			if c.wantPlink != "" {
				if got := stringMetadata(bt.Metadata, "paymentLink"); got != c.wantPlink {
					t.Fatalf("paymentLink = %q, want %q (charge link not inherited)", got, c.wantPlink)
				}
			}
			if c.wantCustID != "" {
				if got := stringMetadata(bt.Metadata, "stripeCustomerId"); got != c.wantCustID {
					t.Fatalf("stripeCustomerId = %q, want %q", got, c.wantCustID)
				}
			}
			amount := stripeStatementLineAmount(bt)
			tx := stripeClassificationTransaction(acc, bt, amount)
			cat.Apply(&tx)
			if c.wantNegative && amount >= 0 {
				t.Errorf("booked amount = %.2f, want negative (refund)", amount)
			}

			if tx.Category != c.wantCat {
				t.Errorf("category = %q, want %q", tx.Category, c.wantCat)
			}
			if c.wantColl != "" && tx.Collective != c.wantColl {
				t.Errorf("collective = %q, want %q", tx.Collective, c.wantColl)
			}
			if c.wantType != "" && tx.Type != c.wantType {
				t.Errorf("type = %q, want %q", tx.Type, c.wantType)
			}
			if got := stripeOdooAccountCode(bt, tx, mappings); got != c.wantAccount {
				t.Errorf("account = %q, want %q", got, c.wantAccount)
			}
			if c.wantRef != "" {
				if got := stripeOdooPaymentRef(bt, tx); got != c.wantRef {
					t.Errorf("payment_ref = %q, want %q", got, c.wantRef)
				}
			}
		})
	}
}

// TestReconcilePartnerCreateOrReuse verifies the reconcile partner-linking
// planner (local only — no Odoo calls): a Stripe charge's customer id is used
// to reuse an existing partner when one already carries it, or to plan a new
// partner carrying that customer id when none matches.
func TestReconcilePartnerCreateOrReuse(t *testing.T) {
	const custID = "gcus_1QXSOSFAhaWeDyowXUztiDtM"
	line := noPartnerLine{ID: 1, Name: "F. Pierik", AccountNumber: custID}

	// Reuse: an existing partner (#77) already carries this customer id as a bank.
	reuse := planPartnerLinks([]noPartnerLine{line},
		func(a string) int {
			if normalizeBankAccountNumber(a) == normalizeBankAccountNumber(custID) {
				return 77
			}
			return 0
		},
		func(string) int { return 0 },
	)
	if got, ok := reuse.ToExisting[1]; !ok || got.PartnerID != 77 {
		t.Fatalf("reuse: line should map to existing partner #77, got %+v", reuse.ToExisting)
	}
	if len(reuse.NewGroups) != 0 {
		t.Fatalf("reuse: no new partner should be planned, got %+v", reuse.NewGroups)
	}

	// Create: nothing matches by customer id or name → plan a new partner that
	// carries the customer id as an account to attach.
	create := planPartnerLinks([]noPartnerLine{line},
		func(string) int { return 0 },
		func(string) int { return 0 },
	)
	if len(create.NewGroups) != 1 {
		t.Fatalf("create: expected exactly one new partner group, got %+v", create.NewGroups)
	}
	for _, g := range create.NewGroups {
		if !g.Accounts[custID] {
			t.Fatalf("create: new partner must carry the customer id %q as an account, got %+v", custID, g.Accounts)
		}
	}
}

// TestLiveStripeDonationsCategorized validates the ACTUAL generated output (not
// a seeded fixture): every transaction carrying an openletter/commonshub Stripe
// payment link must resolve to donation + the right collective. This is the
// guard the hermetic tests can't be — it catches live-config drift and stale
// generated data (e.g. an old month a pull backfilled but generate didn't
// reprocess). Set CHB_LIVE_DATA_DIR=$HOME/.chb/data and run after `generate`.
func TestLiveStripeDonationsCategorized(t *testing.T) {
	dataDir := os.Getenv("CHB_LIVE_DATA_DIR")
	if dataDir == "" {
		t.Skip("set CHB_LIVE_DATA_DIR=$HOME/.chb/data to validate the live generated output")
	}
	const (
		openLetterPlink = "plink_1TGev1FAhaWeDyowQqEek3mT"
		commonsHubPlink = "plink_1R7tuJFAhaWeDyowGmrFTAYd"
	)
	files, _ := filepath.Glob(filepath.Join(dataDir, "*", "*", "generated", "transactions.json"))
	if len(files) == 0 {
		t.Fatalf("no generated transactions.json under %s — run `chb generate --history` first", dataDir)
	}
	checked := 0
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		var txs []TransactionEntry
		if json.Unmarshal(data, &txs) != nil {
			var wrap struct {
				Transactions []TransactionEntry `json:"transactions"`
			}
			if json.Unmarshal(data, &wrap) != nil {
				continue
			}
			txs = wrap.Transactions
		}
		for i := range txs {
			tx := txs[i]
			pl := stringMetadata(tx.Metadata, "paymentLink")
			if pl != openLetterPlink && pl != commonsHubPlink {
				continue
			}
			checked++
			if got := txDisplayCategory(tx); got != "donation" {
				t.Errorf("%s (%s): category=%q, want donation\n  %s", tx.ID, pl, got, f)
			}
			wantColl := "openletter"
			if pl == commonsHubPlink && tx.Amount != 10 { // €10 on this link is openletter; the rest is commonshub
				wantColl = "commonshub"
			}
			if got := txDisplayCollective(tx); got != wantColl {
				t.Errorf("%s (%s, €%.0f): collective=%q, want %q\n  %s", tx.ID, pl, tx.Amount, got, wantColl, f)
			}
		}
	}
	if checked == 0 {
		t.Skip("no openletter/commonshub payment-link transactions in the live data")
	}
	t.Logf("validated %d openletter/commonshub donation transactions", checked)
}
