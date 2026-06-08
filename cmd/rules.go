package cmd

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// RuleMatch defines what a rule matches against.
type RuleMatch struct {
	Sender      string   `json:"sender,omitempty"`      // glob on counterparty for incoming (IBAN, 0xaddr, name)
	Recipient   string   `json:"recipient,omitempty"`   // glob on counterparty for outgoing (IBAN, 0xaddr, name)
	Counterparty string  `json:"counterparty,omitempty"` // glob on counterparty, any direction (use sender/recipient when you only want one direction)
	Description string   `json:"description,omitempty"` // glob on metadata.description / metadata.memo only — does NOT fall back to counterparty (use the counterparty field for that)
	IBAN        string   `json:"iban,omitempty"`        // exact match on counterparty IBAN (spaces stripped, case-insensitive)
	Account     string   `json:"account,omitempty"`     // account slug (fridge, coffee, stripe, savings)
	Collective  string   `json:"collective,omitempty"`  // glob on the collective resolved so far (e.g. "genesis", "*idg*")
	Provider    string   `json:"provider,omitempty"`    // stripe, etherscan, monerium
	Currency    string   `json:"currency,omitempty"`    // EUR, EURe, EURb, CHT
	Amount      *float64 `json:"amount,omitempty"`      // exact signed GROSS amount, rounded to cents
	// MinAmount / MaxAmount are inclusive bounds on the ABSOLUTE
	// gross amount (sign-independent). Use direction:"in" /
	// direction:"out" alongside when you want to scope a range to
	// one direction. Exact-match `Amount` (above) stays
	// signed-gross for back-compat.
	MinAmount   *float64 `json:"amount_min,omitempty"`
	MaxAmount   *float64 `json:"amount_max,omitempty"`
	Direction   string   `json:"direction,omitempty"`   // "in" or "out"
	Application string   `json:"application,omitempty"` // stripe connect app: luma, opencollective, etc.
	PaymentLink string   `json:"paymentLink,omitempty"` // Stripe Checkout payment link ID
	// Kind matches the provider-native classifier stashed in
	// metadata.kind by each provider's generate step. For Stripe this is
	// the reporting_category (charge / fee / payout / refund / …). Useful
	// for catching payouts and other system-driven movements that have no
	// description / IBAN / counterparty for the other matchers to lock onto.
	Kind string `json:"kind,omitempty"`

	// Uncategorized, when true, makes the rule match only if the transaction
	// has no category yet at this point in the rule pass. Because rules are
	// applied in order against the progressively-mutated transaction, this
	// turns a rule into a genuine low-priority catch-all: "apply only if no
	// earlier rule already categorised this row". Pair it with a low position
	// in rules.json so specific rules win first.
	Uncategorized bool `json:"uncategorized,omitempty"`

	// Before / After bound the transaction date (in Europe/Brussels).
	// Format YYYY-MM-DD. Before is exclusive (tx date < Before); After is
	// inclusive (tx date >= After). Use for time-scoped one-off rules, e.g.
	// "round Stripe amounts until end of Feb 2025" → before:"2025-03-01".
	Before string `json:"before,omitempty"`
	After  string `json:"after,omitempty"`

	// Invoice / bill matchers (only meaningful when the rule's Target is
	// "invoice" or "bill"). Title globs the move's printed number /
	// reference (e.g. "MEM/*", "CHB/*"). Partner globs the customer or
	// vendor display name from the private invoice/bill file.
	Title   string `json:"title,omitempty"`
	Partner string `json:"partner,omitempty"`
}

// RuleAssign defines what a matching rule assigns.
// Category/Collective/Event are fallback assignments (only set when empty).
// Type and Description are overrides (always applied when present).
// Tags are additive — merged with whatever the row already carries,
// deduplicated, and never removed by a rule. Useful for orthogonal
// labelling (e.g. ["shifter", "vat:21%"]) that doesn't fit into the
// single-value Category / Collective slots.
type RuleAssign struct {
	Category    string   `json:"category,omitempty"`    // category slug
	Collective  string   `json:"collective,omitempty"`  // collective slug
	Event       string   `json:"event,omitempty"`       // event UID
	Type        string   `json:"type,omitempty"`        // override tx.Type (CREDIT/DEBIT/MINT/BURN/INTERNAL/TRANSFER)
	Description string   `json:"description,omitempty"` // override metadata.description
	Tags        []string `json:"tags,omitempty"`        // additive labels (e.g. "shifter", "vat:21%")
	// Override makes Category/Collective authoritative for this rule: they are
	// applied even when the transaction already carries a value, instead of the
	// default fallback (set-only-if-empty) behaviour. Use sparingly for rules
	// that must win regardless of earlier matches (e.g. "all €121 income is
	// coworking"). Event is still fallback-only.
	Override bool `json:"override,omitempty"`
}

// Rule is a categorization rule.
//
// Target controls which record type the rule fires against:
//
//   - ""           ⇒ "transaction" (default; preserves back-compat)
//   - "transaction" ⇒ ledger transactions (MatchesTransaction)
//   - "invoice"    ⇒ outgoing invoices (MatchesMove with moveKindInvoice)
//   - "bill"       ⇒ vendor bills      (MatchesMove with moveKindBill)
//
// A rule with no target never matches an invoice/bill — that's the
// whole point of the field: existing rules.json files keep working
// untouched, and invoice rules are explicitly opted in.
type Rule struct {
	Target string     `json:"target,omitempty"`
	Match  RuleMatch  `json:"match"`
	Assign RuleAssign `json:"assign"`
}

// ruleTarget normalises the Target field for comparison. Empty / blank
// values default to "transaction" so the existing rules keep firing
// against transactions without needing a migration.
func (r *Rule) ruleTarget() string {
	t := strings.ToLower(strings.TrimSpace(r.Target))
	if t == "" {
		return "transaction"
	}
	return t
}

func rulesPath() string {
	return settingsFilePath("rules.json")
}

// LoadRules reads rules from APP_DATA_DIR/settings/rules.json. The file is
// seeded from the embedded defaults on first run and kept in sync by
// EnsureSettingsBootstrapped when the user hasn't edited it locally.
func LoadRules() ([]Rule, error) {
	data, err := os.ReadFile(rulesPath())
	if err != nil {
		if os.IsNotExist(err) {
			return []Rule{}, nil
		}
		return nil, err
	}
	var rules []Rule
	if err := json.Unmarshal(data, &rules); err != nil {
		return nil, err
	}
	return rules, nil
}

// SaveRules writes rules to APP_DATA_DIR/settings/rules.json.
func SaveRules(rules []Rule) error {
	data, err := json.MarshalIndent(rules, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(rulesPath())
	os.MkdirAll(dir, 0755)
	return os.WriteFile(rulesPath(), data, 0644)
}

// counterpartyMatchTargets returns the lower-cased identifiers a
// sender/recipient/counterparty glob may match against: the counterparty's
// display name, its IBAN (from PII enrichment), and its 0x wallet address
// (from CounterpartyID, token contracts excluded). This is what lets a rule
// key on a bank IBAN or an on-chain address, not just a name — as the
// RuleMatch field docs promise. An empty slice is normalised to a single ""
// target so a non-glob pattern fails (and a "*" pattern still matches) exactly
// as the previous name-only behaviour did.
func counterpartyMatchTargets(tx TransactionEntry) []string {
	var targets []string
	if name := strings.TrimSpace(tx.Counterparty); name != "" {
		targets = append(targets, strings.ToLower(name))
	}
	if iban := normalizeIBAN(stringMetadata(tx.Metadata, "iban")); iban != "" {
		targets = append(targets, strings.ToLower(iban))
	}
	if addr := txCounterpartyAddress(tx); addr != "" {
		targets = append(targets, strings.ToLower(addr))
	}
	if len(targets) == 0 {
		return []string{""}
	}
	return targets
}

// globMatchAny reports whether pattern matches any of the candidate targets.
func globMatchAny(pattern string, targets []string) bool {
	for _, t := range targets {
		if globMatch(pattern, t) {
			return true
		}
	}
	return false
}

// MatchesTransaction checks if a rule matches a transaction.
func (r *Rule) MatchesTransaction(tx TransactionEntry) bool {
	if r.ruleTarget() != "transaction" {
		return false
	}
	m := r.Match

	if m.Account != "" {
		if !strings.EqualFold(m.Account, tx.AccountSlug) {
			return false
		}
	}

	if m.Collective != "" {
		// Glob against the collective resolved so far: the struct field (set by
		// provider enrichment or an earlier rule in this pass), falling back to
		// a "collective" tag or metadata.collective when the field is still
		// empty — some sources carry the collective only as a tag. Matched
		// case-insensitively against the pre-canonicalisation slug, so
		// "collective: genesis" or "collective: *idg*" works.
		coll := tx.Collective
		if coll == "" {
			coll = firstTransactionTagValue(tx, "collective")
		}
		if coll == "" {
			coll = stringMetadata(tx.Metadata, "collective")
		}
		if !globMatch(strings.ToLower(m.Collective), strings.ToLower(coll)) {
			return false
		}
	}

	if m.Provider != "" {
		if !strings.EqualFold(m.Provider, tx.Provider) {
			return false
		}
	}

	if m.Currency != "" {
		if !strings.EqualFold(m.Currency, tx.Currency) {
			return false
		}
	}

	if m.Amount != nil {
		// Match against the signed GROSS amount (txAmount), not net.
		// Operators think in gross — "the €10 subscription rule" should
		// catch a €10 Stripe charge regardless of the ~€0.30 fee that
		// makes the net €9.70.
		if roundCents(txAmount(tx)) != roundCents(*m.Amount) {
			return false
		}
	}

	if m.MinAmount != nil || m.MaxAmount != nil {
		abs := math.Abs(txAmount(tx))
		if m.MinAmount != nil && roundCents(abs) < roundCents(*m.MinAmount) {
			return false
		}
		if m.MaxAmount != nil && roundCents(abs) > roundCents(*m.MaxAmount) {
			return false
		}
	}

	if m.Direction != "" {
		if m.Direction == "in" && !tx.IsIncoming() {
			return false
		}
		if m.Direction == "out" && !tx.IsOutgoing() {
			return false
		}
	}

	if m.Uncategorized && strings.TrimSpace(tx.Category) != "" {
		return false
	}

	if m.Before != "" || m.After != "" {
		txDate := time.Unix(tx.Timestamp, 0).In(BrusselsTZ()).Format("2006-01-02")
		if m.Before != "" && !(txDate < m.Before) { // exclusive upper bound
			return false
		}
		if m.After != "" && txDate < m.After { // inclusive lower bound
			return false
		}
	}

	if m.Application != "" {
		txApp := tx.Application
		if app, ok := tx.Metadata["application"]; ok {
			if s, ok := app.(string); ok {
				txApp = s
			}
		}
		if !strings.EqualFold(m.Application, txApp) {
			return false
		}
	}

	if m.PaymentLink != "" {
		txPaymentLink := firstNonEmptyStripeMetadata(tx.Metadata, "paymentLink", "payment_link")
		if !strings.EqualFold(m.PaymentLink, txPaymentLink) {
			return false
		}
	}

	if m.Kind != "" {
		// metadata.kind is the provider-native classifier — for Stripe this
		// is the reporting_category ("payout", "fee", "charge", "refund").
		// We exact-match (case-insensitive); glob isn't needed here since
		// Stripe's category vocabulary is small and stable.
		if !strings.EqualFold(m.Kind, stringMetadata(tx.Metadata, "kind")) {
			return false
		}
	}

	if m.IBAN != "" {
		txIBAN := normalizeIBAN(stringMetadata(tx.Metadata, "iban"))
		if txIBAN == "" {
			return false
		}
		pattern := normalizeIBAN(m.IBAN)
		if strings.ContainsAny(pattern, "*?") {
			if !globMatch(strings.ToLower(pattern), strings.ToLower(txIBAN)) {
				return false
			}
		} else if txIBAN != pattern {
			return false
		}
	}

	if m.Sender != "" {
		if !tx.IsIncoming() {
			return false
		}
		if !globMatchAny(strings.ToLower(m.Sender), counterpartyMatchTargets(tx)) {
			return false
		}
	}

	if m.Recipient != "" {
		if !tx.IsOutgoing() {
			return false
		}
		if !globMatchAny(strings.ToLower(m.Recipient), counterpartyMatchTargets(tx)) {
			return false
		}
	}

	if m.Counterparty != "" {
		// Direction-agnostic counterparty match. Use sender/recipient when
		// you need to scope to one direction; counterparty matches both.
		if !globMatchAny(strings.ToLower(m.Counterparty), counterpartyMatchTargets(tx)) {
			return false
		}
	}

	if m.Description != "" {
		// Match against metadata.description, then metadata.memo (Monerium
		// SEPA reference, etc.). Does NOT fall back to counterparty — use
		// the `counterparty` field above for that. Keeping the two
		// concerns separate so "match by description" and "match by other
		// party" stay distinguishable in rules.json.
		description := stringMetadata(tx.Metadata, "description")
		if description == "" {
			description = stringMetadata(tx.Metadata, "memo")
		}
		if !globMatch(strings.ToLower(m.Description), strings.ToLower(description)) {
			return false
		}
	}

	return true
}

// RuleSummary returns a human-readable summary of the match conditions.
func (r *Rule) RuleSummary() string {
	var parts []string
	if r.Match.Sender != "" {
		parts = append(parts, fmt.Sprintf("sender: %s", r.Match.Sender))
	}
	if r.Match.Recipient != "" {
		parts = append(parts, fmt.Sprintf("recipient: %s", r.Match.Recipient))
	}
	if r.Match.Counterparty != "" {
		parts = append(parts, fmt.Sprintf("counterparty: %s", r.Match.Counterparty))
	}
	if r.Match.Description != "" {
		parts = append(parts, fmt.Sprintf("description: %s", r.Match.Description))
	}
	if r.Match.Account != "" {
		parts = append(parts, fmt.Sprintf("account: %s", r.Match.Account))
	}
	if r.Match.Provider != "" {
		parts = append(parts, fmt.Sprintf("provider: %s", r.Match.Provider))
	}
	if r.Match.Currency != "" {
		parts = append(parts, fmt.Sprintf("currency: %s", r.Match.Currency))
	}
	if r.Match.Amount != nil {
		parts = append(parts, fmt.Sprintf("amount: %.2f", *r.Match.Amount))
	}
	if r.Match.MinAmount != nil {
		parts = append(parts, fmt.Sprintf("amount ≥ %.2f", *r.Match.MinAmount))
	}
	if r.Match.MaxAmount != nil {
		parts = append(parts, fmt.Sprintf("amount ≤ %.2f", *r.Match.MaxAmount))
	}
	if r.Match.Direction != "" {
		parts = append(parts, fmt.Sprintf("direction: %s", r.Match.Direction))
	}
	if r.Match.Application != "" {
		parts = append(parts, fmt.Sprintf("app: %s", r.Match.Application))
	}
	if r.Match.PaymentLink != "" {
		parts = append(parts, fmt.Sprintf("paymentLink: %s", r.Match.PaymentLink))
	}
	if r.Match.IBAN != "" {
		parts = append(parts, fmt.Sprintf("iban: %s", r.Match.IBAN))
	}
	if r.Match.Kind != "" {
		parts = append(parts, fmt.Sprintf("kind: %s", r.Match.Kind))
	}
	if r.Match.Title != "" {
		parts = append(parts, fmt.Sprintf("title: %s", r.Match.Title))
	}
	if r.Match.Partner != "" {
		parts = append(parts, fmt.Sprintf("partner: %s", r.Match.Partner))
	}
	if len(parts) == 0 {
		return "(no conditions)"
	}
	return strings.Join(parts, ", ")
}

// MatchesMove checks whether the rule applies to an invoice / bill row.
// Returns false unless r.Target is "invoice" / "bill" AND matches kind.
// A rule with no match conditions matches every row of its target type —
// the canonical pattern for a default-assign rule (e.g. "every invoice
// gets collective=commonshub unless overridden").
func (r *Rule) MatchesMove(m OdooOutgoingInvoicePublic, partner string, kind moveKind) bool {
	target := r.ruleTarget()
	if kind.isBill {
		if target != "bill" {
			return false
		}
	} else {
		if target != "invoice" {
			return false
		}
	}

	if r.Match.Title != "" {
		if !globMatch(strings.ToLower(r.Match.Title), strings.ToLower(strings.TrimSpace(m.Title))) {
			return false
		}
	}

	if r.Match.Partner != "" {
		if !globMatch(strings.ToLower(r.Match.Partner), strings.ToLower(strings.TrimSpace(partner))) {
			return false
		}
	}

	if r.Match.Description != "" {
		// For moves, "description" matches the FIRST non-section
		// line item only — same text the operator sees in the
		// "Description" column of `chb invoices`. We deliberately
		// don't scan the whole line-item haystack: an invoice
		// titled "Ostrom Event Space" + line 2 "Coffee, tea" would
		// otherwise match a `*Coffee*` rule meant for coffee
		// invoices.
		hay := strings.ToLower(moveFirstLineItem(m))
		if !globMatch(strings.ToLower(r.Match.Description), hay) {
			return false
		}
	}

	if r.Match.Currency != "" {
		if !strings.EqualFold(r.Match.Currency, m.Currency) {
			return false
		}
	}

	if r.Match.Amount != nil {
		if roundCents(m.TotalAmount) != roundCents(*r.Match.Amount) {
			return false
		}
	}

	if r.Match.MinAmount != nil || r.Match.MaxAmount != nil {
		abs := math.Abs(m.TotalAmount)
		if r.Match.MinAmount != nil && roundCents(abs) < roundCents(*r.Match.MinAmount) {
			return false
		}
		if r.Match.MaxAmount != nil && roundCents(abs) > roundCents(*r.Match.MaxAmount) {
			return false
		}
	}

	return true
}

// ApplyMoveRules walks every rule whose target matches the kind and
// fills in any (collective, category) the move doesn't already have.
// Tags are additive — merged from every matching rule, deduplicated,
// and never removed. Rules without conditions act as default-
// assigners — keep them at the END of rules.json so explicit
// overrides win first.
//
// Mutates m in place.
func ApplyMoveRules(m *OdooOutgoingInvoicePublic, partner string, kind moveKind, rules []Rule) {
	for _, r := range rules {
		if !r.MatchesMove(*m, partner, kind) {
			continue
		}
		if m.Collective == "" && r.Assign.Collective != "" {
			m.Collective = r.Assign.Collective
		}
		if m.Category == "" && r.Assign.Category != "" {
			m.Category = r.Assign.Category
		}
		for _, t := range r.Assign.Tags {
			t = strings.TrimSpace(t)
			if t == "" {
				continue
			}
			if !containsString(m.Tags, t) {
				m.Tags = append(m.Tags, t)
			}
		}
		// NOTE: we do NOT early-return when collective+category are
		// both set — later rules can still contribute additional
		// tags. The first-matching-rule-wins-per-field semantic only
		// applies to the singular fields.
	}
}

// containsString reports whether haystack contains needle (exact,
// case-sensitive). Used for tag dedupe in ApplyMoveRules.
func containsString(haystack []string, needle string) bool {
	for _, h := range haystack {
		if h == needle {
			return true
		}
	}
	return false
}
