package cmd

import (
	"fmt"
	"strings"
)

// RulesAdd appends a new categorization rule binding a counterparty
// identifier to a (collective, category) assignment.
//
//	chb rules add <identifier> [--collective=<slug>] [--category=<slug>] [--direction=in|out]
//
// Identifier types:
//   - IBAN          → match.iban (direction-agnostic; works for both in/out)
//   - cus_…  (Stripe) or 0x… (Ethereum) → require --direction=in or out
func RulesAdd(args []string) {
	if HasFlag(args, "--help", "-h", "help") {
		printRulesAddHelp()
		return
	}

	identifier := ""
	category := GetOption(args, "--category")
	collective := GetOption(args, "--collective")
	direction := GetOption(args, "--direction")

	// First non-flag arg is the identifier.
	for i := 0; i < len(args); i++ {
		a := args[i]
		if strings.HasPrefix(a, "--") || strings.HasPrefix(a, "-") {
			// Flag-value pairs that aren't `--flag=value`.
			if !strings.Contains(a, "=") {
				i++
			}
			continue
		}
		identifier = a
		break
	}

	if identifier == "" {
		Errorf("%s✗ chb rules add requires an identifier (IBAN, cus_…, or 0x…)%s", Fmt.Red, Fmt.Reset)
		return
	}
	if category == "" && collective == "" {
		Errorf("%s✗ chb rules add requires at least one of --category=<slug> or --collective=<slug>%s", Fmt.Red, Fmt.Reset)
		return
	}

	rule, err := buildCounterpartyRule(identifier, direction, category, collective)
	if err != nil {
		Errorf("%s✗ %v%s", Fmt.Red, err, Fmt.Reset)
		return
	}

	rules, err := LoadRules()
	if err != nil {
		Errorf("%s✗ load rules: %v%s", Fmt.Red, err, Fmt.Reset)
		return
	}

	// If a rule already targets the same counterparty (same Match
	// struct), merge the new Assign into it rather than appending a
	// duplicate. Lets the user build up `--category` and `--collective`
	// in separate invocations without churn in rules.json.
	action := "added"
	existingIdx := findRuleByMatch(rules, rule.Match)
	if existingIdx >= 0 {
		merged := mergeRuleAssign(rules[existingIdx].Assign, rule.Assign)
		rules[existingIdx].Assign = merged
		rule = rules[existingIdx]
		action = "merged into existing rule"
	} else {
		rules = append(rules, rule)
	}

	if err := SaveRules(rules); err != nil {
		Errorf("%s✗ save rules: %v%s", Fmt.Red, err, Fmt.Reset)
		return
	}

	fmt.Printf("\n%s✓ Rule %s%s — %d total rules in %s\n\n", Fmt.Green, action, Fmt.Reset, len(rules), rulesPath())
	fmt.Printf("  %sMatch:%s   %s\n", Fmt.Dim, Fmt.Reset, describeRuleMatch(rule.Match))
	fmt.Printf("  %sAssign:%s  %s\n\n", Fmt.Dim, Fmt.Reset, describeRuleAssign(rule.Assign))
}

// findRuleByMatch returns the index of the first rule whose Match
// struct equals m exactly, or -1 if none. Used by `chb rules add` to
// merge rather than duplicate when targeting the same counterparty.
func findRuleByMatch(rules []Rule, m RuleMatch) int {
	for i := range rules {
		if rules[i].Match == m {
			return i
		}
	}
	return -1
}

// mergeRuleAssign overlays the new Assign on top of an existing one:
// non-empty fields in `new` win, empty fields fall back to `existing`.
// This means a follow-up `chb rules add --collective=X` adds the
// collective without wiping the category set earlier.
func mergeRuleAssign(existing, new RuleAssign) RuleAssign {
	out := existing
	if new.Category != "" {
		out.Category = new.Category
	}
	if new.Collective != "" {
		out.Collective = new.Collective
	}
	if new.Event != "" {
		out.Event = new.Event
	}
	if new.Type != "" {
		out.Type = new.Type
	}
	if new.Description != "" {
		out.Description = new.Description
	}
	return out
}

func buildCounterpartyRule(identifier, direction, category, collective string) (Rule, error) {
	rule := Rule{Assign: RuleAssign{Category: category, Collective: collective}}

	normalizedIBAN := normalizeIBAN(identifier)
	switch {
	case looksLikeIBAN(normalizedIBAN):
		rule.Match.IBAN = normalizedIBAN
		// IBAN works for either direction — Monerium counterparts come
		// through whether the user is sending or receiving — so we
		// don't force --direction here. If the caller supplied one,
		// honour it as an extra filter.
		if direction != "" {
			if direction != "in" && direction != "out" {
				return rule, fmt.Errorf("--direction must be 'in' or 'out', got %q", direction)
			}
			rule.Match.Direction = direction
		}
	case strings.HasPrefix(identifier, "cus_") || looksLikeHexAddress(identifier):
		if direction == "" {
			return rule, fmt.Errorf("--direction=in|out is required for %s (the same address can appear as both sender and recipient)", identifier)
		}
		if direction != "in" && direction != "out" {
			return rule, fmt.Errorf("--direction must be 'in' or 'out', got %q", direction)
		}
		rule.Match.Direction = direction
		if direction == "in" {
			rule.Match.Sender = identifier
		} else {
			rule.Match.Recipient = identifier
		}
	default:
		return rule, fmt.Errorf("unrecognized identifier format %q — expected IBAN, cus_…, or 0x…", identifier)
	}
	return rule, nil
}

// looksLikeIBAN does a structural check (no checksum verification): 2
// letters + 2 digits + 11–30 alphanumeric, total 15–34 chars.
func looksLikeIBAN(s string) bool {
	if len(s) < 15 || len(s) > 34 {
		return false
	}
	if !isASCIIAlpha(s[0]) || !isASCIIAlpha(s[1]) {
		return false
	}
	if !isASCIIDigit(s[2]) || !isASCIIDigit(s[3]) {
		return false
	}
	for i := 4; i < len(s); i++ {
		c := s[i]
		if !isASCIIAlpha(c) && !isASCIIDigit(c) {
			return false
		}
	}
	return true
}

func isASCIIAlpha(b byte) bool { return b >= 'A' && b <= 'Z' }
func isASCIIDigit(b byte) bool { return b >= '0' && b <= '9' }

// looksLikeHexAddress accepts an EVM-style address: `0x` + 40 hex digits.
func looksLikeHexAddress(s string) bool {
	if len(s) != 42 {
		return false
	}
	if s[0] != '0' || (s[1] != 'x' && s[1] != 'X') {
		return false
	}
	for i := 2; i < len(s); i++ {
		c := s[i]
		isHex := (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
		if !isHex {
			return false
		}
	}
	return true
}

func describeRuleMatch(m RuleMatch) string {
	parts := []string{}
	if m.IBAN != "" {
		parts = append(parts, "iban="+m.IBAN)
	}
	if m.Sender != "" {
		parts = append(parts, "sender="+m.Sender)
	}
	if m.Recipient != "" {
		parts = append(parts, "recipient="+m.Recipient)
	}
	if m.Description != "" {
		parts = append(parts, "description="+m.Description)
	}
	if m.Direction != "" {
		parts = append(parts, "direction="+m.Direction)
	}
	if m.Currency != "" {
		parts = append(parts, "currency="+m.Currency)
	}
	if m.Account != "" {
		parts = append(parts, "account="+m.Account)
	}
	if m.Provider != "" {
		parts = append(parts, "provider="+m.Provider)
	}
	if m.Application != "" {
		parts = append(parts, "application="+m.Application)
	}
	if len(parts) == 0 {
		return "(empty — would match every tx)"
	}
	return strings.Join(parts, ", ")
}

func describeRuleAssign(a RuleAssign) string {
	parts := []string{}
	if a.Category != "" {
		parts = append(parts, "category="+a.Category)
	}
	if a.Collective != "" {
		parts = append(parts, "collective="+a.Collective)
	}
	if a.Event != "" {
		parts = append(parts, "event="+a.Event)
	}
	if a.Type != "" {
		parts = append(parts, "type="+a.Type)
	}
	if a.Description != "" {
		parts = append(parts, "description="+a.Description)
	}
	if len(parts) == 0 {
		return "(empty — would assign nothing)"
	}
	return strings.Join(parts, ", ")
}

func printRulesAddHelp() {
	f := Fmt
	fmt.Printf(`
%schb rules add%s — Append a categorization rule for a counterparty

%sUSAGE%s
  %schb rules add%s <identifier> [--collective=<slug>] [--category=<slug>] [--direction=in|out]

%sIDENTIFIER TYPES%s
  IBAN           Direction-agnostic (Monerium counterpart; works in & out).
                 e.g. BE31891874100655
  cus_…          Stripe customer — requires --direction.
  0x…            Ethereum address — requires --direction.

%sEXAMPLES%s
  %schb rules add%s BE31891874100655 --collective=commonshub --category=cleaning
  %schb rules add%s cus_QbkYjHh3CFevdN --direction=in --collective=commonshub --category=tickets
  %schb rules add%s 0x6fdf0aae33e313d9c98d2aa19bcd8ef777912cbf --direction=in --category=savings

%sOPTIONS%s
  %s--category%s <slug>    Assign this category (fallback — only when tx has no category)
  %s--collective%s <slug>  Assign this collective
  %s--direction%s <in|out> Required for cus_ and 0x identifiers
  %s--help, -h%s           Show this help

Rules are appended to %s.
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		rulesPath(),
	)
}
