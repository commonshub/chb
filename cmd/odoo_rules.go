package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// OdooRule applies during Odoo sync: when a transaction matches the
// criteria in Match, the values in Set are pushed onto the resulting
// Odoo statement line (partner_id) and the counterpart move line
// (account_id, resolved from AccountCode).
//
// Both PartnerName and AccountName are human-readable caches stored
// alongside the IDs to keep `chb odoo rules` reviewable without
// hitting Odoo on every list.
type OdooRule struct {
	Match OdooRuleMatch `json:"match"`
	Set   OdooRuleSet   `json:"set"`
}

// OdooRuleMatch — currently matches by category, collective, and
// optionally tx direction ("in" = CREDIT/MINT, "out" = DEBIT/BURN).
// An empty value means "don't constrain".
type OdooRuleMatch struct {
	Category   string `json:"category,omitempty"`
	Collective string `json:"collective,omitempty"`
	Direction  string `json:"direction,omitempty"` // "in" | "out" | ""
}

// OdooRuleSet — what to write on the Odoo line when Match succeeds.
// Either PartnerID or AccountCode (or both) should be set.
type OdooRuleSet struct {
	PartnerID   int    `json:"partner_id,omitempty"`
	PartnerName string `json:"partner_name,omitempty"`
	AccountCode string `json:"account_code,omitempty"`
	AccountName string `json:"account_name,omitempty"`
}

func odooRulesPath() string {
	return settingsFilePath("odoo_rules.json")
}

// LoadOdooRules reads odoo_rules.json. Missing file → empty list (not
// an error) so rules can be opt-in.
func LoadOdooRules() ([]OdooRule, error) {
	data, err := os.ReadFile(odooRulesPath())
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var rules []OdooRule
	if err := json.Unmarshal(data, &rules); err != nil {
		return nil, fmt.Errorf("parse %s: %v", odooRulesPath(), err)
	}
	return rules, nil
}

func saveOdooRules(rules []OdooRule) error {
	data, err := json.MarshalIndent(rules, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(odooRulesPath()), 0755); err != nil {
		return err
	}
	return os.WriteFile(odooRulesPath(), data, 0644)
}

// MatchOdooRule returns the first rule whose Match clause is satisfied
// by the transaction, or nil when no rule applies.
func MatchOdooRule(rules []OdooRule, tx TransactionEntry) *OdooRule {
	cat := txDisplayCategory(tx)
	coll := txDisplayCollective(tx)
	for i, r := range rules {
		if r.Match.Category != "" && !strings.EqualFold(r.Match.Category, cat) {
			continue
		}
		if r.Match.Collective != "" && !strings.EqualFold(r.Match.Collective, coll) {
			continue
		}
		switch strings.ToLower(r.Match.Direction) {
		case "in", "incoming", "credit":
			if !tx.IsIncoming() {
				continue
			}
		case "out", "outgoing", "debit":
			if !tx.IsOutgoing() {
				continue
			}
		case "":
			// no direction constraint
		default:
			continue // unknown direction — never matches
		}
		if r.Match.Category == "" && r.Match.Collective == "" && r.Match.Direction == "" {
			continue // a rule with no match criteria is a no-op
		}
		return &rules[i]
	}
	return nil
}

// OdooRulesCommand is the `chb odoo rules ...` entry point.
func OdooRulesCommand(args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		printOdooRulesHelp()
		return nil
	}
	sub := ""
	if len(args) > 0 {
		sub = args[0]
	}
	switch sub {
	case "", "list":
		return printOdooRulesList()
	case "add":
		return odooRulesAdd(args[1:])
	case "remove", "rm", "delete":
		return odooRulesRemove(args[1:])
	case "edit":
		return odooRulesEdit()
	case "path":
		fmt.Println(odooRulesPath())
		return nil
	default:
		return fmt.Errorf("unknown subcommand %q (try: list, add, remove, edit)", sub)
	}
}

func printOdooRulesList() error {
	rules, err := LoadOdooRules()
	if err != nil {
		return err
	}
	if len(rules) == 0 {
		fmt.Printf("\n%sNo Odoo rules defined yet.%s\n", Fmt.Dim, Fmt.Reset)
		fmt.Printf("  Add one with: %schb odoo rules add --category donation --partner 2666%s\n", Fmt.Cyan, Fmt.Reset)
		fmt.Printf("  File: %s%s%s\n\n", Fmt.Dim, odooRulesPath(), Fmt.Reset)
		return nil
	}

	fmt.Printf("\n%s🪄 Odoo Sync Rules%s  %s(%s)%s\n\n",
		Fmt.Bold, Fmt.Reset, Fmt.Dim, odooRulesPath(), Fmt.Reset)

	headers := []string{"#", "When category", "When collective", "Direction", "Set partner", "Set account"}
	rows := make([][]string, 0, len(rules))
	for i, r := range rules {
		rows = append(rows, []string{
			fmt.Sprintf("%d", i+1),
			truncOrDash(r.Match.Category, 18),
			truncOrDash(r.Match.Collective, 18),
			truncOrDash(r.Match.Direction, 6),
			formatRulePartner(r.Set),
			formatRuleAccount(r.Set),
		})
	}
	totalRow := []string{"", Pluralize(len(rules), "rule", ""), "", "", "", ""}
	renderTicketsTable(headers, rows, totalRow, map[int]bool{0: true})
	return nil
}

func truncOrDash(s string, n int) string {
	if s == "" {
		return "—"
	}
	return Truncate(s, n)
}

func formatRulePartner(s OdooRuleSet) string {
	if s.PartnerID == 0 {
		return "—"
	}
	if s.PartnerName != "" {
		return fmt.Sprintf("#%d %s", s.PartnerID, Truncate(s.PartnerName, 22))
	}
	return fmt.Sprintf("#%d", s.PartnerID)
}

func formatRuleAccount(s OdooRuleSet) string {
	if s.AccountCode == "" {
		return "—"
	}
	if s.AccountName != "" {
		return fmt.Sprintf("%s %s", s.AccountCode, Truncate(s.AccountName, 22))
	}
	return s.AccountCode
}

// odooRulesAdd inserts a new rule. Flags:
//
//	--category <slug>         (required, the category to match)
//	--collective <slug>       (optional, additional match constraint)
//	--partner <id|name>       resolves to an Odoo res.partner
//	--account <code|id>       resolves to an Odoo account.account
//
// At least one of --partner / --account must be provided.
func odooRulesAdd(args []string) error {
	category := GetOption(args, "--category")
	collective := GetOption(args, "--collective")
	partnerArg := GetOption(args, "--partner")
	accountArg := GetOption(args, "--account")
	direction := strings.ToLower(GetOption(args, "--direction"))

	if category == "" && collective == "" {
		return fmt.Errorf("--category or --collective is required")
	}
	if partnerArg == "" && accountArg == "" {
		return fmt.Errorf("at least one of --partner or --account is required")
	}
	switch direction {
	case "", "in", "incoming", "credit":
		if direction != "" {
			direction = "in"
		}
	case "out", "outgoing", "debit":
		direction = "out"
	default:
		return fmt.Errorf("--direction must be one of: in, out (got %q)", direction)
	}

	creds, err := ResolveOdooCredentials()
	if err != nil {
		return err
	}
	uid, err := odooAuth(creds.URL, creds.DB, creds.Login, creds.Password)
	if err != nil || uid == 0 {
		return fmt.Errorf("Odoo authentication failed: %v", err)
	}

	set := OdooRuleSet{}
	if partnerArg != "" {
		id, name, err := resolveOdooPartnerArg(creds, uid, partnerArg)
		if err != nil {
			return err
		}
		set.PartnerID = id
		set.PartnerName = name
	}
	if accountArg != "" {
		code, name, err := resolveOdooAccountArg(creds, uid, accountArg)
		if err != nil {
			return err
		}
		set.AccountCode = code
		set.AccountName = name
	}

	rules, err := LoadOdooRules()
	if err != nil {
		return err
	}
	// Replace an existing rule with the exact same Match — keeps the
	// list de-duplicated and lets `add` double as `update`.
	newRule := OdooRule{
		Match: OdooRuleMatch{Category: category, Collective: collective, Direction: direction},
		Set:   set,
	}
	replaced := false
	for i, r := range rules {
		if strings.EqualFold(r.Match.Category, category) &&
			strings.EqualFold(r.Match.Collective, collective) &&
			strings.EqualFold(r.Match.Direction, direction) {
			rules[i] = newRule
			replaced = true
			break
		}
	}
	if !replaced {
		rules = append(rules, newRule)
	}
	if err := saveOdooRules(rules); err != nil {
		return err
	}

	verb := "Added"
	if replaced {
		verb = "Updated"
	}
	fmt.Printf("\n%s✓ %s rule%s\n", Fmt.Green, verb, Fmt.Reset)
	fmt.Printf("  match: category=%q collective=%q direction=%q\n", category, collective, direction)
	fmt.Printf("  set:   partner=%s  account=%s\n\n",
		formatRulePartner(set), formatRuleAccount(set))
	return printOdooRulesList()
}

// odooRulesRemove removes a rule by 1-based index OR by --category /
// --collective match.
func odooRulesRemove(args []string) error {
	rules, err := LoadOdooRules()
	if err != nil {
		return err
	}
	if len(rules) == 0 {
		return fmt.Errorf("no rules to remove")
	}

	idx := -1
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		n, err := strconv.Atoi(args[0])
		if err != nil || n < 1 || n > len(rules) {
			return fmt.Errorf("invalid rule number %q (1..%d)", args[0], len(rules))
		}
		idx = n - 1
	} else {
		category := GetOption(args, "--category")
		collective := GetOption(args, "--collective")
		if category == "" && collective == "" {
			return fmt.Errorf("specify a 1-based index, --category, or --collective")
		}
		for i, r := range rules {
			if category != "" && !strings.EqualFold(r.Match.Category, category) {
				continue
			}
			if collective != "" && !strings.EqualFold(r.Match.Collective, collective) {
				continue
			}
			idx = i
			break
		}
		if idx < 0 {
			return fmt.Errorf("no rule matches category=%q collective=%q", category, collective)
		}
	}

	removed := rules[idx]
	rules = append(rules[:idx], rules[idx+1:]...)
	if err := saveOdooRules(rules); err != nil {
		return err
	}
	fmt.Printf("\n%s✓ removed rule:%s category=%q collective=%q\n\n",
		Fmt.Green, Fmt.Reset, removed.Match.Category, removed.Match.Collective)
	return printOdooRulesList()
}

// odooRulesEdit opens the rules file in the user's $EDITOR.
func odooRulesEdit() error {
	path := odooRulesPath()
	if _, err := os.Stat(path); os.IsNotExist(err) {
		// Ensure a valid empty list so the editor has something parseable.
		if err := saveOdooRules(nil); err != nil {
			return err
		}
	}
	editor := os.Getenv("EDITOR")
	if editor == "" {
		editor = os.Getenv("VISUAL")
	}
	if editor == "" {
		fmt.Printf("$EDITOR is not set. Edit this file directly:\n  %s\n", path)
		return nil
	}
	cmd := exec.Command(editor, path)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return err
	}
	// Re-validate after edit so a malformed file fails loudly here, not
	// silently the next time a sync runs.
	if _, err := LoadOdooRules(); err != nil {
		return fmt.Errorf("rules file is now invalid: %v", err)
	}
	return printOdooRulesList()
}

// resolveOdooPartnerArg accepts a numeric partner id OR a name / name
// fragment. When ambiguous (multiple matches) it errors with the
// candidate list so the user can re-issue with an explicit id.
func resolveOdooPartnerArg(creds *OdooCredentials, uid int, arg string) (int, string, error) {
	if id, err := strconv.Atoi(arg); err == nil && id > 0 {
		rows, err := odooSearchReadAllMaps(creds, uid, "res.partner",
			[]interface{}{[]interface{}{"id", "=", id}},
			[]string{"id", "name"}, "id asc")
		if err != nil {
			return 0, "", fmt.Errorf("lookup partner #%d: %v", id, err)
		}
		if len(rows) == 0 {
			return 0, "", fmt.Errorf("no partner with id %d", id)
		}
		return id, odooString(rows[0]["name"]), nil
	}
	rows, err := odooSearchReadAllMaps(creds, uid, "res.partner",
		[]interface{}{[]interface{}{"name", "ilike", arg}},
		[]string{"id", "name"}, "id asc")
	if err != nil {
		return 0, "", fmt.Errorf("search partner %q: %v", arg, err)
	}
	if len(rows) == 0 {
		return 0, "", fmt.Errorf("no partner matches %q", arg)
	}
	if len(rows) > 1 {
		var lines []string
		for i, r := range rows {
			if i >= 5 {
				lines = append(lines, fmt.Sprintf("    … and %d more", len(rows)-5))
				break
			}
			lines = append(lines, fmt.Sprintf("    #%d %s", odooInt(r["id"]), odooString(r["name"])))
		}
		return 0, "", fmt.Errorf("ambiguous partner %q — specify the id:\n%s", arg, strings.Join(lines, "\n"))
	}
	return odooInt(rows[0]["id"]), odooString(rows[0]["name"]), nil
}

// resolveOdooAccountArg accepts an account code (e.g. "610100") or an
// explicit Odoo internal id (prefixed with "#"). Returns the code
// (canonical for our rule storage) and the account name.
//
// We always prefer the code lookup — short numeric strings like "7000"
// are not Odoo ids; they're partial codes (which we surface as a
// candidate list via lookupAccountByCode). Use "#1234" when you really
// mean the internal id.
func resolveOdooAccountArg(creds *OdooCredentials, uid int, arg string) (string, string, error) {
	if strings.HasPrefix(arg, "#") {
		id, err := strconv.Atoi(strings.TrimPrefix(arg, "#"))
		if err != nil || id <= 0 {
			return "", "", fmt.Errorf("invalid account id %q", arg)
		}
		rows, err := odooSearchReadAllMaps(creds, uid, "account.account",
			[]interface{}{[]interface{}{"id", "=", id}},
			[]string{"id", "code", "name"}, "id asc")
		if err != nil {
			return "", "", fmt.Errorf("lookup account #%d: %v", id, err)
		}
		if len(rows) != 1 {
			return "", "", fmt.Errorf("no account with id #%d", id)
		}
		return odooString(rows[0]["code"]), odooString(rows[0]["name"]), nil
	}
	return lookupAccountByCode(creds, uid, arg)
}

func lookupAccountByCode(creds *OdooCredentials, uid int, code string) (string, string, error) {
	rows, err := odooSearchReadAllMaps(creds, uid, "account.account",
		[]interface{}{[]interface{}{"code", "=", code}},
		[]string{"id", "code", "name"}, "id asc")
	if err != nil {
		return "", "", fmt.Errorf("search account code %q: %v", code, err)
	}
	if len(rows) == 1 {
		return odooString(rows[0]["code"]), odooString(rows[0]["name"]), nil
	}

	// No exact match — fall back to a fuzzy code prefix / name search so
	// the error message lists candidates the user can pick from.
	candidates, _ := odooSearchReadAllMaps(creds, uid, "account.account",
		[]interface{}{
			"|",
			[]interface{}{"code", "=ilike", code + "%"},
			[]interface{}{"name", "ilike", code},
		},
		[]string{"id", "code", "name"}, "code asc")
	if len(candidates) == 0 {
		return "", "", fmt.Errorf("no account with code %q", code)
	}
	var lines []string
	for i, r := range candidates {
		if i >= 10 {
			lines = append(lines, fmt.Sprintf("    … and %d more", len(candidates)-10))
			break
		}
		lines = append(lines, fmt.Sprintf("    %s  %s", odooString(r["code"]), odooString(r["name"])))
	}
	return "", "", fmt.Errorf("no account with exact code %q — candidates:\n%s", code, strings.Join(lines, "\n"))
}

// findOdooAccountIDByCode resolves an account code to its Odoo id at
// sync time. Cheap-cached per-process.
var odooAccountIDByCodeCache = map[string]int{}

func findOdooAccountIDByCode(creds *OdooCredentials, uid int, code string) (int, error) {
	if id, ok := odooAccountIDByCodeCache[code]; ok {
		return id, nil
	}
	rows, err := odooSearchReadAllMaps(creds, uid, "account.account",
		[]interface{}{[]interface{}{"code", "=", code}},
		[]string{"id"}, "id asc")
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, fmt.Errorf("Odoo account code %q not found", code)
	}
	id := odooInt(rows[0]["id"])
	odooAccountIDByCodeCache[code] = id
	return id, nil
}

// applyOdooRulesToExistingLines walks the journal's already-imported
// statement lines, matches each one to its source local transaction by
// unique_import_id, and pushes any rule-driven partner_id /
// account_code that isn't yet reflected in Odoo. Returns
// (reviewedCount, updatedCount): reviewed = lines in window that had
// at least one matching rule (so an actual diff was checked); updated
// = subset that received (or would receive, in dry-run) a write.
// No-op when no rules exist.
//
// since/until narrow the scan window when set; both zero means "all".
// When dryRun is true, no writes are issued; instead, every divergence
// is printed to stdout and counted. Use this for `--dry-run` previews.
func applyOdooRulesToExistingLines(creds *OdooCredentials, uid int, acc *AccountConfig, since, until time.Time, dryRun bool) (reviewed, updated int, err error) {
	rules, loadErr := LoadOdooRules()
	if loadErr != nil {
		return 0, 0, loadErr
	}
	if len(rules) == 0 || acc == nil || acc.OdooJournalID == 0 {
		return 0, 0, nil
	}

	// Build importID → matched rule for the local txs that have one.
	localTxs := loadAccountTransactionsForOdoo(acc)
	ruleByImport := make(map[string]*OdooRule, len(localTxs))
	for _, tx := range localTxs {
		r := MatchOdooRule(rules, tx)
		if r == nil {
			continue
		}
		if r.Set.PartnerID == 0 && r.Set.AccountCode == "" {
			continue
		}
		ruleByImport[buildUniqueImportID(acc, tx)] = r
	}
	if len(ruleByImport) == 0 {
		return 0, 0, nil
	}

	// Pull the existing Odoo lines (id, partner, move, date, ref) so we
	// can diff against the rule and skip the no-op cases.
	rows, fetchErr := odooSearchReadAllMaps(creds, uid, "account.bank.statement.line",
		[]interface{}{[]interface{}{"journal_id", "=", acc.OdooJournalID}},
		[]string{"id", "partner_id", "move_id", "unique_import_id", "date", "payment_ref"},
		"id desc")
	if fetchErr != nil {
		return 0, 0, fmt.Errorf("fetch journal lines: %v", fetchErr)
	}

	for _, row := range rows {
		// Window filter: skip lines outside [since, until] when set.
		if !since.IsZero() || !until.IsZero() {
			lineDate, dateErr := time.Parse("2006-01-02", odooString(row["date"]))
			if dateErr == nil {
				if !since.IsZero() && lineDate.Before(since) {
					continue
				}
				if !until.IsZero() && !lineDate.Before(until) {
					continue
				}
			}
		}
		importID := odooString(row["unique_import_id"])
		rule, ok := ruleByImport[importID]
		if !ok {
			continue
		}
		lineID := odooInt(row["id"])
		if lineID == 0 {
			continue
		}
		// At this point we know the line is in the window AND has a
		// matching rule — count it as reviewed even if no diff exists.
		reviewed++
		changed := false

		if rule.Set.PartnerID > 0 {
			currentPartner := odooFieldID(row["partner_id"])
			if currentPartner != rule.Set.PartnerID {
				if dryRun {
					fmt.Printf("    %s↳ would set partner #%d %s on line #%d  %s  %s%s\n",
						Fmt.Dim, rule.Set.PartnerID, rule.Set.PartnerName, lineID,
						odooString(row["date"]), Truncate(odooString(row["payment_ref"]), 40), Fmt.Reset)
				} else {
					if _, writeErr := odooExec(creds.URL, creds.DB, uid, creds.Password,
						"account.bank.statement.line", "write",
						[]interface{}{[]interface{}{lineID}, map[string]interface{}{
							"partner_id": rule.Set.PartnerID,
						}}, nil); writeErr != nil {
						return reviewed, updated, fmt.Errorf("update partner on line #%d: %v", lineID, writeErr)
					}
				}
				changed = true
			}
		}

		if rule.Set.AccountCode != "" {
			ruleAccountID, lookupErr := findOdooAccountIDByCode(creds, uid, rule.Set.AccountCode)
			if lookupErr != nil {
				return reviewed, updated, lookupErr
			}
			currentCounterpartAccountID := 0
			currentCounterpartLineID := 0
			if mid := odooFieldID(row["move_id"]); mid > 0 {
				cp, accID, cpErr := lookupCounterpartMoveLine(creds, uid, mid)
				if cpErr == nil {
					currentCounterpartLineID = cp
					currentCounterpartAccountID = accID
				}
			}
			if currentCounterpartLineID > 0 && currentCounterpartAccountID != ruleAccountID {
				if dryRun {
					fmt.Printf("    %s↳ would set account %s %s on counterpart of line #%d  %s  %s%s\n",
						Fmt.Dim, rule.Set.AccountCode, rule.Set.AccountName, lineID,
						odooString(row["date"]), Truncate(odooString(row["payment_ref"]), 40), Fmt.Reset)
				} else if applyErr := applyOdooRuleAccount(creds, uid, []int{lineID}, rule.Set.AccountCode); applyErr != nil {
					return reviewed, updated, applyErr
				}
				changed = true
			}
		}

		if changed {
			updated++
		}
	}
	return reviewed, updated, nil
}

// lookupCounterpartMoveLine returns the (line id, account id) of the
// non-bank leg of the given move. Used to detect whether a rule's
// account_code is already in place before paying the draft/write/post
// cost in applyOdooRuleAccount.
func lookupCounterpartMoveLine(creds *OdooCredentials, uid int, moveID int) (int, int, error) {
	rows, err := odooSearchReadAllMaps(creds, uid, "account.move.line",
		[]interface{}{[]interface{}{"move_id", "=", moveID}},
		[]string{"id", "account_id", "account_type"}, "id asc")
	if err != nil {
		return 0, 0, err
	}
	for _, r := range rows {
		t := odooString(r["account_type"])
		if t == "asset_cash" || t == "liability_credit_card" {
			continue
		}
		return odooInt(r["id"]), odooFieldID(r["account_id"]), nil
	}
	return 0, 0, nil
}

// applyOdooRuleAccount rewrites the counterpart account.move.line of
// each just-created bank statement line to use the chart-of-accounts
// account named by the rule. Same pattern as the internal-transfer
// marker: draft → write account_id on the non-bank leg → repost.
func applyOdooRuleAccount(creds *OdooCredentials, uid int, lineIDs []int, accountCode string, progress ...*statusLine) error {
	if len(lineIDs) == 0 || accountCode == "" {
		return nil
	}
	accountID, err := findOdooAccountIDByCode(creds, uid, accountCode)
	if err != nil {
		return err
	}

	var status *statusLine
	if len(progress) > 0 {
		status = progress[0]
	}
	for i, lineID := range lineIDs {
		if status != nil && (i == 0 || (i+1)%10 == 0 || i+1 == len(lineIDs)) {
			status.Update("Applying account %s to line %d/%d...", accountCode, i+1, len(lineIDs))
		}
		line, err := readStatementLineForReconcile(creds, uid, lineID)
		if err != nil {
			return fmt.Errorf("read line #%d: %v", lineID, err)
		}
		if line.MoveID == 0 {
			continue
		}
		counterpartID, err := findStatementCounterpartMoveLine(creds, uid, line)
		if err != nil {
			return fmt.Errorf("find counterpart for line #%d: %v", lineID, err)
		}
		if counterpartID == 0 {
			continue
		}
		_, _ = odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.move", "button_draft",
			[]interface{}{[]interface{}{line.MoveID}}, nil)
		_, err = odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.move.line", "write",
			[]interface{}{[]interface{}{counterpartID}, map[string]interface{}{"account_id": accountID}}, nil)
		if err != nil {
			return fmt.Errorf("write account on move line #%d: %v", counterpartID, err)
		}
		if _, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.move", "action_post",
			[]interface{}{[]interface{}{line.MoveID}}, nil); err != nil {
			return fmt.Errorf("repost move #%d: %v", line.MoveID, err)
		}
	}
	return nil
}

func printOdooRulesHelp() {
	f := Fmt
	fmt.Printf(`
%schb odoo rules%s — Apply Odoo partner / account assignments by category

%sUSAGE%s
  %schb odoo rules%s                              List all rules
  %schb odoo rules add --category donation --partner 2666%s
  %schb odoo rules add --category rent --account 610100%s
  %schb odoo rules add --category fridge --partner "SELF-SERVED FRIDGE" --direction in%s
  %schb odoo rules remove 2%s                     Remove the 2nd rule
  %schb odoo rules remove --category donation%s   Remove by category match
  %schb odoo rules edit%s                         Open the rules file in $EDITOR
  %schb odoo rules path%s                         Print the rules file path

  %s--direction in%s   only match incoming (CREDIT/MINT) txs
  %s--direction out%s  only match outgoing (DEBIT/BURN) txs

%sBEHAVIOUR%s
  When syncing a transaction into an Odoo journal, the first matching
  rule applies its values:
    • %spartner_id%s is written on the bank statement line
    • %saccount_code%s is resolved to an account.account id and written
      on the counterpart account.move.line (replacing the suspense /
      auto-assigned account)

  Rules are stored in %s%s%s.
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Dim, odooRulesPath(), f.Reset,
	)
}
