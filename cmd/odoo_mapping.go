package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

// OdooMapping maps a semantic tag (category / collective + direction) to a
// specific Odoo account.account code and/or res.partner id. Loaded from
// odoo_mapping.json and applied during `chb generate` (the result lives in
// providers/odoo/<db>/pending/ for Odoo push paths to consume).
//
// Note: this is a lookup table, not a rule engine. Pattern matching against
// descriptions / IBANs / amounts happens earlier in rules.json (semantic
// categorisation → category/collective); odoo_mapping.json then resolves
// those semantic tags into Odoo identifiers.
//
// PartnerName / AccountName are human-readable caches stored alongside the
// IDs so `chb odoo mapping` is reviewable without hitting Odoo on every list.
type OdooMapping struct {
	Match OdooMappingMatch  `json:"match"`
	Set   OdooMappingResult `json:"set"`
}

// OdooMappingMatch — matches by category, collective, and direction. An
// empty value means "don't constrain". Merchant-specific overrides (e.g.
// "proximus posts to 616030, not the generic utilities account") belong in
// rules.json with a more specific category name; this file only maps
// semantic tags → Odoo account/partner identifiers.
type OdooMappingMatch struct {
	Category   string `json:"category,omitempty"`
	Collective string `json:"collective,omitempty"`
	Direction  string `json:"direction,omitempty"` // "in" | "out" | ""
}

// OdooMappingResult — what to write on the Odoo line when Match succeeds.
// Either PartnerID or AccountCode (or both) should be set.
type OdooMappingResult struct {
	PartnerID   int    `json:"partner_id,omitempty"`
	PartnerName string `json:"partner_name,omitempty"`
	AccountCode string `json:"account_code,omitempty"`
	AccountName string `json:"account_name,omitempty"`
}

func odooMappingPath() string {
	return settingsFilePath("odoo_mapping.json")
}

// legacyOdooRulesPath returns the pre-rename settings location. Used once
// by LoadOdooMappings to migrate users automatically.
func legacyOdooRulesPath() string {
	return settingsFilePath("odoo_rules.json")
}

// LoadOdooMappings reads odoo_mapping.json (falling back to the legacy
// odoo_rules.json once, renaming it in place if found). Missing file →
// empty list (not an error) so mappings can be opt-in.
func LoadOdooMappings() ([]OdooMapping, error) {
	path := odooMappingPath()
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if _, err := os.Stat(legacyOdooRulesPath()); err == nil {
			if err := os.Rename(legacyOdooRulesPath(), path); err == nil {
				Warnf("%s'odoo_rules.json' renamed to 'odoo_mapping.json'%s", Fmt.Dim, Fmt.Reset)
			}
		}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var mappings []OdooMapping
	if err := json.Unmarshal(data, &mappings); err != nil {
		return nil, fmt.Errorf("parse %s: %v", path, err)
	}
	return mappings, nil
}

func saveOdooMappings(mappings []OdooMapping) error {
	data, err := json.MarshalIndent(mappings, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(odooMappingPath()), 0755); err != nil {
		return err
	}
	return os.WriteFile(odooMappingPath(), data, 0644)
}

// applyOdooMapping runs LookupOdooMapping for tx and writes the resulting
// AccountCode / PartnerID back onto the entry. Called once per tx during
// generate so downstream push/merge code can trust the resolved values on
// transactions.json without re-running the lookup chain. Idempotent and
// safe for empty mapping lists (leaves the fields untouched).
func applyOdooMapping(mappings []OdooMapping, tx *TransactionEntry) {
	if tx == nil {
		return
	}
	matched := LookupOdooMapping(mappings, *tx)
	if matched == nil {
		tx.AccountCode = ""
		tx.PartnerID = 0
		return
	}
	tx.AccountCode = matched.Set.AccountCode
	tx.PartnerID = matched.Set.PartnerID
}

// LookupOdooMapping returns the first mapping whose Match clause is
// satisfied by the transaction, or nil when none apply. Mappings are
// evaluated in file order; the operator orders them most-specific-first.
func LookupOdooMapping(mappings []OdooMapping, tx TransactionEntry) *OdooMapping {
	cat := txDisplayCategory(tx)
	coll := txDisplayCollective(tx)
	// An internal transfer (Type INTERNAL) IS category "internal_transfer" for
	// accounting purposes even when generate didn't stamp the category — a payout
	// between our own accounts, an on-chain transfer between two tracked wallets,
	// etc. Resolve it to the internal-transfer account everywhere: the generated
	// AccountCode, the push, and the `fix` reclassification all key off this.
	if cat == "" && strings.EqualFold(strings.TrimSpace(tx.Type), "INTERNAL") {
		cat = "internal_transfer"
	}
	for i, r := range mappings {
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
			continue // an entry with no match criteria is a no-op
		}
		return &mappings[i]
	}
	return nil
}

// OdooMappingCommand is the `chb odoo mapping ...` entry point. The legacy
// alias `chb odoo rules` is wired in main.go with a deprecation notice.
func OdooMappingCommand(args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		printOdooMappingHelp()
		return nil
	}
	sub := ""
	if len(args) > 0 {
		sub = args[0]
	}
	switch sub {
	case "", "list":
		return printOdooMappingList()
	case "add":
		return odooMappingAdd(args[1:])
	case "remove", "rm", "delete":
		return odooMappingRemove(args[1:])
	case "edit":
		return odooMappingEdit()
	case "path":
		fmt.Println(odooMappingPath())
		return nil
	default:
		return fmt.Errorf("unknown subcommand %q (try: list, add, remove, edit)", sub)
	}
}

func printOdooMappingList() error {
	mappings, err := LoadOdooMappings()
	if err != nil {
		return err
	}
	if len(mappings) == 0 {
		fmt.Printf("\n%sNo Odoo mappings defined yet.%s\n", Fmt.Dim, Fmt.Reset)
		fmt.Printf("  Add one with: %schb odoo mapping add --category donation --partner 2666%s\n", Fmt.Cyan, Fmt.Reset)
		fmt.Printf("  File: %s%s%s\n\n", Fmt.Dim, odooMappingPath(), Fmt.Reset)
		return nil
	}

	fmt.Printf("\n%s🪄 Odoo Mappings%s  %s(%s)%s\n\n",
		Fmt.Bold, Fmt.Reset, Fmt.Dim, odooMappingPath(), Fmt.Reset)

	headers := []string{"#", "When category", "When collective", "Direction", "Set partner", "Set account"}
	rows := make([][]string, 0, len(mappings))
	for i, r := range mappings {
		rows = append(rows, []string{
			fmt.Sprintf("%d", i+1),
			truncOrDash(r.Match.Category, 18),
			truncOrDash(r.Match.Collective, 18),
			truncOrDash(r.Match.Direction, 6),
			formatMappingPartner(r.Set),
			formatMappingAccount(r.Set),
		})
	}
	totalRow := []string{"", Pluralize(len(mappings), "mapping", ""), "", "", "", ""}
	renderTicketsTable(headers, rows, totalRow, map[int]bool{0: true})
	return nil
}

func truncOrDash(s string, n int) string {
	if s == "" {
		return "—"
	}
	return Truncate(s, n)
}

func formatMappingPartner(s OdooMappingResult) string {
	if s.PartnerID == 0 {
		return "—"
	}
	if s.PartnerName != "" {
		return fmt.Sprintf("#%d %s", s.PartnerID, Truncate(s.PartnerName, 22))
	}
	return fmt.Sprintf("#%d", s.PartnerID)
}

func formatMappingAccount(s OdooMappingResult) string {
	if s.AccountCode == "" {
		return "—"
	}
	if s.AccountName != "" {
		return fmt.Sprintf("%s %s", s.AccountCode, Truncate(s.AccountName, 22))
	}
	return s.AccountCode
}

// odooMappingAdd inserts a new mapping. Flags:
//
//	--category <slug>         (required, the category to match)
//	--collective <slug>       (optional, additional match constraint)
//	--partner <id|name>       resolves to an Odoo res.partner
//	--account <code|id>       resolves to an Odoo account.account
//
// At least one of --partner / --account must be provided.
func odooMappingAdd(args []string) error {
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

	set := OdooMappingResult{}
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

	mappings, err := LoadOdooMappings()
	if err != nil {
		return err
	}
	// Replace an existing mapping with the exact same Match — keeps the
	// list de-duplicated and lets `add` double as `update`.
	newMapping := OdooMapping{
		Match: OdooMappingMatch{Category: category, Collective: collective, Direction: direction},
		Set:   set,
	}
	replaced := false
	for i, r := range mappings {
		if strings.EqualFold(r.Match.Category, category) &&
			strings.EqualFold(r.Match.Collective, collective) &&
			strings.EqualFold(r.Match.Direction, direction) {
			mappings[i] = newMapping
			replaced = true
			break
		}
	}
	if !replaced {
		mappings = append(mappings, newMapping)
	}
	if err := saveOdooMappings(mappings); err != nil {
		return err
	}

	verb := "Added"
	if replaced {
		verb = "Updated"
	}
	fmt.Printf("\n%s✓ %s mapping%s\n", Fmt.Green, verb, Fmt.Reset)
	fmt.Printf("  match: category=%q collective=%q direction=%q\n", category, collective, direction)
	fmt.Printf("  set:   partner=%s  account=%s\n\n",
		formatMappingPartner(set), formatMappingAccount(set))
	return printOdooMappingList()
}

// odooMappingRemove removes a mapping by 1-based index OR by --category /
// --collective match.
func odooMappingRemove(args []string) error {
	mappings, err := LoadOdooMappings()
	if err != nil {
		return err
	}
	if len(mappings) == 0 {
		return fmt.Errorf("no mappings to remove")
	}

	idx := -1
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		n, err := strconv.Atoi(args[0])
		if err != nil || n < 1 || n > len(mappings) {
			return fmt.Errorf("invalid mapping number %q (1..%d)", args[0], len(mappings))
		}
		idx = n - 1
	} else {
		category := GetOption(args, "--category")
		collective := GetOption(args, "--collective")
		if category == "" && collective == "" {
			return fmt.Errorf("specify a 1-based index, --category, or --collective")
		}
		for i, r := range mappings {
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
			return fmt.Errorf("no mapping matches category=%q collective=%q", category, collective)
		}
	}

	removed := mappings[idx]
	mappings = append(mappings[:idx], mappings[idx+1:]...)
	if err := saveOdooMappings(mappings); err != nil {
		return err
	}
	fmt.Printf("\n%s✓ removed mapping:%s category=%q collective=%q\n\n",
		Fmt.Green, Fmt.Reset, removed.Match.Category, removed.Match.Collective)
	return printOdooMappingList()
}

// odooMappingEdit opens the mappings file in the user's $EDITOR.
func odooMappingEdit() error {
	path := odooMappingPath()
	if _, err := os.Stat(path); os.IsNotExist(err) {
		// Ensure a valid empty list so the editor has something parseable.
		if err := saveOdooMappings(nil); err != nil {
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
	if _, err := LoadOdooMappings(); err != nil {
		return fmt.Errorf("mapping file is now invalid: %v", err)
	}
	return printOdooMappingList()
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
// (canonical for our mapping storage) and the account name.
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

// lookupCounterpartMoveLine returns the (line id, account id) of the
// non-bank leg of the given move. Used to detect whether a mapping's
// account_code is already in place before paying the draft/write/post
// cost in applyOdooMappingAccount.
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

// applyOdooMappingAccount rewrites the counterpart account.move.line of
// each just-created bank statement line to use the chart-of-accounts
// account named by the mapping.
//
// Implementation note: Odoo's bank-statement-line create atomically
// creates AND posts the move, so there's no draft window we can hook
// into via plain XML-RPC. The honest answer is "draft → write → repost"
// per move. What we *can* do is batch the three steps across many lines
// instead of doing them per-line — for a 188-line KBC merge that's the
// difference between ~940 RPCs and ~5.
//
// All callers funnel into applyOdooMappingAccountBatch, which itself
// chunks the bulk-draft / bulk-write / bulk-post calls so even very
// large journals stay under any single-call payload limit.
func applyOdooMappingAccount(creds *OdooCredentials, uid int, lineIDs []int, accountCode string, progress ...*statusLine) error {
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
	if status != nil {
		status.Update("Resolving counterparts for %d line(s)…", len(lineIDs))
	}

	// 1. Batch-read the statement lines to learn their move_ids.
	lineRows, err := odooReadMapsByIDs(creds, uid, "account.bank.statement.line",
		uniquePositiveInts(lineIDs), []string{"id", "move_id"})
	if err != nil {
		return fmt.Errorf("read statement lines: %v", err)
	}
	moveIDs := make([]int, 0, len(lineRows))
	for _, row := range lineRows {
		if mid := odooFieldID(row["move_id"]); mid > 0 {
			moveIDs = append(moveIDs, mid)
		}
	}
	if len(moveIDs) == 0 {
		return nil
	}

	// 2. Batch-resolve the non-bank counterpart move.line for each move.
	counterpartByMoveID, err := fetchCounterpartMoveLinesByMoveID(creds, uid, moveIDs)
	if err != nil {
		return fmt.Errorf("find counterparts: %v", err)
	}
	counterpartIDs := make([]int, 0, len(counterpartByMoveID))
	for _, info := range counterpartByMoveID {
		if info.LineID > 0 {
			counterpartIDs = append(counterpartIDs, info.LineID)
		}
	}
	if len(counterpartIDs) == 0 {
		return nil
	}

	// 3. One batched draft → write → post pass, chunked internally.
	return applyOdooMappingAccountBatch(creds, uid, moveIDs, counterpartIDs, accountID, accountCode, status)
}

func printOdooMappingHelp() {
	f := Fmt
	fmt.Printf(`
%schb odoo mapping%s — Map semantic tags (category/collective) to Odoo account / partner

%sUSAGE%s
  %schb odoo mapping%s                                List all mappings
  %schb odoo mapping add --category donation --partner 2666%s
  %schb odoo mapping add --category rent --account 610100%s
  %schb odoo mapping add --category fridge --partner "SELF-SERVED FRIDGE" --direction in%s
  %schb odoo mapping remove 2%s                       Remove the 2nd mapping
  %schb odoo mapping remove --category donation%s     Remove by category match
  %schb odoo mapping edit%s                           Open the mapping file in $EDITOR
  %schb odoo mapping path%s                           Print the mapping file path

  %s--direction in%s   only match incoming (CREDIT/MINT) txs
  %s--direction out%s  only match outgoing (DEBIT/BURN) txs

%sBEHAVIOUR%s
  Mappings are applied during %schb generate%s, which writes the resolved
  partner_id and account_code into providers/odoo/<db>/pending/ alongside
  each transaction. Odoo push paths read those pre-resolved values; they
  never run the lookup chain themselves.

  Mappings are stored in %s%s%s.
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
		f.Cyan, f.Reset,
		f.Dim, odooMappingPath(), f.Reset,
	)
}
