package cmd

import (
	"fmt"
	"os"
	"strings"
	"time"
)

// statusInSyncWindow is how close an account's last sync has to be to the
// global last sync to be considered "synced in the latest run". A full
// `chb sync` loop stamps each source as it finishes — and the global marker
// last, once everything is done — so accounts trail the global timestamp by
// however long the rest of the loop took. The window is generous enough to
// absorb a slow loop yet far tighter than the hourly cron cadence, so a
// genuinely stale account (skipped, errored, or only ever synced manually)
// still falls outside it and gets its own timestamp printed.
const statusInSyncWindow = 30 * time.Minute

const statusLabelWidth = 13 // len("App data dir:")

// StatusJSON is the machine-readable shape emitted by `chb status --json`.
type StatusJSON struct {
	Version    string              `json:"version"`
	Commit     string              `json:"commit,omitempty"`
	AppDataDir string              `json:"appDataDir"`
	DataDir    string              `json:"dataDir"`
	OdooURL    string              `json:"odooUrl,omitempty"`
	OdooDB     string              `json:"odooDb,omitempty"`
	LastSync   string              `json:"lastSync,omitempty"`
	Accounts   []StatusAccountJSON `json:"accounts"`
}

// StatusAccountJSON is one account entry in the JSON status output.
type StatusAccountJSON struct {
	Slug     string `json:"slug"`
	Name     string `json:"name,omitempty"`
	Provider string `json:"provider"`
	LastSync string `json:"lastSync,omitempty"`
	InSync   bool   `json:"inSync"`
}

// Status prints a high-level snapshot of the local chb install: version,
// where data lives, the most recent sync across all sources, and the
// per-account sync state (only spelling out an account's own timestamp when
// it lags the global last sync).
func Status(args []string) {
	if HasFlag(args, "--help", "-h", "help") {
		printStatusHelp()
		return
	}

	configs := LoadAccountConfigs()
	state := LoadSyncState()
	globalLast := globalLastSyncTime(state)

	if JSONMode(args) {
		emitStatusJSON(configs, globalLast)
		return
	}

	f := Fmt
	fmt.Printf("\n%s📊 chb status%s\n\n", f.Bold, f.Reset)

	printStatusField("Version", statusVersionLine())
	printStatusField("App data dir", AppDataDir())
	printStatusField("Data dir", DataDir())
	if url, db := configuredOdoo(); url != "" {
		line := url
		if db != "" {
			line = fmt.Sprintf("%s  %s(db: %s)%s", url, f.Dim, db, f.Reset)
		}
		printStatusField("Odoo", line)
	}
	if globalLast.IsZero() {
		printStatusField("Last sync", f.Yellow+"never"+f.Reset)
	} else {
		printStatusField("Last sync", formatTimeAgoWithAbsolute(globalLast))
	}

	fmt.Printf("\n  %sAccounts%s (%d)\n", f.Bold, f.Reset, len(configs))
	if len(configs) == 0 {
		fmt.Printf("    %sNone configured — run `chb setup`%s\n", f.Dim, f.Reset)
		fmt.Println()
		return
	}

	slugW, provW := 0, 0
	for _, acc := range configs {
		if len(acc.Slug) > slugW {
			slugW = len(acc.Slug)
		}
		if len(acc.Provider) > provW {
			provW = len(acc.Provider)
		}
	}

	for i := range configs {
		acc := &configs[i]
		last := LastSyncTime("account:" + strings.ToLower(acc.Slug))
		fmt.Printf("    %s%-*s%s  %s%-*s%s  %s\n",
			f.Cyan, slugW, acc.Slug, f.Reset,
			f.Dim, provW, acc.Provider, f.Reset,
			accountStatusDetail(last, globalLast))
	}
	fmt.Println()
}

// accountStatusDetail renders the right-hand sync column for one account:
// green "synced" when it kept pace with the global last sync, a yellow
// "never synced" when it has none, otherwise its own lagging timestamp.
func accountStatusDetail(last, globalLast time.Time) string {
	f := Fmt
	switch {
	case last.IsZero():
		return f.Yellow + "never synced" + f.Reset
	case accountInSync(last, globalLast):
		return f.Green + "synced" + f.Reset
	default:
		return formatTimeAgoWithAbsolute(last)
	}
}

// accountInSync reports whether an account's last sync is close enough to the
// global last sync to count as part of the same run.
func accountInSync(last, globalLast time.Time) bool {
	if last.IsZero() || globalLast.IsZero() {
		return false
	}
	diff := globalLast.Sub(last)
	if diff < 0 {
		diff = -diff
	}
	return diff <= statusInSyncWindow
}

// globalLastSyncTime returns the most recent LastSync timestamp across every
// tracked source — the per-provider sources, every account, every linked
// Odoo journal, and the recent/history run markers. This is the "global last
// sync" the per-account view is compared against.
func globalLastSyncTime(state *SyncState) time.Time {
	var latest time.Time
	consider := func(raw string) {
		if raw == "" {
			return
		}
		if t, err := time.Parse(time.RFC3339, raw); err == nil && t.After(latest) {
			latest = t
		}
	}
	considerSource := func(ss *SyncSourceState) {
		if ss != nil {
			consider(ss.LastSync)
		}
	}
	if state == nil {
		return latest
	}
	considerSource(state.Calendars)
	considerSource(state.Transactions)
	considerSource(state.Invoices)
	considerSource(state.Bills)
	considerSource(state.Attachments)
	considerSource(state.Messages)
	considerSource(state.Images)
	for _, ss := range state.Accounts {
		considerSource(ss)
	}
	for _, ss := range state.OdooJournals {
		considerSource(ss)
	}
	if state.Runs != nil {
		consider(state.Runs.LastRecentSync)
		consider(state.Runs.LastHistorySync)
	}
	return latest
}

// configuredOdoo returns the configured Odoo instance URL and the database it
// resolves to (ODOO_DATABASE, else derived from the URL host), or empty strings
// when no Odoo instance is configured. Env is loaded from config.env at
// startup, so this reflects both config.env and any --odoo-url/--odoo-db flags.
func configuredOdoo() (url, db string) {
	url = strings.TrimSpace(os.Getenv("ODOO_URL"))
	if url == "" {
		return "", ""
	}
	db = strings.TrimSpace(os.Getenv("ODOO_DATABASE"))
	if db == "" {
		db = odooDBFromURL(url)
	}
	return url, db
}

func statusVersionLine() string {
	ver := Version
	if ver == "" {
		ver = "dev"
	}
	short := CommitSHA
	if len(short) > 7 {
		short = short[:7]
	}
	switch {
	case short != "" && CommitDate != "":
		return fmt.Sprintf("%s %s(%s, %s)%s", ver, Fmt.Dim, short, CommitDate, Fmt.Reset)
	case short != "":
		return fmt.Sprintf("%s %s(%s)%s", ver, Fmt.Dim, short, Fmt.Reset)
	default:
		return ver
	}
}

func printStatusField(label, value string) {
	fmt.Printf("  %s%-*s%s %s\n", Fmt.Dim, statusLabelWidth, label+":", Fmt.Reset, value)
}

func emitStatusJSON(configs []AccountConfig, globalLast time.Time) {
	out := StatusJSON{
		Version:    Version,
		Commit:     CommitSHA,
		AppDataDir: AppDataDir(),
		DataDir:    DataDir(),
		Accounts:   make([]StatusAccountJSON, 0, len(configs)),
	}
	out.OdooURL, out.OdooDB = configuredOdoo()
	if out.Version == "" {
		out.Version = "dev"
	}
	if !globalLast.IsZero() {
		out.LastSync = globalLast.UTC().Format(time.RFC3339)
	}
	for i := range configs {
		acc := &configs[i]
		last := LastSyncTime("account:" + strings.ToLower(acc.Slug))
		entry := StatusAccountJSON{
			Slug:     acc.Slug,
			Name:     acc.Name,
			Provider: acc.Provider,
			InSync:   accountInSync(last, globalLast),
		}
		if !last.IsZero() {
			entry.LastSync = last.UTC().Format(time.RFC3339)
		}
		out.Accounts = append(out.Accounts, entry)
	}
	_ = EmitJSON(out)
}

func printStatusHelp() {
	f := Fmt
	fmt.Printf(`
%schb status%s — Show the local install at a glance

%sUSAGE%s
  %schb status%s [--json]

%sOUTPUT%s
  chb version, app data dir, data dir, the configured Odoo instance (URL +
  database, when set), the most recent sync across all sources, and each
  configured account with its sync state (accounts that kept pace with the
  last sync show "synced"; lagging ones show their own timestamp).

%sOPTIONS%s
  %s--json%s              Output as JSON
  %s--help, -h%s          Show this help
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
	)
}
