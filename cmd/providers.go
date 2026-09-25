package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Provider owns one external upstream. A provider syncs data into the monthly
// providers/<provider>/ archive, then maps that archived data into standard
// generated objects. Cross-provider enrichment belongs in DataProcessor.
type Provider interface {
	Source() string
	EnvVars() []ProviderEnvVar
	SyncSourceData(*ProviderSyncContext, ProviderSyncScope) error
	GenerateObjects(*ProviderGenerateContext, ProviderGenerateScope) (*ProviderGeneratedObjects, error)
}

type ProviderEnvVar struct {
	Name        string
	Description string
	Required    bool
}

type ProviderSyncContext struct {
	DataDir  string
	Settings *Settings
}

type ProviderGenerateContext struct {
	DataDir  string
	Settings *Settings
}

type ProviderSyncScope struct {
	Source     string
	Account    string
	StartMonth string
	EndMonth   string
	Force      bool
}

type ProviderGenerateScope struct {
	Year  string
	Month string
}

type ProviderGeneratedObjects struct {
	Transactions []TransactionEntry
	Events       []FullEvent
	Messages     []json.RawMessage
	Images       []ImageEntry
}

func providerSourceRelPath(source string, elems ...string) string {
	parts := append([]string{"providers", normalizeSourceName(source)}, elems...)
	return filepath.Join(parts...)
}

func providerSourcePath(dataDir, year, month, source string, elems ...string) string {
	parts := []string{dataDir, year, month, providerSourceRelPath(source, elems...)}
	return filepath.Join(parts...)
}

func writeProviderSourceJSON(dataDir, year, month, source string, v interface{}, elems ...string) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return writeMonthFile(dataDir, year, month, providerSourceRelPath(source, elems...), data)
}

func processorDataRelPath(processor string, elems ...string) string {
	parts := append([]string{"processors", normalizeSourceName(processor)}, elems...)
	return filepath.Join(parts...)
}

func processorDataPath(dataDir, year, month, processor string, elems ...string) string {
	parts := []string{dataDir, year, month, processorDataRelPath(processor, elems...)}
	return filepath.Join(parts...)
}

func writeProcessorDataJSON(dataDir, year, month, processor string, v interface{}, elems ...string) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return writeMonthFile(dataDir, year, month, processorDataRelPath(processor, elems...), data)
}

func normalizeSourceName(source string) string {
	source = strings.TrimSpace(strings.ToLower(source))
	source = strings.ReplaceAll(source, "_", "-")
	source = strings.Join(strings.Fields(source), "-")
	return source
}

func registeredProviders() []Provider {
	return nil
}

type providerCommandSpec struct {
	Name        string
	Description string
	Commands    []string
	// Sync runs the provider's pull and returns a short one-line summary
	// suitable for the streaming row in the "Fetching latest data" table
	// (e.g. "12 new transactions" or "already synced").
	Sync     func([]string) (string, error)
	Generate func([]string) error
}

func providerCommandSpecs() []providerCommandSpec {
	return []providerCommandSpec{
		{
			Name:        "ics",
			Description: "Calendar ICS feeds for room bookings and public events.",
			Commands:    []string{"sync", "generate"},
			Sync: func(args []string) (string, error) {
				b, e, err := CalendarsSync(args)
				return formatNewSummary(b, "booking", "bookings", e, "event", "events"), err
			},
			Generate: GenerateEvents,
		},
		{
			Name:        "etherscan",
			Description: "Etherscan-compatible chain transaction archives.",
			Commands:    []string{"sync", "generate"},
			Sync:        syncTransactionsProvider("etherscan"),
			Generate:    GenerateTransactions,
		},
		{
			Name:        "stripe",
			Description: "Stripe balance, charge, payout, and membership archives.",
			Commands:    []string{"sync", "generate"},
			Sync:        syncTransactionsProvider("stripe"),
			Generate:    GenerateTransactions,
		},
		{
			Name:        "monerium",
			Description: "Monerium order and EUR transaction archives.",
			Commands:    []string{"sync", "generate"},
			Sync:        syncTransactionsProvider("monerium"),
			Generate:    GenerateTransactions,
		},
		{
			Name:        "discord",
			Description: "Discord message and image attachment archives.",
			Commands:    []string{"sync", "generate"},
			Sync: func(args []string) (string, error) {
				msgs, err := MessagesSync(args)
				if err != nil {
					return formatCountSummary(msgs, "new message", "new messages"), err
				}
				imgs, err := ImagesSync(args)
				return formatNewSummary(msgs, "new message", "new messages", imgs, "new image", "new images"), err
			},
			Generate: GenerateMessages,
		},
		{
			Name:        "odoo",
			Description: "Odoo invoices, bills, attachments, and accounting metadata.",
			Commands:    []string{"sync", "generate"},
			Sync: func(args []string) (string, error) {
				return "", OdooProviderSync(args)
			},
			Generate: GenerateMembers,
		},
		{
			Name:        "intervat",
			Description: "Belgian VAT declarations (Intervat XML), imported from the drop folder latest/providers/intervat/.",
			Commands:    []string{"pull", "generate"},
			Sync:        pullVATInbox,
			Generate: func(args []string) error {
				_, err := generateVAT(DataDir())
				return err
			},
		},
		{
			Name:        "nostr",
			Description: "Nostr annotations and metadata archives.",
			Commands:    []string{"pull", "generate"},
			// Pull-only here: chb pull (= run every provider's Sync) must
			// never trigger Nostr writes. The push side is reached via
			// `chb nostr push` / `chb push`.
			Sync: func(args []string) (string, error) {
				return "", NostrPull(args)
			},
			Generate: GenerateTransactions,
		},
	}
}

func syncTransactionsProvider(source string) func([]string) (string, error) {
	return func(args []string) (string, error) {
		if GetOption(args, "--source") == "" {
			args = append([]string{"--source", source}, args...)
		}
		n, err := TransactionsSync(args)
		return formatCountSummary(n, "new transaction", "new transactions"), err
	}
}

// ProvidersCommand routes provider-scoped commands.
//
// The shape is intentionally regular:
//
//	chb providers <provider|*> <sync|generate> [args]
//
// Top-level chb sync and chb generate are shorthands for the wildcard forms.
func ProvidersCommand(args []string) error {
	if len(args) == 0 || args[0] == "--help" || args[0] == "-h" || args[0] == "help" {
		PrintProvidersHelp()
		return nil
	}

	provider := normalizeSourceName(args[0])
	if provider == "all" {
		provider = "*"
	}
	if provider != "*" {
		if _, ok := providerSpec(provider); !ok {
			if actionIdx := providerActionIndex(args); actionIdx > 1 {
				return ProvidersCommand(append([]string{"*", args[actionIdx]}, args[actionIdx+1:]...))
			}
		}
	}
	if provider == "" {
		PrintProvidersHelp()
		return nil
	}

	if len(args) < 2 {
		if provider == "*" {
			PrintProvidersHelp()
			return nil
		}
		if spec, ok := providerSpec(provider); ok {
			PrintProviderHelp(spec)
			return nil
		}
		return fmt.Errorf("unknown provider %q", args[0])
	}

	action := strings.ToLower(strings.TrimSpace(args[1]))
	rest := args[2:]
	switch provider {
	case "*":
		switch action {
		case "pull", "sync":
			if action == "sync" {
				Warnf("%s'sync' is deprecated — use 'pull' instead%s", Fmt.Dim, Fmt.Reset)
			}
			if HasFlag(rest, "--help", "-h", "help") {
				PrintSyncAllHelp()
				return nil
			}
			return runAllProviderSync(rest)
		case "generate":
			if len(rest) > 0 {
				switch strings.ToLower(rest[0]) {
				case "transactions", "tx":
					return GenerateTransactions(rest[1:])
				case "events":
					return GenerateEvents(rest[1:])
				case "messages":
					return GenerateMessages(rest[1:])
				case "members":
					return GenerateMembers(rest[1:])
				}
			}
			return Generate(rest)
		default:
			return fmt.Errorf("unknown providers action %q; expected sync or generate", action)
		}
	default:
		spec, ok := providerSpec(provider)
		if !ok {
			return fmt.Errorf("unknown provider %q", args[0])
		}
		switch action {
		case "pull", "sync":
			if action == "sync" {
				Warnf("%s'sync' is deprecated — use 'pull' instead%s", Fmt.Dim, Fmt.Reset)
			}
			if spec.Sync == nil {
				return fmt.Errorf("provider %q does not support pull", provider)
			}
			_, err := spec.Sync(rest)
			return err
		case "generate":
			if spec.Generate == nil {
				return fmt.Errorf("provider %q does not support generate", provider)
			}
			return spec.Generate(rest)
		case "help", "--help", "-h":
			PrintProviderHelp(spec)
			return nil
		default:
			return fmt.Errorf("unknown provider action %q; expected pull or generate", action)
		}
	}
}

func providerActionIndex(args []string) int {
	for i, arg := range args {
		switch strings.ToLower(strings.TrimSpace(arg)) {
		case "pull", "sync", "generate":
			return i
		}
	}
	return -1
}

func providerSpec(name string) (providerCommandSpec, bool) {
	for _, spec := range providerCommandSpecs() {
		if spec.Name == name {
			return spec, true
		}
	}
	return providerCommandSpec{}, false
}

func runAllProviderSync(args []string) error {
	verbose := HasFlag(args, "--verbose", "-v") || HasFlag(args, "--debug")
	ResetCapturedStepDiagnostics()
	startedAt := time.Now()
	defer func() {
		// Per-phase wall-clock footer. Warnings/errors stay buffered
		// on the row's ⚠/✗ mark and in the daily log; the per-phase
		// "Issues" block has been removed — it became noisy on
		// runs with lots of processor warnings. The single
		// "N errors and M warnings, written in <log>" tail printed
		// by PrintDiagnosticsSummary at process exit is enough.
		fmt.Fprintf(os.Stderr, "\n  %sPull done in %s%s\n",
			Fmt.Dim, FormatElapsedFixed(time.Since(startedAt).Round(100*time.Millisecond)), Fmt.Reset)
	}()

	// Sort providers alphabetically by display name so the streaming
	// progress rows appear in a stable, predictable order.
	specs := providerCommandSpecs()
	sort.Slice(specs, func(i, j int) bool {
		return providerDisplayName(specs[i].Name) < providerDisplayName(specs[j].Name)
	})

	// Header: bold banner, then a plan table listing every provider
	// with its accounts/scope. The live progress rows below drop the
	// account column because the operator already saw it in the plan
	// — keeping it on every row just forces the summary column to
	// truncate mid-word (this used to clip "fetching #room/lo…" or
	// "Odoo: 3632 l…" on a 120-col terminal).
	fmt.Printf("\n  %sFetching latest data%s%s\n", Fmt.Bold, Fmt.Reset, odooTargetHeaderSuffix())
	renderPullPlan(specs, pullProviderHeaderItems())
	fmt.Fprintln(os.Stderr)

	for _, spec := range specs {
		if spec.Sync == nil {
			continue
		}
		display := providerDisplayName(spec.Name)
		if verbose {
			fmt.Printf("\n%s━━━ %s ━━━%s\n", Fmt.Bold, display, Fmt.Reset)
			if _, err := spec.Sync(args); err != nil {
				return err
			}
			continue
		}
		// Compact: live status line on stderr, swallow stdout chatter.
		// Wrap with diagnostic capture so any warning emitted during this
		// spec.Sync ends up in the footer (and as a ⚠ mark on the row)
		// instead of interrupting the spinner mid-frame.
		diag := BeginStepDiagnostics(display)
		sl := NewStatusLine(display)
		SetActiveStatusLine(sl)
		restore := silenceStdout()
		summary, err := spec.Sync(args)
		restore()
		SetActiveStatusLine(nil)
		EndStepDiagnostics()
		if err != nil && summary == "" {
			summary = "error: " + truncErr(err)
		}
		if summary == "" {
			summary = "already synced"
		}
		sl.Final(StepMark(err, diag), summary)
		if err != nil {
			return err
		}
	}
	return nil
}

// renderPullPlan renders the plan table for `chb pull` / `chb sync`'s
// pull phase. One row per provider that will actually run (driven by
// providerCommandSpecs), with the per-account detail rows from
// pullProviderHeaderItems folded in below their parent provider. The
// table-from-items helper used by push would silently drop providers
// without an Odoo-style scope (Nostr, Monerium-with-no-slug, Odoo)
// because compact mode skips empty-scope rows — and that gave the
// operator a misleading "this is everything" plan.
func renderPullPlan(specs []providerCommandSpec, items []SyncHeaderItem) {
	itemsByLabel := map[string][]SyncHeaderItem{}
	for _, it := range items {
		itemsByLabel[it.Label] = append(itemsByLabel[it.Label], it)
	}
	type planRow struct {
		Label    string
		Scope    string
		LastSync time.Time
	}
	var rows []planRow
	for _, spec := range specs {
		if spec.Sync == nil {
			continue
		}
		label := providerDisplayName(spec.Name)
		matches := itemsByLabel[label]
		if len(matches) == 0 {
			rows = append(rows, planRow{Label: label})
			continue
		}
		for _, m := range matches {
			rows = append(rows, planRow{Label: label, Scope: m.Scope, LastSync: m.LastSync})
		}
	}
	if len(rows) == 0 {
		return
	}
	labelW, scopeW := 0, 0
	for _, r := range rows {
		if n := displayWidth(r.Label); n > labelW {
			labelW = n
		}
		if n := displayWidth(r.Scope); n > scopeW {
			scopeW = n
		}
	}
	if scopeW > 40 {
		scopeW = 40
	}
	for _, r := range rows {
		last := formatRelativeAndAbsolute(r.LastSync)
		fmt.Printf("    %-*s  %-*s  %s%s%s\n",
			labelW, r.Label,
			scopeW, truncate(r.Scope, scopeW),
			Fmt.Dim, last, Fmt.Reset)
	}
}

func providerDisplayName(name string) string {
	switch name {
	case "ics":
		return "ICS"
	case "odoo":
		return "Odoo"
	default:
		if name == "" {
			return ""
		}
		return strings.ToUpper(name[:1]) + name[1:]
	}
}

func sortedProviderCommandSpecs() []providerCommandSpec {
	specs := providerCommandSpecs()
	sort.Slice(specs, func(i, j int) bool { return specs[i].Name < specs[j].Name })
	return specs
}
