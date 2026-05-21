package main

import (
	"os"
	"strings"

	"github.com/CommonsHub/chb/cmd"
)

// VERSION is injected at release build time via ldflags.
var VERSION string

func exitWithError(err error) {
	// Blank line on stderr before the footer so it visually separates
	// from whatever the failed step last printed (status-line rewrites,
	// per-journal rows, etc. can otherwise run flush against the
	// "Error:" line and bury it). Kept out of the Errorf payload so the
	// diagnostics log file doesn't pick up the stray blank.
	os.Stderr.WriteString("\n")
	cmd.Errorf("%sError:%s %v", cmd.Fmt.Red, cmd.Fmt.Reset, err)
	exitAfterDiagnostics()
}

func exitWithUsage(format string, args ...interface{}) {
	cmd.Errorf(format, args...)
	exitAfterDiagnostics()
}

func exitAfterDiagnostics() {
	cmd.ExitWithDiagnostics(1)
}

func main() {
	cmd.Version = cmd.ResolveVersion(VERSION)
	cmd.EnsureSettingsBootstrapped()
	cmd.LoadEnvFromConfig()
	defer cmd.CloseDiagnosticsLog()
	defer cmd.PrintDiagnosticsSummary()

	// Global flags that override settings loaded from config.env. Parsed and
	// stripped from os.Args before any command sees them so subcommand
	// arg-parsing doesn't have to know about them. Each maps to the env var
	// ResolveOdooCredentials reads.
	args := applyGlobalOdooFlags(os.Args[1:])

	if len(args) == 0 {
		cmd.PrintHelp(cmd.Version)
		return
	}

	if needsWritableDataDir(args) {
		if _, err := cmd.EnsureWritableDataDir(); err != nil {
			exitWithError(err)
		}
	}

	switch args[0] {
	case "--help", "-h", "help":
		cmd.PrintHelp(cmd.Version)
	case "--version", "-v", "version":
		cmd.PrintVersion()
	case "setup":
		if len(args) > 1 && args[1] == "nostr" {
			if err := cmd.SetupNostr(); err != nil {
				exitWithError(err)
			}
		} else if len(args) > 1 && args[1] == "odoo" {
			if err := cmd.SetupOdoo(); err != nil {
				exitWithError(err)
			}
		} else {
			if err := cmd.Setup(); err != nil {
				exitWithError(err)
			}
		}
	case "settings":
		cmd.PrintSettings()
	case "tokens":
		cmd.Tokens(args[1:])
	case "update":
		yes := cmd.HasFlag(args[1:], "--yes", "-y")
		if err := cmd.Update(yes); err != nil {
			exitWithError(err)
		}
	case "calendars":
		if len(args) > 1 && args[1] == "sync" {
			_, _, err := cmd.CalendarsSync(args[2:])
			if err != nil {
				exitWithError(err)
			}
		} else if len(args) > 1 && (args[1] == "help" || args[1] == "--help" || args[1] == "-h") {
			cmd.PrintCalendarsHelp()
		} else {
			cmd.Calendars(args[1:])
		}
	case "fridge":
		cmd.Fridge(args[1:])
	case "vendors":
		cmd.Vendors(args[1:])
	case "customers":
		cmd.Customers(args[1:])
	case "events":
		if len(args) > 1 && args[1] == "sync" {
			exitWithUsage("%s`chb events sync` was removed. Use `chb calendars sync`.%s", cmd.Fmt.Yellow, cmd.Fmt.Reset)
		} else if len(args) > 1 && args[1] == "stats" {
			cmd.EventsStats(args[2:])
		} else if len(args) > 1 && args[1] == "tickets" {
			cmd.EventsTickets(args[2:])
		} else {
			cmd.EventsList(args[1:])
		}
	case "rooms":
		cmd.Rooms(args[1:])
	case "bookings":
		if len(args) > 1 && args[1] == "sync" {
			exitWithUsage("%s`chb bookings sync` was removed. Use `chb calendars sync`.%s", cmd.Fmt.Yellow, cmd.Fmt.Reset)
		} else if len(args) > 1 && args[1] == "stats" {
			cmd.BookingsStats(args[2:])
		} else {
			cmd.BookingsList(args[1:])
		}
	case "transactions":
		// Parse subcommand and currency from args in any order
		// e.g. "transactions sync", "transactions EUR categorize", "transactions CHT"
		txArgs := args[1:]
		txSubcmd := ""
		for _, a := range txArgs {
			switch strings.ToLower(a) {
			case "sync", "categorize", "stats":
				txSubcmd = strings.ToLower(a)
			case "publish":
				exitWithUsage("%s`chb transactions publish` was removed. Use `chb nostr sync transactions`.%s", cmd.Fmt.Yellow, cmd.Fmt.Reset)
			}
		}
		// Check for "sync nostr" compound subcommand
		hasSyncNostr := txSubcmd == "sync" && hasArg(txArgs, "nostr")

		switch {
		case hasSyncNostr:
			exitWithUsage("%s`chb transactions sync nostr` was removed. Use `chb nostr sync transactions`.%s", cmd.Fmt.Yellow, cmd.Fmt.Reset)
		case txSubcmd == "sync":
			if _, err := cmd.TransactionsSync(txArgs); err != nil {
				exitWithError(err)
			}
		case txSubcmd == "categorize":
			cmd.TransactionsCategorize(txArgs)
		case txSubcmd == "stats":
			cmd.TransactionsStats(txArgs)
		default:
			cmd.TransactionsBrowser(txArgs)
		}
	case "invoices":
		invSub := ""
		if len(args) > 1 {
			invSub = args[1]
		}
		switch invSub {
		case "sync":
			if len(args) > 2 && args[2] == "nostr" {
				exitWithUsage("%s`chb invoices sync nostr` was removed. Use `chb nostr sync invoices`.%s", cmd.Fmt.Yellow, cmd.Fmt.Reset)
			}
			if _, err := cmd.InvoicesSync(args[1:]); err != nil {
				exitWithError(err)
			}
		case "categorize":
			if err := cmd.InvoicesCategorize(args[2:]); err != nil {
				exitWithError(err)
			}
		case "publish":
			exitWithUsage("%s`chb invoices publish` was removed. Use `chb nostr sync invoices`.%s", cmd.Fmt.Yellow, cmd.Fmt.Reset)
		default:
			if err := cmd.InvoicesList(args[1:]); err != nil {
				exitWithError(err)
			}
		}
	case "bills":
		billSub := ""
		if len(args) > 1 {
			billSub = args[1]
		}
		switch billSub {
		case "sync":
			if len(args) > 2 && args[2] == "nostr" {
				exitWithUsage("%s`chb bills sync nostr` was removed. Use `chb nostr sync bills`.%s", cmd.Fmt.Yellow, cmd.Fmt.Reset)
			}
			if _, err := cmd.BillsSync(args[1:]); err != nil {
				exitWithError(err)
			}
		case "categorize":
			if err := cmd.BillsCategorize(args[2:]); err != nil {
				exitWithError(err)
			}
		case "publish":
			exitWithUsage("%s`chb bills publish` was removed. Use `chb nostr sync bills`.%s", cmd.Fmt.Yellow, cmd.Fmt.Reset)
		default:
			if err := cmd.BillsList(args[1:]); err != nil {
				exitWithError(err)
			}
		}
	case "messages":
		if len(args) > 1 && args[1] == "sync" {
			if _, err := cmd.MessagesSync(args[2:]); err != nil {
				exitWithError(err)
			}
		} else if len(args) > 1 && args[1] == "stats" {
			cmd.MessagesStats(args[2:])
		} else {
			exitWithUsage("%sUsage: chb messages [sync|stats]%s", cmd.Fmt.Yellow, cmd.Fmt.Reset)
		}
	case "images":
		if len(args) > 1 && (args[1] == "sync" || args[1] == "help" || args[1] == "--help" || args[1] == "-h") {
			if _, err := cmd.ImagesSync(args[1:]); err != nil {
				exitWithError(err)
			}
		} else {
			exitWithUsage("%sUsage: chb images sync [options]%s", cmd.Fmt.Yellow, cmd.Fmt.Reset)
		}
	case "attachments":
		if len(args) > 1 && (args[1] == "sync" || args[1] == "help" || args[1] == "--help" || args[1] == "-h") {
			if _, err := cmd.AttachmentsSync(args[1:]); err != nil {
				exitWithError(err)
			}
		} else {
			exitWithUsage("%sUsage: chb attachments sync [options]%s", cmd.Fmt.Yellow, cmd.Fmt.Reset)
		}
	case "providers":
		if err := cmd.ProvidersCommand(args[1:]); err != nil {
			exitWithError(err)
		}
	case "generate":
		if err := cmd.ProvidersCommand(append([]string{"*", "generate"}, args[1:]...)); err != nil {
			exitWithError(err)
		}
	case "members":
		if len(args) > 1 && args[1] == "sync" {
			if err := cmd.MembersSync(args[2:]); err != nil {
				exitWithError(err)
			}
		} else {
			cmd.MembersStats(args[1:])
		}
	case "odoo":
		subcmd := ""
		if len(args) > 1 {
			subcmd = args[1]
		}
		switch subcmd {
		case "pull", "sync":
			if subcmd == "sync" {
				cmd.Warnf("%s'chb odoo sync' is deprecated — use 'chb odoo pull' instead%s", cmd.Fmt.Dim, cmd.Fmt.Reset)
			}
			// Fetch-only: invoices + bills + partners + analytic plans
			// + categories. Pushing to journals lives under
			// `chb odoo journals push` (see below).
			if err := cmd.OdooSyncAll(args[2:]); err != nil {
				exitWithError(err)
			}
		case "categories":
			// Back-compat: what `chb odoo sync` used to be.
			catArgs := args[2:]
			if len(catArgs) > 0 && catArgs[0] == "sync" {
				catArgs = catArgs[1:]
			}
			if _, err := cmd.OdooAnalyticSync(catArgs); err != nil {
				exitWithError(err)
			}
		case "partners":
			partnerArgs := args[2:]
			if len(partnerArgs) > 0 && partnerArgs[0] == "sync" {
				partnerArgs = partnerArgs[1:]
			}
			if _, err := cmd.OdooPartnersSync(partnerArgs); err != nil {
				exitWithError(err)
			}
		case "invoices":
			invArgs := args[2:]
			if len(invArgs) > 0 && invArgs[0] == "sync" {
				invArgs = invArgs[1:]
			}
			if _, err := cmd.InvoicesSync(invArgs); err != nil {
				exitWithError(err)
			}
		case "bills":
			billArgs := args[2:]
			if len(billArgs) > 0 && billArgs[0] == "sync" {
				billArgs = billArgs[1:]
			}
			if _, err := cmd.BillsSync(billArgs); err != nil {
				exitWithError(err)
			}
		case "journals":
			if err := cmd.OdooJournals(args[2:]); err != nil {
				exitWithError(err)
			}
		case "reconcile":
			if err := cmd.OdooReconcileCommand(args[2:]); err != nil {
				exitWithError(err)
			}
		case "get":
			if err := cmd.OdooGet(args[2:]); err != nil {
				exitWithError(err)
			}
		case "mapping", "mappings":
			if err := cmd.OdooMappingCommand(args[2:]); err != nil {
				exitWithError(err)
			}
		case "rules":
			cmd.Warnf("%s'chb odoo rules' is deprecated — use 'chb odoo mapping' instead%s", cmd.Fmt.Dim, cmd.Fmt.Reset)
			if err := cmd.OdooMappingCommand(args[2:]); err != nil {
				exitWithError(err)
			}
		case "accounts":
			if err := cmd.OdooAccountsCommand(args[2:]); err != nil {
				exitWithError(err)
			}
		case "backup":
			if err := cmd.OdooBackup(args[2:]); err != nil {
				exitWithError(err)
			}
		default:
			cmd.PrintOdooHelp()
		}
	case "nostr":
		if len(args) <= 1 {
			exitWithUsage("%sUsage: chb nostr <pull|push|pending|sync> [scope] [options]%s", cmd.Fmt.Yellow, cmd.Fmt.Reset)
		}
		switch args[1] {
		case "pull":
			if err := cmd.NostrPull(args[2:]); err != nil {
				exitWithError(err)
			}
		case "push":
			if err := cmd.NostrPush(args[2:]); err != nil {
				exitWithError(err)
			}
		case "publish":
			cmd.Warnf("%s'chb nostr publish' is deprecated — use 'chb nostr push' instead%s", cmd.Fmt.Dim, cmd.Fmt.Reset)
			if err := cmd.NostrPush(args[2:]); err != nil {
				exitWithError(err)
			}
		case "pending":
			if err := cmd.NostrPending(args[2:]); err != nil {
				exitWithError(err)
			}
		case "sync":
			cmd.Warnf("%s'chb nostr sync' is deprecated — use 'chb nostr pull' / 'chb nostr push' instead%s", cmd.Fmt.Dim, cmd.Fmt.Reset)
			if err := cmd.NostrSync(args[2:]); err != nil {
				exitWithError(err)
			}
		default:
			exitWithUsage("%sUsage: chb nostr <pull|push|pending|sync> [scope] [options]%s", cmd.Fmt.Yellow, cmd.Fmt.Reset)
		}
	case "rules":
		if len(args) > 1 && args[1] == "add" {
			cmd.RulesAdd(args[2:])
		} else {
			cmd.RulesCommand(args[1:])
		}
	case "accounts":
		cmd.AccountsCommand(args[1:])
	case "stats":
		cmd.Stats(args[1:])
	case "doctor":
		if err := cmd.Doctor(args[1:]); err != nil {
			exitWithError(err)
		}
	case "tools":
		if err := cmd.Tools(args[1:]); err != nil {
			exitWithError(err)
		}
	case "pull":
		if err := cmd.ProvidersCommand(append([]string{"*", "pull"}, args[1:]...)); err != nil {
			exitWithError(err)
		}
	case "push":
		// Push to every target (Odoo journals + Nostr outbox). Useful as
		// the second half of `chb sync` and as a standalone "publish
		// everything ready" call for cron jobs that pull continuously and
		// push periodically.
		if err := cmd.PushAllTargets(args[1:]); err != nil {
			exitWithError(err)
		}
	case "sync":
		// `chb sync` is the full hourly-cron loop: pull from every source,
		// generate the derived outputs, then push to every target.
		// Equivalent to:
		//   chb pull && chb generate && chb push
		// Designed to be safe to run unattended (push uses the
		// auto-reconcile-when-≤20-new-lines policy so manual back-fills
		// stay out of cron's reach).
		if cmd.HasFlag(args[1:], "--help", "-h", "help") {
			cmd.PrintSyncCronHelp()
			return
		}
		if err := cmd.ProvidersCommand(append([]string{"*", "pull"}, args[1:]...)); err != nil {
			exitWithError(err)
		}
		if err := cmd.Generate(args[1:]); err != nil {
			exitWithError(err)
		}
		if err := cmd.PushAllTargets(args[1:]); err != nil {
			exitWithError(err)
		}
	case "report":
		if err := cmd.Report(args[1:]); err != nil {
			exitWithError(err)
		}
	case "income":
		if err := cmd.Income(args[1:]); err != nil {
			exitWithError(err)
		}
	case "expenses":
		if err := cmd.Expenses(args[1:]); err != nil {
			exitWithError(err)
		}
	default:
		cmd.Errorf("%sUnknown command: %s%s", cmd.Fmt.Red, args[0], cmd.Fmt.Reset)
		cmd.PrintHelp(cmd.Version)
		exitAfterDiagnostics()
	}
}

// applyGlobalOdooFlags consumes Odoo-targeting flags from args and applies
// them as env-var overrides before any subcommand parses its own args.
// Recognised forms (kebab- and snake-case both accepted):
//
//	--odoo-db <slug>   / --odoo_db=<slug>     → overrides ODOO_DATABASE (+ infers URL)
//	--odoo-url <url>   / --odoo_url=<url>     → overrides ODOO_URL (+ infers DB)
//
// **Interchangeable**: when only one of the two is provided, the other is
// derived using the convention `https://<db>.odoo.com`. So
// `chb --odoo-db citizenspring-test2 ...` is enough to retarget the whole
// session. Passing both flags pins each independently (useful for self-hosted
// instances whose URL doesn't follow the SaaS pattern).
//
// Strips the consumed args so subcommands never see them.
func applyGlobalOdooFlags(args []string) []string {
	out := make([]string, 0, len(args))
	dbFlag, urlFlag := "", ""
	knownPrefixes := []string{"--odoo-db", "--odoo_db", "--odoo-url", "--odoo_url"}
	for i := 0; i < len(args); i++ {
		a := args[i]
		consumed := false
		for _, p := range knownPrefixes {
			value := ""
			if a == p {
				if i+1 < len(args) {
					value = args[i+1]
					i++
				}
				consumed = true
			} else if strings.HasPrefix(a, p+"=") {
				value = a[len(p)+1:]
				consumed = true
			}
			if !consumed {
				continue
			}
			switch p {
			case "--odoo-db", "--odoo_db":
				dbFlag = value
			case "--odoo-url", "--odoo_url":
				urlFlag = value
			}
			break
		}
		if !consumed {
			out = append(out, a)
		}
	}

	// Derive the missing half. The convention `<db>.odoo.com` holds for
	// every Odoo SaaS instance — operators with a self-hosted setup just
	// pass both flags explicitly.
	if dbFlag != "" && urlFlag == "" {
		urlFlag = "https://" + dbFlag + ".odoo.com"
	}
	if urlFlag != "" && dbFlag == "" {
		dbFlag = cmd.OdooDBFromURL(urlFlag)
	}
	if urlFlag != "" {
		_ = os.Setenv("ODOO_URL", urlFlag)
	}
	if dbFlag != "" {
		_ = os.Setenv("ODOO_DATABASE", dbFlag)
	}
	return out
}

func hasArg(args []string, target string) bool {
	for _, a := range args {
		if strings.EqualFold(a, target) {
			return true
		}
	}
	return false
}

func needsWritableDataDir(args []string) bool {
	if len(args) == 0 || cmd.HasFlag(args, "--help", "-h", "help") {
		return false
	}

	switch args[0] {
	case "setup", "sync", "generate":
		return true
	case "providers":
		return len(args) > 2 && (strings.EqualFold(args[2], "sync") || strings.EqualFold(args[2], "generate"))
	case "calendars", "invoices", "bills", "messages", "images", "attachments", "members", "odoo":
		return len(args) > 1 && strings.EqualFold(args[1], "sync")
	case "transactions":
		return hasArg(args[1:], "sync")
	default:
		return false
	}
}
