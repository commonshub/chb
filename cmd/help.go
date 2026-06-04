package cmd

import (
	"fmt"
	"strings"
)

func PrintHelp(version string) {
	f := Fmt
	versionLabel := version
	if versionLabel == "" {
		versionLabel = "dev"
	} else if versionLabel != "dev" {
		versionLabel = "v" + versionLabel
	}
	fmt.Printf(`
%schb%s %s%s%s — Commons Hub Brussels CLI

%sUSAGE%s
  %schb%s <command> [options]

%sCOMMANDS%s
  %sevents%s              List upcoming events
  %scalendars%s           Show calendar summary
  %scalendars pull%s      Pull calendar providers
  %sevents stats%s        Show event statistics
  %srooms%s               List all rooms with pricing
  %sbookings%s            List upcoming room bookings
  %sbookings stats%s      Show booking statistics
  %stransactions pull%s   Fetch blockchain / Stripe / Monerium transactions
  %stransactions stats%s  Show transaction statistics
  %ssearch%s              Spotlight search across txs, invoices & bills (-i for TUI)
  %scontacts%s            Look up a contact + its invoices, bills & transactions
  %snostr pull/push%s     Fetch/publish Nostr annotations
  %sinvoices pull%s       Fetch outgoing invoices from Odoo
  %sbills pull%s          Fetch vendor bills from Odoo
  %sattachments pull%s    Download invoice and bill attachments from Odoo
  %smessages pull%s       Fetch Discord messages
  %smessages stats%s      Show message statistics
  %simages pull%s         Download images from Discord and Luma
  %sproviders%s           List providers and provider-scoped commands
  %spull%s                Pull from every configured source
  %sgenerate%s            Generate derived data files (transactions, events, …)
  %spush%s                Push to every configured target (Odoo journals + Nostr)
  %ssync%s                Full cron loop: chb pull && chb push
  %smembers pull%s        Fetch membership data from Stripe/Odoo
  %sreport%s <date-range>  Generate monthly/yearly report
  %sincome%s <date-range>  Income by category for a date range
  %sexpenses%s <date-range>  Expenses by category for a date range
  %sstatus%s              Show version, data dirs, and last sync at a glance
  %sstats%s               Show data directory size and breakdown
  %sclean%s               Migrate legacy file layouts and prune stale dirs
  %sdoctor%s              Audit DATA_DIR integrity and suggest fixes
  %stools%s               Run debugging helpers

%sOPTIONS%s
  %s--help, -h%s          Show help for a command
  %s--version, -v%s       Show version info
  %s--odoo-db <slug>%s    Override the Odoo DB (auto-derives URL: <slug>.odoo.com)
  %s--odoo-url <url>%s    Override the Odoo URL (auto-derives DB from hostname)
  %ssetup%s               Configure API keys interactively
  %sversion%s             Show version info
  %supdate%s              Check for updates and install latest release
  %supdate -y%s           Update without confirmation

%sEXAMPLES%s
  %s$ chb sync                               # the full loop: pull from sources, push to targets (cron-friendly)
  $ chb pull                                # pull from every configured source
  $ chb push                                # push to every configured target (Odoo + Nostr)
  $ chb generate                            # rebuild generated/ outputs
  $ chb events                              # next 10 upcoming events
  $ chb pull 2025/11 --force                # re-pull everything for Nov 2025
  $ chb calendars pull                      # pull calendar providers only
  $ chb transactions pull 2025/03           # pull transactions for Mar 2025
  $ chb accounts stripe pull                # pull one account from its source
  $ chb odoo pull                           # fetch Odoo state (categories, partners, journal lines)
  $ chb odoo journals push                  # push to every linked Odoo journal
  $ chb odoo journals 28 push --dry-run     # preview push for journal #28
  $ chb odoo journals 48 reconcile          # run the reconcile verb on one journal
  $ chb odoo journals 48 pull               # refresh local cache for one journal
  $ chb --odoo-db citizenspring-test2 odoo journals 48
  $ chb nostr push                          # publish pending Nostr events
  $ chb providers ics generate              # generate calendar outputs
  $ chb providers * pull 2025/11            # pull all providers for Nov 2025
  $ chb report 2025/11                      # monthly report
  $ chb report 202511                       # monthly report
  $ chb report 2025                         # yearly report%s

%sENVIRONMENT%s
  %sAPP_DATA_DIR%s        App state directory; config is in $APP_DATA_DIR/settings (default: ~/.chb)
  %sDATA_DIR%s            Generated data directory (default: $APP_DATA_DIR/data)

  %sETHERSCAN_API_KEY%s   Etherscan/Gnosisscan API key
  %sDISCORD_BOT_TOKEN%s   Discord bot token
`,
		f.Bold, f.Reset, f.Dim, versionLabel, f.Reset, // chb v<version>
		f.Bold, f.Reset, // USAGE
		f.Cyan, f.Reset, // chb in usage
		f.Bold, f.Reset, // COMMANDS
		// 32 command rows (events, calendars, calendars pull, events stats,
		// rooms, bookings, bookings stats, transactions pull, transactions stats,
		// search, contacts, nostr pull/push, invoices pull, bills pull, attachments
		// pull, messages pull, messages stats, images pull, providers, pull,
		// generate, push, sync, members pull, report, income, expenses,
		// status, stats, clean, doctor, tools)
		f.Cyan, f.Reset, f.Cyan, f.Reset, f.Cyan, f.Reset, f.Cyan, f.Reset, f.Cyan, f.Reset,
		f.Cyan, f.Reset, f.Cyan, f.Reset, f.Cyan, f.Reset, f.Cyan, f.Reset, f.Cyan, f.Reset,
		f.Cyan, f.Reset, f.Cyan, f.Reset, f.Cyan, f.Reset, f.Cyan, f.Reset, f.Cyan, f.Reset,
		f.Cyan, f.Reset, f.Cyan, f.Reset, f.Cyan, f.Reset, f.Cyan, f.Reset, f.Cyan, f.Reset,
		f.Cyan, f.Reset, f.Cyan, f.Reset, f.Cyan, f.Reset, f.Cyan, f.Reset, f.Cyan, f.Reset,
		f.Cyan, f.Reset, f.Cyan, f.Reset, f.Cyan, f.Reset, f.Cyan, f.Reset, f.Cyan, f.Reset,
		f.Cyan, f.Reset, f.Cyan, f.Reset, // search + contacts (31st, 32nd rows)
		f.Bold, f.Reset, // OPTIONS
		// 8 options rows
		f.Yellow, f.Reset, f.Yellow, f.Reset, f.Yellow, f.Reset, f.Yellow, f.Reset,
		f.Yellow, f.Reset, f.Yellow, f.Reset, f.Yellow, f.Reset, f.Yellow, f.Reset,
		f.Bold, f.Reset, // EXAMPLES
		f.Dim, f.Reset, // example block
		f.Bold, f.Reset, // ENVIRONMENT
		// 4 env vars
		f.Yellow, f.Reset, f.Yellow, f.Reset, f.Yellow, f.Reset, f.Yellow, f.Reset,
	)
}

func PrintProvidersHelp() {
	f := Fmt
	fmt.Printf(`
%schb providers%s — Provider command registry

%sUSAGE%s
  %schb providers%s
  %schb providers%s <provider|*> <pull|generate> [year[/month]] [options]

%sPROVIDERS%s
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
	)
	for _, spec := range sortedProviderCommandSpecs() {
		fmt.Printf("  %s%-10s%s %s\n", f.Cyan, spec.Name, f.Reset, strings.Join(spec.Commands, "|"))
		if spec.Description != "" {
			fmt.Printf("             %s%s%s\n", f.Dim, spec.Description, f.Reset)
		}
	}
	fmt.Printf(`
%sALIASES%s
  %schb pull%s       Same as %schb providers * pull%s
  %schb generate%s   Same as %schb providers * generate%s

%sEXAMPLES%s
  %schb providers ics pull%s 2025/11
  %schb providers ics generate%s
  %schb providers stripe pull%s --since 2025/01
  %schb providers * generate%s

%sNote%s
  %sProviders are pull-only sources. Push to targets via: chb odoo journals push / chb nostr push.
  Or run the full loop in one shot: chb sync (= chb pull && chb push).%s
`,
		f.Bold, f.Reset,
		f.Cyan, f.Reset, f.Cyan, f.Reset,
		f.Cyan, f.Reset, f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Dim, f.Reset,
	)
}

func PrintProviderHelp(spec providerCommandSpec) {
	f := Fmt
	fmt.Printf(`
%schb providers %s%s — %s

%sUSAGE%s
  %schb providers %s pull%s [year[/month]] [options]
  %schb providers %s generate%s [year[/month]] [options]

%sCOMMANDS%s
  %spull%s       Pull provider data into the monthly archive (alias: sync)
  %sgenerate%s   Transform archived provider data into generated outputs
`,
		f.Bold, spec.Name, f.Reset, spec.Description,
		f.Bold, f.Reset,
		f.Cyan, spec.Name, f.Reset,
		f.Cyan, spec.Name, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
	)
}

func PrintToolsHelp() {
	f := Fmt
	fmt.Printf(`
%schb tools%s — Debugging helpers

%sUSAGE%s
  %schb tools%s <command> [options]

%sCOMMANDS%s
  %sgetUrlMetadata%s <url>   Fetch title, description, and og:image from a page

%sEXAMPLES%s
  %schb tools getUrlMetadata%s https://example.com/event
  %schb tools getUrlMetadata%s https://example.com/event --verbose
  %schb tools getUrlMetadata%s https://example.com/event --debug
	`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
	)
}

func PrintGetURLMetadataHelp() {
	f := Fmt
	fmt.Printf(`
%schb tools getUrlMetadata%s — Fetch URL metadata for debugging

%sUSAGE%s
  %schb tools getUrlMetadata%s <url> [--verbose] [--debug]

%sOUTPUT%s
  Prints the fetched URL, final URL after redirects, HTTP status, content type,
  title, description, og:image, and explicit fetch/debug errors.

%sOPTIONS%s
  %s--verbose, -v%s       Also print all discovered HTML meta tags
  %s--debug%s             Write debug.<domain>.log with request/response details
  %s--help, -h%s          Show this help
	`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
	)
}

func PrintDoctorHelp() {
	f := Fmt
	fmt.Printf(`
%schb doctor%s — Audit the local data directory

%sUSAGE%s
  %schb doctor%s

%sCHECKS%s
  • Room Discord channel directories exist in latest/providers/discord/
  • Generated files exist when provider archives are present
  • images.json entries use canonical year/month image paths
  • Referenced local image files exist under DATA_DIR
  • images.json does not contain deprecated proxyUrl fields or \u escapes

%sEXIT STATUS%s
  Returns non-zero when issues are found.

%sEXAMPLES%s
  %schb doctor%s
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
	)
}

func PrintSyncAllHelp() {
	f := Fmt
	printMirrorModeHelpBanner()
	fmt.Printf(`
%schb pull%s — Pull data from every configured provider (remote → local)

%sProviders:%s calendars (room bookings and public events), transactions (Gnosis/Stripe/Monerium),
invoices/bills/attachments (Odoo), messages (Discord), members (Stripe/Odoo), images (Discord/Luma).
For the full loop (pull + push), use: chb sync.

%sUSAGE%s
  %schb pull%s [year[/month]] [options]
  %schb providers * pull%s [year[/month]] [options]
  %schb calendars pull%s [year[/month]] [options]
  %schb transactions pull%s [year[/month]] [options]
  %schb invoices pull%s [year[/month]] [options]
  %schb bills pull%s [year[/month]] [options]
  %schb attachments pull%s [year[/month]] [options]
  %schb messages pull%s [year[/month]] [options]
  %schb members pull%s [options]

%sTIME RANGE%s
  %s(no args)%s            Pull from the last successful sync until now
  %s<date-range>%s         Pull a date/month/year range (e.g. 2025/11, 2025/Q4)
  %s--since%s <date>       Pull from a date to now
  %s--history%s            Pull from earliest cached month (or 2024/01 if fresh)

%sOPTIONS%s
  %s--force%s              Re-fetch even if cached data exists
  %s--verbose, -v%s        Show per-step progress instead of the compact view
  %s--help, -h%s           Show this help

%sEXAMPLES%s
  %schb pull%s                        Pull latest data (all providers)
  %schb pull --history%s              Pull history from where cache left off
  %schb pull --since 2024/06%s        Pull from June 2024 to now
  %schb pull 2025%s                   Pull all of 2025
  %schb pull 2025/11%s                Pull November 2025
  %schb pull 2025/11 --force%s        Re-pull November 2025 (overwrite cache)
  %schb calendars pull%s              Pull calendars only (latest)
  %schb calendars pull --history%s    Pull calendar history
  %schb transactions pull --since 202401%s  Pull transactions from Jan 2024
  %schb invoices pull%s               Pull outgoing invoices (latest)
  %schb bills pull%s                  Pull vendor bills (latest)
  %schb attachments pull%s            Pull Odoo attachments (latest)
`,
		f.Bold, f.Reset,
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
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Dim, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Bold, f.Reset, // OPTIONS
		f.Yellow, f.Reset, // --force
		f.Yellow, f.Reset, // --verbose
		f.Yellow, f.Reset, // --help
		f.Bold, f.Reset, // EXAMPLES
		f.Cyan, f.Reset, f.Cyan, f.Reset, f.Cyan, f.Reset, // pull / --history / --since
		f.Cyan, f.Reset, f.Cyan, f.Reset, f.Cyan, f.Reset, // 2025 / 2025/11 / 2025/11 --force
		f.Cyan, f.Reset, f.Cyan, f.Reset, f.Cyan, f.Reset, // calendars / --history / transactions --since
		f.Cyan, f.Reset, f.Cyan, f.Reset, f.Cyan, f.Reset, // invoices / bills / attachments
	)
}

// PrintStatsHelp is defined in stats.go

func PrintEventsHelp() {
	f := Fmt
	fmt.Printf(`
%schb events%s — List events from the local data directory

%sUSAGE%s
  %schb events%s [options]
  %schb events stats%s [year[/month]]   Event counts/attendance summary
  %schb events tickets%s [year[/month]] Ticket-sale summary (gross/fees/VAT/net)

%sOPTIONS%s
  %s-n%s <count>           Number of events to show (default: 10)
  %s--since%s <date>       Events from this date, sorted oldest first
  %s--until%s <date>       Events up to this date, sorted newest first
  %s--skip%s <count>       Skip first N events
  %s--all%s                Show all events (no date filter)
  %s--help, -h%s           Show this help

See %schb events stats --help%s and %schb events tickets --help%s for subcommand options.
`,
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
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
	)
}

func PrintCalendarsHelp() {
	f := Fmt
	fmt.Printf(`
%schb calendars%s — Show calendar source summary

%sUSAGE%s
  %schb calendars%s
  %schb calendars sync%s [year[/month]] [options]

%sOUTPUT%s
  Lists each calendar source with total public events and private bookings.

%sOPTIONS%s
  %s--months, --breakdown%s  Include month-by-month public/private counts.
  %s--since%s <date>         Only include entries from this date.
  %s--until%s <date>         Only include entries up to this date.
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
	)
}

func PrintCalendarsSyncHelp() {
	f := Fmt
	fmt.Printf(`
%schb calendars sync%s — Sync calendar providers

%sUSAGE%s
  %schb calendars sync%s [year[/month]] [options]

%sOPTIONS%s
  %s<date-range>%s         Sync a date/month/year range (e.g. 2025/11, 2025/Q4)
  %s--since%s <date>       Start syncing from this date (default: previous month)
  %s--force%s              Re-fetch even if cached data exists
  %s--debug%s              Write debug.<domain>.log for OG fetch issues
  %s--history%s            Rebuild entire event history
  %s--help, -h%s           Show this help

%sSOURCES%s
  • Configured calendar providers from calendars.json
  • ICS feeds use provider: "ics"
  • Room feeds reference rooms via the source room field
  • Public events are derived later by 'chb generate events'
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Bold, f.Reset,
	)
}

func PrintBookingsHelp() {
	f := Fmt
	fmt.Printf(`
%schb bookings%s — List room bookings from cached calendar data

%sUSAGE%s
  %schb bookings%s [options]

%sOPTIONS%s
  %s-n%s <count>           Number of bookings to show (default: 10)
  %s--skip%s <count>       Skip first N bookings
  %s--date%s <date-range>  Show bookings for a date/month/year range
  %s--room%s <slug>        Filter by room slug
  %s--all%s                Show all bookings (no date filter)
  %s--help, -h%s           Show this help
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
	)
}

func PrintEventsStatsHelp() {
	f := Fmt
	fmt.Printf(`
%schb events stats%s — Show event statistics

%sUSAGE%s
  %schb events stats%s [options]

%sOPTIONS%s
  %s--format json%s        Output as JSON
  %s--help, -h%s           Show this help
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
	)
}

func PrintBookingsStatsHelp() {
	f := Fmt
	fmt.Printf(`
%schb bookings stats%s — Show booking statistics

%sUSAGE%s
  %schb bookings stats%s [options]

%sOPTIONS%s
  %s--format json%s        Output as JSON
  %s--help, -h%s           Show this help
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
	)
}

func PrintMessagesStatsHelp() {
	f := Fmt
	fmt.Printf(`
%schb messages stats%s — Show message statistics

%sUSAGE%s
  %schb messages stats%s [options]

%sOPTIONS%s
  %s--format json%s        Output as JSON
  %s--help, -h%s           Show this help
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
	)
}
