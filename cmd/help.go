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
  %scalendars sync%s      Sync calendar providers
  %sevents stats%s        Show event statistics
  %srooms%s               List all rooms with pricing
  %sbookings%s            List upcoming room bookings
  %sbookings stats%s      Show booking statistics
  %stransactions sync%s   Fetch blockchain transactions
  %stransactions stats%s  Show transaction statistics
  %snostr sync%s          Publish/fetch Nostr annotations
  %sinvoices sync%s       Fetch outgoing invoices from Odoo
  %sbills sync%s          Fetch vendor bills from Odoo
  %sattachments sync%s    Download invoice and bill attachments from Odoo
  %smessages sync%s       Fetch Discord messages
  %smessages stats%s      Show message statistics
  %simages sync%s         Download images from Discord and Luma
  %sproviders%s           List providers and provider-scoped commands
  %ssync%s                Sync all providers
  %sgenerate%s            Generate derived data files (contributors, images, etc.)
  %smembers sync%s        Fetch membership data from Stripe/Odoo
  %sreport%s <date-range>  Generate monthly/yearly report
  %sstats%s               Show data directory size and breakdown
  %sdoctor%s              Audit DATA_DIR integrity and suggest fixes
  %stools%s               Run debugging helpers

%sOPTIONS%s
  %s--help, -h%s          Show help for a command
  %s--version, -v%s       Show version info
  %ssetup%s               Configure API keys interactively
  %sversion%s             Show version info
  %supdate%s              Check for updates and install latest release
  %supdate -y%s           Update without confirmation

%sEXAMPLES%s
  %s$ chb events                          # next 10 upcoming events
  $ chb calendars sync                   # sync calendar providers
  $ chb calendars sync 2025/11           # sync calendars for Nov 2025
  $ chb calendars sync 2025              # sync calendars for all of 2025
  $ chb sync 2025/11 --force             # resync everything for Nov 2025
  $ chb sync 2025 --force                # resync everything for all of 2025
  $ chb transactions sync 2025/03        # sync transactions for Mar 2025
  $ chb nostr sync transactions          # publish/fetch transaction annotations
  $ chb invoices sync                    # sync outgoing invoices from Odoo
  $ chb bills sync                       # sync vendor bills from Odoo
  $ chb attachments sync                 # sync invoice/bill attachments from Odoo
  $ chb messages sync 2025              # sync messages for all of 2025
  $ chb providers ics generate          # generate calendar outputs
  $ chb providers * sync 2025/11        # sync all providers for Nov 2025
  $ chb calendars sync 2025/06           # sync calendars for Jun 2025
  $ chb tools getUrlMetadata https://example.com/event
  $ chb report 2025/11                   # monthly report
  $ chb report 202511                    # monthly report
  $ chb report 2025                      # yearly report%s

%sENVIRONMENT%s
  %sAPP_DATA_DIR%s        App state directory; config is in $APP_DATA_DIR/settings (default: ~/.chb)
  %sDATA_DIR%s            Generated data directory (default: $APP_DATA_DIR/data)

  %sETHERSCAN_API_KEY%s   Etherscan/Gnosisscan API key
  %sDISCORD_BOT_TOKEN%s   Discord bot token
`,
		f.Bold, f.Reset, f.Dim, versionLabel, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
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
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
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
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Bold, f.Reset,
		f.Dim, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
	)
}

func PrintProvidersHelp() {
	f := Fmt
	fmt.Printf(`
%schb providers%s — Provider command registry

%sUSAGE%s
  %schb providers%s
  %schb providers%s <provider|*> <sync|generate> [year[/month]] [options]

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
  %schb sync%s       Same as %schb providers * sync%s
  %schb generate%s   Same as %schb providers * generate%s

%sEXAMPLES%s
  %schb providers ics sync%s 2025/11
  %schb providers ics generate%s
  %schb providers stripe sync%s --since 2025/01
  %schb providers * generate%s
`,
		f.Bold, f.Reset,
		f.Cyan, f.Reset, f.Cyan, f.Reset,
		f.Cyan, f.Reset, f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
	)
}

func PrintProviderHelp(spec providerCommandSpec) {
	f := Fmt
	fmt.Printf(`
%schb providers %s%s — %s

%sUSAGE%s
  %schb providers %s sync%s [year[/month]] [options]
  %schb providers %s generate%s [year[/month]] [options]

%sCOMMANDS%s
  %ssync%s       Fetch provider data into the monthly archive
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
	fmt.Printf(`
%schb sync%s — Sync all providers

%sProviders:%s calendars (room bookings and public events), transactions (Gnosis/Stripe),
invoices/bills/attachments (Odoo), messages (Discord), members (Stripe/Odoo)

%sUSAGE%s
  %schb sync%s [year[/month]] [options]
  %schb providers * sync%s [year[/month]] [options]
  %schb calendars sync%s [year[/month]] [options]
  %schb transactions sync%s [year[/month]] [options]
  %schb invoices sync%s [year[/month]] [options]
  %schb bills sync%s [year[/month]] [options]
  %schb attachments sync%s [year[/month]] [options]
  %schb messages sync%s [year[/month]] [options]
  %schb members sync%s [options]

%sTIME RANGE%s
  %s(no args)%s            Sync previous month + current month (and future events)
  %s<date-range>%s         Sync a date/month/year range (e.g. 2025/11, 2025/Q4)
  %s--since%s <date>       Sync from a date to now
  %s--history%s            Sync from earliest cached month (or 2024/01 if fresh)

%sOPTIONS%s
  %s--force%s              Re-fetch even if cached data exists
  %s--help, -h%s           Show this help

%sEXAMPLES%s
  %schb sync%s                        Sync latest data (all providers)
  %schb sync --history%s              Sync history from where cache left off
  %schb sync --since 2024/06%s       Sync from June 2024 to now
  %schb sync 2025%s                   Sync all of 2025
  %schb sync 2025/11%s                Sync November 2025
  %schb sync 2025/11 --force%s        Resync November 2025 (overwrite cache)
  %schb calendars sync%s              Sync calendars only (latest)
  %schb calendars sync --history%s    Sync calendar history
  %schb transactions sync --since 202401%s  Sync transactions from Jan 2024
  %schb invoices sync%s               Sync outgoing invoices (latest)
  %schb bills sync%s                  Sync vendor bills (latest)
  %schb attachments sync%s            Sync Odoo attachments (latest)
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
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
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
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
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
