package cmd

import "fmt"

func PrintHelp(version string) {
	f := Fmt
	fmt.Printf(`
%schb%s %sv%s%s — Commons Hub Brussels CLI

%sUSAGE%s
  %schb%s <command> [options]

%sCOMMANDS%s
  %sevents%s              List upcoming events
  %sevents sync%s         Fetch events from Luma feeds
  %sevents stats%s        Show event statistics
  %srooms%s               List all rooms with pricing
  %sbookings%s            List upcoming room bookings
  %sbookings sync%s       Sync room booking calendars
  %sbookings stats%s      Show booking statistics
  %stransactions sync%s   Fetch blockchain transactions
  %stransactions stats%s  Show transaction statistics
  %smessages sync%s       Fetch Discord messages
  %smessages stats%s      Show message statistics
  %simages sync%s         Download images from Discord and Luma
  %ssync%s                Sync everything (events, transactions, bookings, messages, generate, images)
  %sgenerate%s            Generate derived data files (contributors, images, etc.)
  %smembers sync%s        Fetch membership data from Stripe/Odoo
  %sreport%s <period>     Generate monthly/yearly report
  %sstats%s               Show data directory size and breakdown

%sOPTIONS%s
  %s--help, -h%s          Show help for a command
  %s--version, -v%s       Show version and check for updates
  %ssetup%s               Configure API keys interactively
  %sversion%s             Show version and check for updates
  %supdate%s              Update chb to the latest version

%sEXAMPLES%s
  %s$ chb events                          # next 10 upcoming events
  $ chb events sync                      # sync events from Luma
  $ chb events sync 2025/11              # sync events for Nov 2025
  $ chb events sync 2025                 # sync events for all of 2025
  $ chb sync 2025/11 --force             # resync everything for Nov 2025
  $ chb sync 2025 --force                # resync everything for all of 2025
  $ chb transactions sync 2025/03        # sync transactions for Mar 2025
  $ chb messages sync 2025              # sync messages for all of 2025
  $ chb bookings sync 2025/06            # sync bookings for Jun 2025
  $ chb report 2025/11                   # monthly report
  $ chb report 2025                      # yearly report%s

%sENVIRONMENT%s
  %sDATA_DIR%s            Data directory (default: ./data)
  %sLUMA_API_KEY%s        Luma API key (enables rich event data)
  %sETHERSCAN_API_KEY%s   Etherscan/Gnosisscan API key
  %sDISCORD_BOT_TOKEN%s   Discord bot token
`,
		f.Bold, f.Reset, f.Dim, version, f.Reset,
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

func PrintSyncAllHelp() {
	f := Fmt
	fmt.Printf(`
%schb sync%s — Sync all data sources

%sSources:%s events (Luma), transactions (Gnosis/Stripe), bookings (Google Calendar),
messages (Discord), members (Stripe/Odoo)

%sUSAGE%s
  %schb sync%s [year[/month]] [options]
  %schb events sync%s [year[/month]] [options]
  %schb transactions sync%s [year[/month]] [options]
  %schb bookings sync%s [year[/month]] [options]
  %schb messages sync%s [year[/month]] [options]
  %schb members sync%s [options]

%sTIME RANGE%s
  %s(no args)%s            Sync latest data (current month + future)
  %s<year/month>%s         Sync a specific month (e.g. 2025/11)
  %s<year>%s               Sync all months of a given year (e.g. 2025)
  %s--since%s YYYY/MM      Sync from a specific month to now (also: YYYYMM)
  %s--history%s            Sync from earliest cached month (or 2024/01 if fresh)

%sOPTIONS%s
  %s--force%s              Re-fetch even if cached data exists
  %s--help, -h%s           Show this help

%sEXAMPLES%s
  %schb sync%s                        Sync latest data (all sources)
  %schb sync --history%s              Sync history from where cache left off
  %schb sync --since 2024/06%s       Sync from June 2024 to now
  %schb sync 2025%s                   Sync all of 2025
  %schb sync 2025/11%s                Sync November 2025
  %schb sync 2025/11 --force%s        Resync November 2025 (overwrite cache)
  %schb events sync%s                 Sync events only (latest)
  %schb events sync --history%s       Sync event history
  %schb transactions sync --since 202401%s  Sync transactions from Jan 2024
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
		f.Bold, f.Reset,
		f.Dim, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
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
	)
}

func PrintStatsHelp() {
	f := Fmt
	fmt.Printf(`
%schb stats%s — Show data directory statistics

%sUSAGE%s
  %schb stats%s

Shows total data size, breakdown by month, and breakdown by data type.
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
	)
}

func PrintEventsHelp() {
	f := Fmt
	fmt.Printf(`
%schb events%s — List events from the local data directory

%sUSAGE%s
  %schb events%s [options]

%sOPTIONS%s
  %s-n%s <count>           Number of events to show (default: 10)
  %s--since%s <YYYYMMDD>   Only events starting after this date (default: today)
  %s--skip%s <count>       Skip first N events
  %s--all%s                Show all events (no date filter)
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
	)
}

func PrintEventsSyncHelp() {
	f := Fmt
	fmt.Printf(`
%schb events sync%s — Fetch events from Luma and regenerate data

%sUSAGE%s
  %schb events sync%s [year[/month]] [options]

%sOPTIONS%s
  %s<year>%s               Sync all months of the given year (e.g. 2025)
  %s<year/month>%s         Sync a specific month (e.g. 2025/11)
  %s--since%s <YYYYMMDD>   Start syncing from this date (default: last month)
  %s--force%s              Re-fetch even if cached data exists
  %s--history%s            Rebuild entire event history
  %s--help, -h%s           Show this help

%sSOURCES%s
  • Luma ICS feed (calendar events)
  • Luma API (covers, guests, tags — requires LUMA_API_KEY)
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
  %s--date%s <YYYYMMDD>    Show bookings for a specific date
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

func PrintTransactionsStatsHelp() {
	f := Fmt
	fmt.Printf(`
%schb transactions stats%s — Show transaction statistics

%sUSAGE%s
  %schb transactions stats%s [options]

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

func PrintBookingsSyncHelp() {
	f := Fmt
	fmt.Printf(`
%schb bookings sync%s — Sync room booking calendars from Google Calendar

%sUSAGE%s
  %schb bookings sync%s [year[/month]] [options]

%sOPTIONS%s
  %s<year>%s               Sync all months of the given year (e.g. 2025)
  %s<year/month>%s         Sync a specific month (e.g. 2025/06)
  %s--room%s <slug>        Only sync a specific room (e.g. satoshi)
  %s--force%s              Re-fetch even if cached data exists
  %s--since%s <YYYYMMDD>   Start syncing from this date
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
