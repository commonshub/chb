package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/CommonsHub/chb/cmd"
)

// VERSION is injected at release build time via ldflags.
var VERSION string

func main() {
	cmd.Version = cmd.ResolveVersion(VERSION)
	cmd.LoadEnvFromConfig()

	args := os.Args[1:]

	if len(args) == 0 {
		cmd.PrintHelp(cmd.Version)
		return
	}

	if needsWritableDataDir(args) {
		if _, err := cmd.EnsureWritableDataDir(); err != nil {
			fmt.Fprintf(os.Stderr, "%sError:%s %v\n", cmd.Fmt.Red, cmd.Fmt.Reset, err)
			os.Exit(1)
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
				fmt.Fprintf(os.Stderr, "%sError:%s %v\n", cmd.Fmt.Red, cmd.Fmt.Reset, err)
				os.Exit(1)
			}
		} else if len(args) > 1 && args[1] == "odoo" {
			if err := cmd.SetupOdoo(); err != nil {
				fmt.Fprintf(os.Stderr, "%sError:%s %v\n", cmd.Fmt.Red, cmd.Fmt.Reset, err)
				os.Exit(1)
			}
		} else {
			if err := cmd.Setup(); err != nil {
				fmt.Fprintf(os.Stderr, "%sError:%s %v\n", cmd.Fmt.Red, cmd.Fmt.Reset, err)
				os.Exit(1)
			}
		}
	case "update":
		yes := cmd.HasFlag(args[1:], "--yes", "-y")
		if err := cmd.Update(yes); err != nil {
			fmt.Fprintf(os.Stderr, "%sError:%s %v\n", cmd.Fmt.Red, cmd.Fmt.Reset, err)
			os.Exit(1)
		}
	case "events":
		if len(args) > 1 && args[1] == "sync" {
			if err := cmd.EventsSync(args[2:]); err != nil {
				fmt.Fprintf(os.Stderr, "%sError:%s %v\n", cmd.Fmt.Red, cmd.Fmt.Reset, err)
				os.Exit(1)
			}
		} else if len(args) > 1 && args[1] == "stats" {
			cmd.EventsStats(args[2:])
		} else {
			cmd.EventsList(args[1:])
		}
	case "rooms":
		cmd.Rooms(args[1:])
	case "bookings":
		if len(args) > 1 && args[1] == "sync" {
			if err := cmd.BookingsSync(args[2:]); err != nil {
				fmt.Fprintf(os.Stderr, "%sError:%s %v\n", cmd.Fmt.Red, cmd.Fmt.Reset, err)
				os.Exit(1)
			}
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
			case "sync", "categorize", "publish", "stats":
				txSubcmd = strings.ToLower(a)
			}
		}
		// Check for "sync nostr" compound subcommand
		hasSyncNostr := txSubcmd == "sync" && hasArg(txArgs, "nostr")

		switch {
		case hasSyncNostr:
			if err := cmd.TransactionsSyncNostr(txArgs); err != nil {
				fmt.Fprintf(os.Stderr, "%sError:%s %v\n", cmd.Fmt.Red, cmd.Fmt.Reset, err)
				os.Exit(1)
			}
		case txSubcmd == "sync":
			if _, err := cmd.TransactionsSync(txArgs); err != nil {
				fmt.Fprintf(os.Stderr, "%sError:%s %v\n", cmd.Fmt.Red, cmd.Fmt.Reset, err)
				os.Exit(1)
			}
		case txSubcmd == "categorize":
			cmd.TransactionsCategorize(txArgs)
		case txSubcmd == "publish":
			if err := cmd.TransactionsPublish(txArgs); err != nil {
				fmt.Fprintf(os.Stderr, "%sError:%s %v\n", cmd.Fmt.Red, cmd.Fmt.Reset, err)
				os.Exit(1)
			}
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
		case "sync", "help", "--help", "-h":
			if len(args) > 2 && args[2] == "nostr" {
				if err := cmd.InvoicesSyncNostr(args[3:]); err != nil {
					fmt.Fprintf(os.Stderr, "%sError:%s %v\n", cmd.Fmt.Red, cmd.Fmt.Reset, err)
					os.Exit(1)
				}
				return
			}
			if _, err := cmd.InvoicesSync(args[1:]); err != nil {
				fmt.Fprintf(os.Stderr, "%sError:%s %v\n", cmd.Fmt.Red, cmd.Fmt.Reset, err)
				os.Exit(1)
			}
		case "categorize":
			if err := cmd.InvoicesCategorize(args[2:]); err != nil {
				fmt.Fprintf(os.Stderr, "%sError:%s %v\n", cmd.Fmt.Red, cmd.Fmt.Reset, err)
				os.Exit(1)
			}
		case "publish":
			if err := cmd.InvoicesPublish(args[2:]); err != nil {
				fmt.Fprintf(os.Stderr, "%sError:%s %v\n", cmd.Fmt.Red, cmd.Fmt.Reset, err)
				os.Exit(1)
			}
		default:
			fmt.Fprintf(os.Stderr, "%sUsage: chb invoices [sync|categorize|publish] [options]%s\n", cmd.Fmt.Yellow, cmd.Fmt.Reset)
			os.Exit(1)
		}
	case "bills":
		billSub := ""
		if len(args) > 1 {
			billSub = args[1]
		}
		switch billSub {
		case "sync", "help", "--help", "-h":
			if len(args) > 2 && args[2] == "nostr" {
				if err := cmd.BillsSyncNostr(args[3:]); err != nil {
					fmt.Fprintf(os.Stderr, "%sError:%s %v\n", cmd.Fmt.Red, cmd.Fmt.Reset, err)
					os.Exit(1)
				}
				return
			}
			if _, err := cmd.BillsSync(args[1:]); err != nil {
				fmt.Fprintf(os.Stderr, "%sError:%s %v\n", cmd.Fmt.Red, cmd.Fmt.Reset, err)
				os.Exit(1)
			}
		case "categorize":
			if err := cmd.BillsCategorize(args[2:]); err != nil {
				fmt.Fprintf(os.Stderr, "%sError:%s %v\n", cmd.Fmt.Red, cmd.Fmt.Reset, err)
				os.Exit(1)
			}
		case "publish":
			if err := cmd.BillsPublish(args[2:]); err != nil {
				fmt.Fprintf(os.Stderr, "%sError:%s %v\n", cmd.Fmt.Red, cmd.Fmt.Reset, err)
				os.Exit(1)
			}
		default:
			fmt.Fprintf(os.Stderr, "%sUsage: chb bills [sync|categorize|publish] [options]%s\n", cmd.Fmt.Yellow, cmd.Fmt.Reset)
			os.Exit(1)
		}
	case "messages":
		if len(args) > 1 && args[1] == "sync" {
			if _, err := cmd.MessagesSync(args[2:]); err != nil {
				fmt.Fprintf(os.Stderr, "%sError:%s %v\n", cmd.Fmt.Red, cmd.Fmt.Reset, err)
				os.Exit(1)
			}
		} else if len(args) > 1 && args[1] == "stats" {
			cmd.MessagesStats(args[2:])
		} else {
			fmt.Fprintf(os.Stderr, "%sUsage: chb messages [sync|stats]%s\n", cmd.Fmt.Yellow, cmd.Fmt.Reset)
			os.Exit(1)
		}
	case "images":
		if len(args) > 1 && (args[1] == "sync" || args[1] == "help" || args[1] == "--help" || args[1] == "-h") {
			if _, err := cmd.ImagesSync(args[1:]); err != nil {
				fmt.Fprintf(os.Stderr, "%sError:%s %v\n", cmd.Fmt.Red, cmd.Fmt.Reset, err)
				os.Exit(1)
			}
		} else {
			fmt.Fprintf(os.Stderr, "%sUsage: chb images sync [options]%s\n", cmd.Fmt.Yellow, cmd.Fmt.Reset)
			os.Exit(1)
		}
	case "attachments":
		if len(args) > 1 && (args[1] == "sync" || args[1] == "help" || args[1] == "--help" || args[1] == "-h") {
			if _, err := cmd.AttachmentsSync(args[1:]); err != nil {
				fmt.Fprintf(os.Stderr, "%sError:%s %v\n", cmd.Fmt.Red, cmd.Fmt.Reset, err)
				os.Exit(1)
			}
		} else {
			fmt.Fprintf(os.Stderr, "%sUsage: chb attachments sync [options]%s\n", cmd.Fmt.Yellow, cmd.Fmt.Reset)
			os.Exit(1)
		}
	case "generate":
		sub := ""
		rest := args[1:]
		if len(rest) > 0 {
			sub = rest[0]
		}
		var genErr error
		switch sub {
		case "transactions", "tx":
			genErr = cmd.GenerateTransactions(rest[1:])
		case "events":
			genErr = cmd.GenerateEvents(rest[1:])
		case "messages":
			genErr = cmd.GenerateMessages(rest[1:])
		case "members":
			genErr = cmd.GenerateMembers(rest[1:])
		default:
			genErr = cmd.Generate(rest)
		}
		if genErr != nil {
			fmt.Fprintf(os.Stderr, "%sError:%s %v\n", cmd.Fmt.Red, cmd.Fmt.Reset, genErr)
			os.Exit(1)
		}
	case "members":
		if len(args) > 1 && args[1] == "sync" {
			if err := cmd.MembersSync(args[2:]); err != nil {
				fmt.Fprintf(os.Stderr, "%sError:%s %v\n", cmd.Fmt.Red, cmd.Fmt.Reset, err)
				os.Exit(1)
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
		case "sync":
			// Meta-command: run invoices + bills + journals sync in order.
			if err := cmd.OdooSyncAll(args[2:]); err != nil {
				fmt.Fprintf(os.Stderr, "%sError:%s %v\n", cmd.Fmt.Red, cmd.Fmt.Reset, err)
				os.Exit(1)
			}
		case "categories":
			// Back-compat: what `chb odoo sync` used to be.
			catArgs := args[2:]
			if len(catArgs) > 0 && catArgs[0] == "sync" {
				catArgs = catArgs[1:]
			}
			if _, err := cmd.OdooAnalyticSync(catArgs); err != nil {
				fmt.Fprintf(os.Stderr, "%sError:%s %v\n", cmd.Fmt.Red, cmd.Fmt.Reset, err)
				os.Exit(1)
			}
		case "invoices":
			invArgs := args[2:]
			if len(invArgs) > 0 && invArgs[0] == "sync" {
				invArgs = invArgs[1:]
			}
			if _, err := cmd.InvoicesSync(invArgs); err != nil {
				fmt.Fprintf(os.Stderr, "%sError:%s %v\n", cmd.Fmt.Red, cmd.Fmt.Reset, err)
				os.Exit(1)
			}
		case "bills":
			billArgs := args[2:]
			if len(billArgs) > 0 && billArgs[0] == "sync" {
				billArgs = billArgs[1:]
			}
			if _, err := cmd.BillsSync(billArgs); err != nil {
				fmt.Fprintf(os.Stderr, "%sError:%s %v\n", cmd.Fmt.Red, cmd.Fmt.Reset, err)
				os.Exit(1)
			}
		case "journals":
			if err := cmd.OdooJournals(args[2:]); err != nil {
				fmt.Fprintf(os.Stderr, "%sError:%s %v\n", cmd.Fmt.Red, cmd.Fmt.Reset, err)
				os.Exit(1)
			}
		case "backup":
			if err := cmd.OdooBackup(args[2:]); err != nil {
				fmt.Fprintf(os.Stderr, "%sError:%s %v\n", cmd.Fmt.Red, cmd.Fmt.Reset, err)
				os.Exit(1)
			}
		default:
			cmd.PrintOdooHelp()
		}
	case "nostr":
		if len(args) > 1 && args[1] == "sync" {
			if err := cmd.NostrSync(args[2:]); err != nil {
				fmt.Fprintf(os.Stderr, "%sError:%s %v\n", cmd.Fmt.Red, cmd.Fmt.Reset, err)
				os.Exit(1)
			}
		} else {
			fmt.Fprintf(os.Stderr, "%sUsage: chb nostr sync <scope> [options]%s\n", cmd.Fmt.Yellow, cmd.Fmt.Reset)
			os.Exit(1)
		}
	case "rules":
		cmd.RulesCommand(args[1:])
	case "accounts":
		cmd.AccountsCommand(args[1:])
	case "stats":
		cmd.Stats(args[1:])
	case "doctor":
		if err := cmd.Doctor(args[1:]); err != nil {
			fmt.Fprintf(os.Stderr, "%sError:%s %v\n", cmd.Fmt.Red, cmd.Fmt.Reset, err)
			os.Exit(1)
		}
	case "tools":
		if err := cmd.Tools(args[1:]); err != nil {
			fmt.Fprintf(os.Stderr, "%sError:%s %v\n", cmd.Fmt.Red, cmd.Fmt.Reset, err)
			os.Exit(1)
		}
	case "sync":
		if err := cmd.SyncAll(args[1:]); err != nil {
			fmt.Fprintf(os.Stderr, "%sError:%s %v\n", cmd.Fmt.Red, cmd.Fmt.Reset, err)
			os.Exit(1)
		}
	case "report":
		if err := cmd.Report(args[1:]); err != nil {
			fmt.Fprintf(os.Stderr, "%sError:%s %v\n", cmd.Fmt.Red, cmd.Fmt.Reset, err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "%sUnknown command: %s%s\n\n", cmd.Fmt.Red, args[0], cmd.Fmt.Reset)
		cmd.PrintHelp(cmd.Version)
		os.Exit(1)
	}
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
	case "setup", "sync":
		return true
	case "events", "bookings", "invoices", "bills", "messages", "images", "attachments", "members", "odoo":
		return len(args) > 1 && strings.EqualFold(args[1], "sync")
	case "transactions":
		return hasArg(args[1:], "sync")
	default:
		return false
	}
}
