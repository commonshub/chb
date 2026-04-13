package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/CommonsHub/chb/cmd"
)

const VERSION = "2.1.6"

func main() {
	cmd.LoadEnvFromConfig()

	args := os.Args[1:]

	if len(args) == 0 {
		cmd.PrintHelp(VERSION)
		return
	}

	// Set version for cmd package
	cmd.Version = VERSION

	switch args[0] {
	case "--help", "-h", "help":
		cmd.PrintHelp(VERSION)
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
			if err := cmd.EventsSync(args[2:], VERSION); err != nil {
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
	case "generate":
		if err := cmd.Generate(args[1:]); err != nil {
			fmt.Fprintf(os.Stderr, "%sError:%s %v\n", cmd.Fmt.Red, cmd.Fmt.Reset, err)
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
			if _, err := cmd.OdooAnalyticSync(args[2:]); err != nil {
				fmt.Fprintf(os.Stderr, "%sError:%s %v\n", cmd.Fmt.Red, cmd.Fmt.Reset, err)
				os.Exit(1)
			}
		case "journals":
			if err := cmd.OdooJournals(args[2:]); err != nil {
				fmt.Fprintf(os.Stderr, "%sError:%s %v\n", cmd.Fmt.Red, cmd.Fmt.Reset, err)
				os.Exit(1)
			}
		default:
			cmd.PrintOdooHelp()
		}
	case "rules":
		cmd.RulesCommand(args[1:])
	case "accounts":
		cmd.AccountsCommand(args[1:])
	case "stats":
		cmd.Stats(args[1:])
	case "sync":
		if err := cmd.SyncAll(args[1:], VERSION); err != nil {
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
		cmd.PrintHelp(VERSION)
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
