package main

import (
	"fmt"
	"os"

	"github.com/commonshub/commonshub.brussels/cli/cmd"
)

const VERSION = "2.0.0"

func main() {
	args := os.Args[1:]

	if len(args) == 0 {
		cmd.PrintHelp(VERSION)
		return
	}

	switch args[0] {
	case "--help", "-h", "help":
		cmd.PrintHelp(VERSION)
	case "--version", "-v":
		fmt.Printf("chb v%s\n", VERSION)
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
		if len(args) > 1 && args[1] == "sync" {
			if err := cmd.TransactionsSync(args[2:]); err != nil {
				fmt.Fprintf(os.Stderr, "%sError:%s %v\n", cmd.Fmt.Red, cmd.Fmt.Reset, err)
				os.Exit(1)
			}
		} else if len(args) > 1 && args[1] == "stats" {
			cmd.TransactionsStats(args[2:])
		} else {
			fmt.Fprintf(os.Stderr, "%sUsage: chb transactions [sync|stats]%s\n", cmd.Fmt.Yellow, cmd.Fmt.Reset)
			os.Exit(1)
		}
	case "messages":
		if len(args) > 1 && args[1] == "sync" {
			if err := cmd.MessagesSync(args[2:]); err != nil {
				fmt.Fprintf(os.Stderr, "%sError:%s %v\n", cmd.Fmt.Red, cmd.Fmt.Reset, err)
				os.Exit(1)
			}
		} else if len(args) > 1 && args[1] == "stats" {
			cmd.MessagesStats(args[2:])
		} else {
			fmt.Fprintf(os.Stderr, "%sUsage: chb messages [sync|stats]%s\n", cmd.Fmt.Yellow, cmd.Fmt.Reset)
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
			fmt.Fprintf(os.Stderr, "%sUsage: chb members sync [options]%s\n", cmd.Fmt.Yellow, cmd.Fmt.Reset)
			os.Exit(1)
		}
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
