package main

import (
	"fmt"
	"os"

	"github.com/CommonsHub/chb/cmd"
)

const VERSION = "2.1.3"

func main() {
	cmd.LoadEnvFromConfig()

	args := os.Args[1:]

	if len(args) == 0 {
		cmd.PrintHelp(VERSION)
		return
	}

	switch args[0] {
	case "--help", "-h", "help":
		cmd.PrintHelp(VERSION)
	case "--version", "-v", "version":
		cmd.CheckLatestVersion(VERSION)
	case "setup":
		if err := cmd.Setup(); err != nil {
			fmt.Fprintf(os.Stderr, "%sError:%s %v\n", cmd.Fmt.Red, cmd.Fmt.Reset, err)
			os.Exit(1)
		}
	case "update":
		if err := cmd.Update(); err != nil {
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
		if len(args) > 1 && args[1] == "sync" {
			if err := cmd.TransactionsSync(args[2:]); err != nil {
				fmt.Fprintf(os.Stderr, "%sError:%s %v\n", cmd.Fmt.Red, cmd.Fmt.Reset, err)
				os.Exit(1)
			}
		} else {
			cmd.TransactionsStats(args[1:])
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
	case "images":
		if len(args) > 1 && (args[1] == "sync" || args[1] == "help" || args[1] == "--help" || args[1] == "-h") {
			if err := cmd.ImagesSync(args[1:]); err != nil {
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
