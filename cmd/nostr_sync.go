package cmd

import (
	"fmt"
	"strings"
)

// NostrSync is the top-level dispatcher for `chb nostr sync [scope]`.
// For each scope it runs three steps in order: publish local annotations to
// relays, fetch remote annotations back, then apply the priority-merge
// generate step so the public output files reflect the latest state.
//
// Scopes:
//
//	transactions   Stripe + blockchain txs
//	invoices       Odoo customer invoices
//	bills          Odoo vendor bills
//	all            run all three
func NostrSync(args []string) error {
	scope := ""
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		scope = strings.ToLower(args[0])
		args = args[1:]
	}
	if scope == "" && HasFlag(args, "--help", "-h", "help") {
		printNostrSyncHelp()
		return nil
	}
	switch scope {
	case "help", "--help", "-h":
		printNostrSyncHelp()
		return nil
	case "transactions", "tx":
		return nostrSyncTransactions(args)
	case "invoices":
		return nostrSyncMoves(moveKindInvoice, args)
	case "bills":
		return nostrSyncMoves(moveKindBill, args)
	case "", "all":
		if err := nostrSyncTransactions(args); err != nil {
			fmt.Printf("  %s✗ transactions: %v%s\n", Fmt.Red, err, Fmt.Reset)
		}
		if err := nostrSyncMoves(moveKindInvoice, args); err != nil {
			fmt.Printf("  %s✗ invoices: %v%s\n", Fmt.Red, err, Fmt.Reset)
		}
		if err := nostrSyncMoves(moveKindBill, args); err != nil {
			fmt.Printf("  %s✗ bills: %v%s\n", Fmt.Red, err, Fmt.Reset)
		}
		return nil
	default:
		return fmt.Errorf("unknown scope %q — use transactions, invoices, bills, or all", scope)
	}
}

// nostrSyncTransactions: publish unpublished categorizations, fetch remote
// annotations, regenerate the unified transactions.json.
func nostrSyncTransactions(args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		printNostrSyncHelp()
		return nil
	}
	fmt.Printf("\n%s📡 Nostr sync — transactions%s\n", Fmt.Bold, Fmt.Reset)
	if err := flushNostrOutboxWithStatus(); err != nil {
		fmt.Printf("  %s⚠ outbox: %v%s\n", Fmt.Yellow, err, Fmt.Reset)
	}
	if err := TransactionsPublish(args); err != nil {
		return err
	}
	if err := TransactionsSyncNostr(args); err != nil {
		return err
	}
	if err := GenerateTransactions(args); err != nil {
		return fmt.Errorf("generate transactions: %w", err)
	}
	return nil
}

// nostrSyncMoves: publish unpublished invoice/bill annotations, fetch remote
// annotations, then run the priority-merge generate so invoices.json /
// bills.json reflect the latest state.
func nostrSyncMoves(kind moveKind, args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		printNostrSyncHelp()
		return nil
	}
	fmt.Printf("\n%s📡 Nostr sync — %s%s\n", Fmt.Bold, kind.labelPl, Fmt.Reset)
	if err := flushNostrOutboxWithStatus(); err != nil {
		fmt.Printf("  %s⚠ outbox: %v%s\n", Fmt.Yellow, err, Fmt.Reset)
	}
	if err := publishMoves(kind, args); err != nil {
		return err
	}
	if err := syncMovesFromNostr(kind, args); err != nil {
		return err
	}
	return generateMovesWithRules(kind, args)
}

func printNostrSyncHelp() {
	f := Fmt
	fmt.Printf(`
%schb nostr sync%s — Publish + fetch Nostr annotations, then merge into outputs

%sUSAGE%s
  %schb nostr sync%s [scope] [year[/month]]

%sSCOPES%s
  %stransactions%s   Stripe + blockchain txs (alias: tx)
  %sinvoices%s       Odoo customer invoices
  %sbills%s          Odoo vendor bills
  %sall%s            run all three

  With no scope, runs all scopes.

%sDESCRIPTION%s
  For each scope this command runs three steps in order:

    1. Flush any signed events from APP_DATA_DIR/nostr/outbox.
    2. Publish local annotations that don't yet have a Nostr event.
    3. Fetch remote annotations back onto cached records.
    4. Run generate: apply the priority chain (Nostr > Odoo analytic >
       local rules) and rewrite the public files.

  Publishing always asks for confirmation before broadcasting.
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
	)
}
