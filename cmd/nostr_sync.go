package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// NostrPull fetches annotations from Nostr relays into the local cache.
// No network writes — pure read. Use `chb nostr push` to publish.
func NostrPull(args []string) error {
	scope, args := nostrParseScope(args)
	if scope == "help" || HasFlag(args, "--help", "-h", "help") {
		printNostrPullHelp()
		return nil
	}
	switch scope {
	case "", "all", "transactions", "tx", "invoices", "bills", "expenses":
		// One read-only pull covers every record type: transactions and
		// Odoo documents are matched by URI (cmd/nostr_annotations_pull.go).
		summary, err := pullNostrAnnotations(DataDir(), HasFlag(args, "--force"))
		if err != nil {
			return err
		}
		fmt.Printf("  ✓ Nostr: %s\n", summary)
		return nil
	default:
		return fmt.Errorf("unknown scope %q — use transactions, invoices, bills, or all", scope)
	}
}

// NostrPush publishes queued + freshly-categorized annotations to Nostr
// relays. Reads the outbox (signed but unsent events) and any local
// categorizations that don't yet have a Nostr event.
func NostrPush(args []string) error {
	scope, args := nostrParseScope(args)
	if scope == "help" || HasFlag(args, "--help", "-h", "help") {
		printNostrPushHelp()
		return nil
	}
	if err := flushNostrOutboxWithStatus(); err != nil {
		fmt.Printf("  %s⚠ outbox: %v%s\n", Fmt.Yellow, err, Fmt.Reset)
	}
	switch scope {
	case "transactions", "tx":
		return TransactionsPublish(args)
	case "invoices":
		return publishMoves(moveKindInvoice, args)
	case "bills":
		return publishMoves(moveKindBill, args)
	case "", "all":
		if err := TransactionsPublish(args); err != nil {
			fmt.Printf("  %s✗ transactions: %v%s\n", Fmt.Red, err, Fmt.Reset)
		}
		if err := publishMoves(moveKindInvoice, args); err != nil {
			fmt.Printf("  %s✗ invoices: %v%s\n", Fmt.Red, err, Fmt.Reset)
		}
		if err := publishMoves(moveKindBill, args); err != nil {
			fmt.Printf("  %s✗ bills: %v%s\n", Fmt.Red, err, Fmt.Reset)
		}
		return nil
	default:
		return fmt.Errorf("unknown scope %q — use transactions, invoices, bills, or all", scope)
	}
}

// NostrPending lists events currently sitting in the local outbox waiting
// to be pushed. The outbox is Nostr's pending-changes file equivalent of
// providers/odoo/pending/.
func NostrPending(args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		fmt.Printf("\n%schb nostr pending%s — List queued Nostr events not yet published\n\n", Fmt.Bold, Fmt.Reset)
		fmt.Printf("  Push them with %schb nostr push%s.\n\n", Fmt.Cyan, Fmt.Reset)
		return nil
	}
	entries, err := os.ReadDir(nostrOutboxDir())
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	var queued []queuedNostrEvent
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		item, err := readQueuedNostrEvent(filepath.Join(nostrOutboxDir(), e.Name()))
		if err == nil {
			queued = append(queued, item)
		}
	}
	if len(queued) == 0 {
		fmt.Printf("\n%sOutbox is empty — nothing pending to push.%s\n\n", Fmt.Dim, Fmt.Reset)
		return nil
	}
	fmt.Printf("\n%s📬 Nostr outbox%s  %s(%s)%s\n\n",
		Fmt.Bold, Fmt.Reset, Fmt.Dim, nostrOutboxDir(), Fmt.Reset)
	for _, q := range queued {
		uri := q.URI
		if uri == "" {
			uri = q.Event.ID
		}
		fmt.Printf("  %s%s%s\n", Fmt.Cyan, uri, Fmt.Reset)
		fmt.Printf("    %squeued %s, kind %d, %d tags%s\n",
			Fmt.Dim, q.QueuedAt, q.Event.Kind, len(q.Event.Tags), Fmt.Reset)
	}
	fmt.Printf("\n  %sPush them with: chb nostr push%s\n\n", Fmt.Dim, Fmt.Reset)
	return nil
}

// nostrParseScope pulls an optional positional scope arg off the front of
// the slice so callers don't have to duplicate this logic.
func nostrParseScope(args []string) (string, []string) {
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		return strings.ToLower(args[0]), args[1:]
	}
	return "", args
}

// NostrSync is the deprecated combined pull+push entry point retained for
// backward compatibility. New callers should use `chb nostr pull` /
// `chb nostr push` explicitly.
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

func printNostrPullHelp() {
	f := Fmt
	fmt.Printf(`
%schb nostr pull%s — Fetch annotations from Nostr relays into the local cache

%sUSAGE%s
  %schb nostr pull%s [scope] [year[/month]]

%sSCOPES%s
  %stransactions%s   Stripe + blockchain txs (alias: tx)
  %sinvoices%s       Odoo customer invoices
  %sbills%s          Odoo vendor bills
  %sall%s            all of the above (default)

Pure read — no network writes. Use %schb nostr push%s to publish.
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
	)
}

func printNostrPushHelp() {
	f := Fmt
	fmt.Printf(`
%schb nostr push%s — Publish queued + freshly-categorized annotations to Nostr relays

%sUSAGE%s
  %schb nostr push%s [scope] [year[/month]]

%sSCOPES%s
  %stransactions%s   Stripe + blockchain txs (alias: tx)
  %sinvoices%s       Odoo customer invoices
  %sbills%s          Odoo vendor bills
  %sall%s            all of the above (default)

Reads %sAPP_DATA_DIR/nostr/outbox/%s for signed-but-unsent events, then
publishes any local categorisations without a Nostr event yet.

%sList what would be pushed first:%s %schb nostr pending%s
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Dim, f.Reset,
		f.Dim, f.Reset,
		f.Cyan, f.Reset,
	)
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
