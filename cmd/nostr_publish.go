package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nbd-wtf/go-nostr"
)

func countPendingTransactionAnnotations(args []string) int {
	posYear, posMonth, posFound := ParseYearMonthArg(args)
	dataDir := DataDir()
	now := time.Now()
	var startMonth, endMonth string
	if posFound {
		if posMonth != "" {
			startMonth = fmt.Sprintf("%s-%s", posYear, posMonth)
			endMonth = startMonth
		} else {
			startMonth = fmt.Sprintf("%s-01", posYear)
			endMonth = fmt.Sprintf("%s-12", posYear)
		}
	} else {
		startMonth = fmt.Sprintf("%d-%02d", now.Year(), now.Month())
		endMonth = startMonth
	}

	publishedIDs := loadPublishedEventIDs()
	count := 0
	yearDirs, _ := os.ReadDir(dataDir)
	for _, yd := range yearDirs {
		if !yd.IsDir() || len(yd.Name()) != 4 {
			continue
		}
		monthDirs, _ := os.ReadDir(filepath.Join(dataDir, yd.Name()))
		for _, md := range monthDirs {
			if !md.IsDir() || len(md.Name()) != 2 {
				continue
			}
			ym := yd.Name() + "-" + md.Name()
			if ym < startMonth || ym > endMonth {
				continue
			}
			txPath := filepath.Join(dataDir, yd.Name(), md.Name(), "generated", "transactions.json")
			data, err := os.ReadFile(txPath)
			if err != nil {
				continue
			}
			var txFile TransactionsFile
			if json.Unmarshal(data, &txFile) != nil {
				continue
			}
			for _, tx := range txFile.Transactions {
				if tx.Category == "" || tx.Type == "INTERNAL" {
					continue
				}
				uri := ""
				if tx.Provider == "stripe" {
					uri = BuildStripeURI(tx.StripeChargeID)
				} else if tx.Provider == "etherscan" && tx.TxHash != "" {
					chainID := 0
					if tx.Chain != nil {
						switch *tx.Chain {
						case "gnosis":
							chainID = 100
						case "celo":
							chainID = 42220
						}
					}
					if chainID > 0 {
						uri = BuildBlockchainURI(chainID, tx.TxHash)
					}
				}
				if uri != "" && !publishedIDs[uri] {
					count++
				}
			}
		}
	}
	return count
}

// TransactionsPublish previews and publishes local categorizations to Nostr.
// NEVER publishes without explicit user confirmation.
func TransactionsPublish(args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		printTransactionsPublishHelp()
		return nil
	}

	// Load keys
	keys := LoadNostrKeys()
	if keys == nil {
		return fmt.Errorf("no Nostr identity configured. Run: chb setup nostr")
	}

	relays := keys.Relays
	if len(relays) == 0 {
		relays = nostrRelays
	}

	// Determine month range
	posYear, posMonth, posFound := ParseYearMonthArg(args)
	dataDir := DataDir()

	now := time.Now()
	var startMonth, endMonth string
	if posFound {
		if posMonth != "" {
			startMonth = fmt.Sprintf("%s-%s", posYear, posMonth)
			endMonth = startMonth
		} else {
			startMonth = fmt.Sprintf("%s-01", posYear)
			endMonth = fmt.Sprintf("%s-12", posYear)
		}
	} else {
		startMonth = fmt.Sprintf("%d-%02d", now.Year(), now.Month())
		endMonth = startMonth
	}

	// Load already-published event IDs
	publishedIDs := loadPublishedEventIDs()

	// Scan transactions and collect unpublished categorizations
	type pendingEvent struct {
		URI        string
		Category   string
		Collective string
		Event      string
		Amount     string
		Currency   string
		Provider   string
	}
	var pending []pendingEvent

	yearDirs, _ := os.ReadDir(dataDir)
	for _, yd := range yearDirs {
		if !yd.IsDir() || len(yd.Name()) != 4 {
			continue
		}
		monthDirs, _ := os.ReadDir(filepath.Join(dataDir, yd.Name()))
		for _, md := range monthDirs {
			if !md.IsDir() || len(md.Name()) != 2 {
				continue
			}
			ym := yd.Name() + "-" + md.Name()
			if ym < startMonth || ym > endMonth {
				continue
			}

			txPath := filepath.Join(dataDir, yd.Name(), md.Name(), "generated", "transactions.json")
			data, err := os.ReadFile(txPath)
			if err != nil {
				continue
			}
			var txFile TransactionsFile
			if json.Unmarshal(data, &txFile) != nil {
				continue
			}

			for _, tx := range txFile.Transactions {
				if tx.Category == "" {
					continue // nothing to publish
				}
				if tx.Type == "INTERNAL" {
					continue
				}

				// Build URI
				var uri string
				if tx.Provider == "stripe" {
					uri = BuildStripeURI(tx.StripeChargeID)
				} else if tx.Provider == "etherscan" && tx.TxHash != "" {
					chainID := 0
					if tx.Chain != nil {
						switch *tx.Chain {
						case "gnosis":
							chainID = 100
						case "celo":
							chainID = 42220
						}
					}
					if chainID > 0 {
						uri = BuildBlockchainURI(chainID, tx.TxHash)
					}
				}
				if uri == "" {
					continue
				}

				// Skip if already published
				if publishedIDs[uri] {
					continue
				}

				amount := fmt.Sprintf("%.2f", tx.Amount)
				if tx.NormalizedAmount != 0 {
					amount = fmt.Sprintf("%.2f", tx.NormalizedAmount)
				}

				pending = append(pending, pendingEvent{
					URI:        uri,
					Category:   tx.Category,
					Collective: tx.Collective,
					Event:      tx.Event,
					Amount:     amount,
					Currency:   tx.Currency,
					Provider:   tx.Provider,
				})
			}
		}
	}

	verbose := HasFlag(args, "--verbose", "-v") || HasFlag(args, "--debug")
	aggregated := quietOdooContext() // running as part of `chb push` / `chb sync`

	if len(pending) == 0 {
		if !aggregated {
			fmt.Printf("\n%s✓ Nothing to publish%s — all categorized transactions already have Nostr annotations.\n\n", Fmt.Green, Fmt.Reset)
		}
		return nil
	}

	// Compact header — only one line by default. Verbose adds the relay
	// list and per-event preview table.
	if !aggregated {
		fmt.Printf("\n%s📡 Publishing %d transaction annotation%s to %d relay%s%s\n",
			Fmt.Bold, len(pending), plural(len(pending)),
			len(relays), plural(len(relays)), Fmt.Reset)
	}
	if verbose && !aggregated {
		for _, r := range relays {
			fmt.Printf("    %s%s%s\n", Fmt.Dim, r, Fmt.Reset)
		}
		fmt.Printf("\n  %s%-40s %-14s %-14s %s%s\n",
			Fmt.Dim, "URI", "Category", "Collective", "Amount", Fmt.Reset)
		fmt.Printf("  %s%s%s\n", Fmt.Dim, strings.Repeat("─", 90), Fmt.Reset)
		for _, p := range pending {
			uri := p.URI
			if len(uri) > 38 {
				uri = uri[:35] + "..."
			}
			collective := p.Collective
			if collective == "" {
				collective = "-"
			}
			currency := p.Currency
			if currency == "" {
				currency = "EUR"
			}
			fmt.Printf("  %-40s %-14s %-14s %s %s\n",
				uri, p.Category, collective, p.Amount, currency)
		}
		fmt.Println()
	}

	// Non-interactive: the operator already opted in by typing
	// `chb nostr push` / `chb push` / `chb sync`. Cron-friendly. Use
	// --dry-run earlier in the args list (handled by the caller) to
	// preview without writing.
	if HasFlag(args, "--dry-run") {
		fmt.Printf("%s(dry-run — no writes)%s\n\n", Fmt.Dim, Fmt.Reset)
		return nil
	}

	// Publish — live status so the operator can see progress instead
	// of staring at a frozen terminal. Each event takes ≤ relayPublishTimeout
	// across all relays.
	status := newStatusLine()
	defer status.Clear()
	published := 0
	failed := 0
	for i, p := range pending {
		status.Update("Publishing Nostr annotations %d/%d (%d ok, %d failed)", i+1, len(pending), published, failed)
		// Build kind 1111 event
		tags := nostr.Tags{
			{"I", p.URI},
			{"K", uriKind(p.URI)},
			{"i", p.URI},
			{"k", uriKind(p.URI)},
			{"category", p.Category},
		}
		if p.Collective != "" {
			tags = append(tags, nostr.Tag{"collective", p.Collective})
		}
		if p.Event != "" {
			tags = append(tags, nostr.Tag{"event", p.Event})
		}
		if p.Amount != "" && p.Currency != "" {
			tags = append(tags, nostr.Tag{"amount", p.Amount, p.Currency})
		}

		ev := &nostr.Event{
			Kind:    1111,
			Tags:    tags,
			Content: "",
		}

		accepted, err := publishNostrEventWithOutbox(keys, p.URI, ev)
		if err != nil {
			failed++
			if verbose {
				fmt.Printf("  %s✗ %s%s\n", Fmt.Red, p.URI, Fmt.Reset)
			}
		} else {
			published++
			_ = accepted
		}
		// 100 ms politeness sleep, but only every 20 events — the
		// individual relay publishes already have their own timeouts
		// + serialization, so we don't need to throttle every event.
		if (i+1)%20 == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}
	status.Clear()

	if !aggregated {
		fmt.Printf("\n%s✓ Published %d annotations%s", Fmt.Green, published, Fmt.Reset)
		if failed > 0 {
			fmt.Printf(" (%s%d failed%s)", Fmt.Red, failed, Fmt.Reset)
		}
		fmt.Println()
		fmt.Println()
	}

	return nil
}

// uriKind returns the "K" tag value for a URI.
func uriKind(uri string) string {
	if strings.HasPrefix(uri, "stripe:") {
		// Stripe IDs carry the type as a prefix (txn_, cus_, acct_, ch_, …),
		// so we promote that prefix to the K tag (e.g. stripe:txn_123 → stripe:txn).
		rest := strings.TrimPrefix(uri, "stripe:")
		if idx := strings.Index(rest, "_"); idx > 0 {
			return "stripe:" + rest[:idx]
		}
		return "stripe"
	}
	if strings.HasPrefix(uri, "ethereum:") {
		parts := strings.SplitN(uri, ":", 4)
		if len(parts) >= 3 {
			return "ethereum:" + parts[2]
		}
	}
	if strings.HasPrefix(uri, "odoo:") {
		// odoo:<host>:<db>:<model>:<id> → odoo:<model>
		parts := strings.Split(uri, ":")
		if len(parts) >= 5 {
			return "odoo:" + parts[3]
		}
		return "odoo"
	}
	return "unknown"
}

// loadPublishedEventIDs reads the durable Nostr queue and returns URIs that
// are already sent or queued for retry.
func loadPublishedEventIDs() map[string]bool {
	ids := map[string]bool{}
	for uri := range loadQueuedNostrEventURIs() {
		ids[uri] = true
	}
	for uri := range loadSentNostrEventURIs() {
		ids[uri] = true
	}
	return ids
}

func printTransactionsPublishHelp() {
	f := Fmt
	fmt.Printf(`
%schb nostr sync transactions%s — Publish/fetch transaction annotations

%sUSAGE%s
  %schb nostr sync transactions%s [year[/month]]

%sDESCRIPTION%s
  Flushes the durable Nostr outbox, publishes local transaction annotations,
  fetches remote annotations back, then regenerates transactions.

%sOPTIONS%s
  %s<year>%s              Publish for a specific year
  %s<year/month>%s        Publish for a specific month
  %s--help, -h%s          Show this help

%sEXAMPLES%s
  %schb nostr sync transactions%s              Current month
  %schb nostr sync transactions 2026/04%s      April 2026
  %schb nostr sync transactions 2026%s         All of 2026
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
	)
}
