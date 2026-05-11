package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/charmbracelet/huh"
	"github.com/nbd-wtf/go-nostr"
)

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
				if tx.Type == "INTERNAL" || tx.Type == "TRANSFER" {
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

	if len(pending) == 0 {
		fmt.Printf("\n%s✓ Nothing to publish%s — all categorized transactions already have Nostr annotations.\n\n", Fmt.Green, Fmt.Reset)
		return nil
	}

	// Preview
	fmt.Printf("\n%s📡 Transactions to publish to:%s\n", Fmt.Bold, Fmt.Reset)
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

	// Confirmation
	var confirm bool
	runField(huh.NewConfirm().
		Title(fmt.Sprintf("Publish %d annotations to Nostr?", len(pending))).
		Value(&confirm))

	if !confirm {
		fmt.Printf("\n%sCancelled.%s\n\n", Fmt.Dim, Fmt.Reset)
		return nil
	}

	// Publish
	published := 0
	failed := 0
	for i, p := range pending {
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
			fmt.Printf("  %s✗ %s%s\n", Fmt.Red, p.URI, Fmt.Reset)
		} else {
			published++
			_ = accepted
		}

		if (i+1)%10 == 0 {
			fmt.Printf("  %s... %d/%d%s\n", Fmt.Dim, i+1, len(pending), Fmt.Reset)
		}

		// Small delay between publishes
		time.Sleep(100 * time.Millisecond)
	}

	fmt.Printf("\n%s✓ Published %d annotations%s", Fmt.Green, published, Fmt.Reset)
	if failed > 0 {
		fmt.Printf(" (%s%d failed%s)", Fmt.Red, failed, Fmt.Reset)
	}
	fmt.Println()
	fmt.Println()

	return nil
}

// uriKind returns the "K" tag value for a URI.
func uriKind(uri string) string {
	if strings.HasPrefix(uri, "stripe:txn:") {
		return "stripe:txn"
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
