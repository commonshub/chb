package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	nostrsource "github.com/CommonsHub/chb/sources/nostr"
	"github.com/charmbracelet/huh"
	"github.com/nbd-wtf/go-nostr"
)

// TransactionsSyncNostr pulls annotations from Nostr relays and pushes
// locally categorized transactions that haven't been published yet.
func TransactionsSyncNostr(args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		printTransactionsSyncNostrHelp()
		return nil
	}

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

	fmt.Printf("\n%s📡 Syncing transactions with Nostr%s\n", Fmt.Bold, Fmt.Reset)
	fmt.Printf("%sRelays:%s\n", Fmt.Dim, Fmt.Reset)
	for _, r := range relays {
		fmt.Printf("  %s%s%s\n", Fmt.Dim, r, Fmt.Reset)
	}
	fmt.Printf("%sMonth range: %s → %s%s\n\n", Fmt.Dim, startMonth, endMonth, Fmt.Reset)

	// Collect all generated transactions for the period
	type txInfo struct {
		Entry TransactionEntry
		URI   string
		YM    string // "YYYY-MM"
	}
	var allTxs []txInfo

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
				uri := txURI(tx)
				if uri == "" {
					continue
				}
				allTxs = append(allTxs, txInfo{Entry: tx, URI: uri, YM: ym})
			}
		}
	}

	if len(allTxs) == 0 {
		fmt.Printf("%sNo transactions found for this period.%s\n\n", Fmt.Dim, Fmt.Reset)
		return nil
	}

	// ── PULL: fetch annotations from Nostr ─────────────────────────────────
	fmt.Printf("%s↓ Pulling annotations from Nostr...%s", Fmt.Bold, Fmt.Reset)

	uris := make([]string, len(allTxs))
	for i, t := range allTxs {
		uris[i] = t.URI
	}

	annotations, err := FetchNostrAnnotations(uris, nil)
	if err != nil {
		fmt.Printf(" %s✗ %v%s\n", Fmt.Red, err, Fmt.Reset)
	} else {
		fmt.Printf(" %s✓ %d annotations found%s\n", Fmt.Green, len(annotations), Fmt.Reset)
	}

	// Save pulled annotations per source per month
	if len(annotations) > 0 {
		// Group annotations by month. Keep the legacy Stripe-specific file,
		// and also write the source-wide annotations file used by generate.
		annotationsByMonth := map[string]map[string]*TxAnnotation{}
		stripeByMonth := map[string]map[string]*TxAnnotation{}
		for _, t := range allTxs {
			ann, ok := annotations[t.URI]
			if !ok {
				continue
			}
			if annotationsByMonth[t.YM] == nil {
				annotationsByMonth[t.YM] = map[string]*TxAnnotation{}
			}
			annotationsByMonth[t.YM][t.URI] = ann
			if t.Entry.Provider != "stripe" {
				continue
			}
			if stripeByMonth[t.YM] == nil {
				stripeByMonth[t.YM] = map[string]*TxAnnotation{}
			}
			stripeByMonth[t.YM][t.URI] = ann
		}
		for ym, monthAnns := range annotationsByMonth {
			parts := strings.Split(ym, "-")
			if len(parts) != 2 {
				continue
			}
			cache := NostrAnnotationCache{
				FetchedAt:   time.Now().UTC().Format(time.RFC3339),
				Annotations: monthAnns,
			}
			_ = nostrsource.WriteJSON(dataDir, parts[0], parts[1], cache, nostrsource.AnnotationsFile)
		}
		for ym, monthAnns := range stripeByMonth {
			parts := strings.Split(ym, "-")
			if len(parts) != 2 {
				continue
			}
			cache := NostrAnnotationCache{
				FetchedAt:   time.Now().UTC().Format(time.RFC3339),
				Annotations: monthAnns,
			}
			_ = nostrsource.WriteJSON(dataDir, parts[0], parts[1], cache, nostrsource.StripeAnnotationsFile)
		}
		fmt.Printf("  %sSaved annotations to disk%s\n", Fmt.Dim, Fmt.Reset)
	}

	// Show what Nostr knows that we don't have locally
	newFromNostr := 0
	for _, t := range allTxs {
		ann, ok := annotations[t.URI]
		if !ok {
			continue
		}
		if t.Entry.Category == "" && ann.Category != "" {
			newFromNostr++
		}
	}
	if newFromNostr > 0 {
		fmt.Printf("  %s%d transactions got new categories from Nostr%s\n", Fmt.Green, newFromNostr, Fmt.Reset)
		fmt.Printf("  %sRun %schb generate%s to apply them.%s\n", Fmt.Dim, Fmt.Cyan, Fmt.Reset+Fmt.Dim, Fmt.Reset)
	}

	// ── PUSH: find locally categorized txs not yet on Nostr ────────────────
	fmt.Printf("\n%s↑ Checking local categorizations to push...%s\n", Fmt.Bold, Fmt.Reset)

	publishedIDs := loadPublishedEventIDs()

	type pendingEvent struct {
		URI        string
		Category   string
		Collective string
		Event      string
		Amount     string
		Currency   string
	}
	var pending []pendingEvent

	for _, t := range allTxs {
		tx := t.Entry
		if tx.Category == "" {
			continue
		}
		if tx.Type == "INTERNAL" || tx.Type == "TRANSFER" {
			continue
		}
		if publishedIDs[t.URI] {
			continue
		}
		// Skip if Nostr already has the same annotation
		if ann, ok := annotations[t.URI]; ok && ann.Category == tx.Category {
			continue
		}

		amount := fmt.Sprintf("%.2f", tx.Amount)
		if tx.NormalizedAmount != 0 {
			amount = fmt.Sprintf("%.2f", tx.NormalizedAmount)
		}
		pending = append(pending, pendingEvent{
			URI:        t.URI,
			Category:   tx.Category,
			Collective: tx.Collective,
			Event:      tx.Event,
			Amount:     amount,
			Currency:   tx.Currency,
		})
	}

	if len(pending) == 0 {
		fmt.Printf("  %s✓ Nothing to publish — all local categorizations are already on Nostr.%s\n\n", Fmt.Green, Fmt.Reset)
		return nil
	}

	fmt.Printf("  %s%d annotations to publish%s\n\n", Fmt.Yellow, len(pending), Fmt.Reset)

	// Preview
	preview := HasFlag(args, "--preview", "-p")
	if !preview {
		var showPreview bool
		runField(huh.NewConfirm().
			Title("Show preview of events to publish?").
			Value(&showPreview))
		preview = showPreview
	}

	if preview {
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

// txURI builds the Nostr URI for a transaction entry.
func txURI(tx TransactionEntry) string {
	if tx.Provider == "stripe" && tx.StripeChargeID != "" {
		return BuildStripeURI(tx.StripeChargeID)
	}
	if tx.Provider == "etherscan" && tx.TxHash != "" {
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
			return BuildBlockchainURI(chainID, tx.TxHash)
		}
	}
	return ""
}

func printTransactionsSyncNostrHelp() {
	f := Fmt
	fmt.Printf(`
%schb nostr sync transactions%s — Pull & push transaction annotations via Nostr

%sUSAGE%s
  %schb nostr sync transactions%s [year[/month]] [options]

%sDESCRIPTION%s
  Two-way sync of transaction categorizations with Nostr relays:

  %s↓ Pull:%s  Fetches annotations from relays and saves them locally.
            Run %schb generate%s afterwards to apply them.
  %s↑ Push:%s  Finds locally categorized transactions not yet published,
            shows a preview, and publishes after confirmation.

%sOPTIONS%s
  %s<year>%s              Sync for a specific year
  %s<year/month>%s        Sync for a specific month
  %s--preview, -p%s       Show event preview without prompting
  %s--help, -h%s          Show this help

%sEXAMPLES%s
  %schb nostr sync transactions%s              Current month
  %schb nostr sync transactions 2026/04%s      April 2026
  %schb nostr sync transactions 2026%s         All of 2026
  %schb nostr sync transactions --preview%s    Auto-show preview
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Green, f.Reset,
		f.Cyan, f.Reset,
		f.Green, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
	)
}
