package cmd

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/huh"
	"github.com/nbd-wtf/go-nostr"
)

// InvoicesPublish publishes local invoice annotations to Nostr as NIP-73
// kind-1111 events. Kept as an internal step behind `chb nostr sync`.
func InvoicesPublish(args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		printMovesPublishHelp("invoices")
		return nil
	}
	return publishMoves(moveKindInvoice, args)
}

// BillsPublish is the vendor-bill equivalent.
func BillsPublish(args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		printMovesPublishHelp("bills")
		return nil
	}
	return publishMoves(moveKindBill, args)
}

func publishMoves(kind moveKind, args []string) error {
	keys := LoadNostrKeys()
	if keys == nil {
		return fmt.Errorf("no Nostr identity configured. Run: chb setup nostr")
	}
	relays := keys.Relays
	if len(relays) == 0 {
		relays = nostrRelays
	}

	creds, err := ResolveOdooCredentials()
	if err != nil {
		return fmt.Errorf("%w (needed to build odoo: URIs)", err)
	}
	host := OdooHost(creds.URL)
	db := creds.DB

	posYear, posMonth, posFound := ParseYearMonthArg(args)
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

	type pending struct {
		URI        string
		Category   string
		Collective string
		Event      string
		Amount     string
		Currency   string
		Label      string
	}
	var plan []pending

	dataDir := DataDir()
	err = walkMoveMonths(dataDir, kind, func(year, month string) error {
		ym := year + "-" + month
		if ym < startMonth || ym > endMonth {
			return nil
		}
		moves, err := loadMoves(dataDir, year, month, kind)
		if err != nil {
			return nil
		}
		for _, m := range moves {
			if m.Category == "" && m.Collective == "" && m.Event == "" {
				continue // nothing to publish
			}
			uri := OdooURI(host, db, kind.model, m.ID)
			if publishedIDs[uri] {
				continue
			}
			plan = append(plan, pending{
				URI:        uri,
				Category:   m.Category,
				Collective: m.Collective,
				Event:      m.Event,
				Amount:     fmt.Sprintf("%.2f", m.TotalAmount),
				Currency:   strings.ToUpper(firstNonEmptyStr(m.Currency, "EUR")),
				Label:      moveDisplayLabel(m),
			})
		}
		return nil
	})
	if err != nil {
		return err
	}
	if len(plan) == 0 {
		fmt.Printf("\n%s✓ Nothing to publish%s — all annotated %s already have Nostr annotations.\n\n",
			Fmt.Green, Fmt.Reset, kind.labelPl)
		return nil
	}

	fmt.Printf("\n%s📡 %s annotations to publish to:%s\n",
		Fmt.Bold, strings.Title(kind.labelPl), Fmt.Reset)
	for _, r := range relays {
		fmt.Printf("    %s%s%s\n", Fmt.Dim, r, Fmt.Reset)
	}
	fmt.Println()
	for _, p := range plan {
		collective := firstNonEmptyStr(p.Collective, "-")
		event := firstNonEmptyStr(p.Event, "-")
		fmt.Printf("  %s%-50s%s  cat=%-12s col=%-14s evt=%s\n",
			Fmt.Dim, truncate(p.Label, 50), Fmt.Reset,
			firstNonEmptyStr(p.Category, "-"), collective, event)
	}
	fmt.Println()

	var confirm bool
	runField(huh.NewConfirm().
		Title(fmt.Sprintf("Publish %d %s annotations to Nostr?", len(plan), kind.label)).
		Value(&confirm))
	if !confirm {
		fmt.Printf("\n%sCancelled.%s\n\n", Fmt.Dim, Fmt.Reset)
		return nil
	}

	published, failed := 0, 0
	for i, p := range plan {
		tags := nostr.Tags{
			{"I", p.URI},
			{"K", uriKind(p.URI)},
			{"i", p.URI},
			{"k", uriKind(p.URI)},
		}
		if p.Category != "" {
			tags = append(tags, nostr.Tag{"category", p.Category})
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
		ev := &nostr.Event{Kind: 1111, Tags: tags}

		accepted, err := publishNostrEventWithOutbox(keys, p.URI, ev)
		if err != nil {
			failed++
			fmt.Printf("  %s✗ %s%s\n", Fmt.Red, p.URI, Fmt.Reset)
		} else {
			published++
			_ = accepted
		}
		if (i+1)%10 == 0 {
			fmt.Printf("  %s... %d/%d%s\n", Fmt.Dim, i+1, len(plan), Fmt.Reset)
		}
		time.Sleep(100 * time.Millisecond)
	}
	fmt.Printf("\n%s✓ Published %d %s annotations%s", Fmt.Green, published, kind.label, Fmt.Reset)
	if failed > 0 {
		fmt.Printf(" (%s%d failed%s)", Fmt.Red, failed, Fmt.Reset)
	}
	fmt.Println()
	fmt.Println()
	return nil
}

// truncate returns s shortened to max characters, appending "..." if cut.
func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	if max <= 3 {
		return s[:max]
	}
	return s[:max-3] + "..."
}

func printMovesPublishHelp(label string) {
	f := Fmt
	fmt.Printf(`
%schb %s publish%s — Publish local annotations to Nostr

%sUSAGE%s
  %schb %s publish%s [year[/month]]

%sDESCRIPTION%s
  Publishes every %s that has a category, collective, or event set but
  doesn't yet have a Nostr annotation. Uses NIP-73 URIs of the form:

      odoo:<host>:<db>:account.move:<id>

  Events are kind 1111 (NIP-22 comments) with category / collective /
  event / amount tags. Requires a Nostr identity — run
  %schb setup nostr%s first.
`,
		f.Bold, label, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, label, f.Reset,
		f.Bold, f.Reset,
		label,
		f.Cyan, f.Reset,
	)
}
