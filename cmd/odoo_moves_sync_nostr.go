package cmd

import (
	"fmt"
	"time"
)

// InvoicesSyncNostr fetches Nostr annotations for every cached invoice and
// merges category / collective / event onto the local records. Safe to run
// repeatedly — existing local annotations are preserved unless the Nostr
// event is newer.
func InvoicesSyncNostr(args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		printMovesSyncNostrHelp("invoices")
		return nil
	}
	return syncMovesFromNostr(moveKindInvoice, args)
}

// BillsSyncNostr is the vendor-bill equivalent.
func BillsSyncNostr(args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		printMovesSyncNostrHelp("bills")
		return nil
	}
	return syncMovesFromNostr(moveKindBill, args)
}

func syncMovesFromNostr(kind moveKind, args []string) error {
	creds, err := ResolveOdooCredentials()
	if err != nil {
		return fmt.Errorf("%w (needed to build odoo: URIs)", err)
	}
	host := OdooHost(creds.URL)
	db := creds.DB

	dataDir := DataDir()

	// First pass: collect all URIs across all months, keep a map of month →
	// move slice (by index) so we can write back.
	type entry struct {
		year, month string
		idx         int
	}
	uriToEntries := map[string][]entry{}
	moveCache := map[string][]OdooOutgoingInvoicePublic{}

	err = walkMoveMonths(dataDir, kind, func(year, month string) error {
		moves, err := loadMoves(dataDir, year, month, kind)
		if err != nil || len(moves) == 0 {
			return nil
		}
		key := year + "-" + month
		moveCache[key] = moves
		for i, m := range moves {
			uri := OdooURI(host, db, kind.model, m.ID)
			uriToEntries[uri] = append(uriToEntries[uri], entry{year, month, i})
		}
		return nil
	})
	if err != nil {
		return err
	}
	if len(uriToEntries) == 0 {
		fmt.Printf("  %sNo cached %s to annotate.%s\n", Fmt.Dim, kind.labelPl, Fmt.Reset)
		return nil
	}

	uris := make([]string, 0, len(uriToEntries))
	for u := range uriToEntries {
		uris = append(uris, u)
	}

	fmt.Printf("\n%s📡 Fetching Nostr annotations for %d %s...%s\n",
		Fmt.Bold, len(uris), kind.labelPl, Fmt.Reset)

	var since *time.Time
	if t := LastSyncTime("nostr-" + kind.labelPl); !t.IsZero() {
		since = &t
	}

	annotations, err := FetchNostrAnnotations(uris, since)
	if err != nil {
		return fmt.Errorf("fetch annotations: %w", err)
	}
	fmt.Printf("  %sGot %d annotations%s\n", Fmt.Dim, len(annotations), Fmt.Reset)

	changedMonths := map[string]bool{}
	applied := 0
	for uri, ann := range annotations {
		if ann == nil || (ann.Category == "" && ann.Collective == "" && ann.Event == "") {
			continue
		}
		for _, e := range uriToEntries[uri] {
			key := e.year + "-" + e.month
			moves := moveCache[key]
			m := moves[e.idx]
			// Only overwrite when there's something to say.
			if ann.Category != "" {
				m.Category = ann.Category
			}
			if ann.Collective != "" {
				m.Collective = ann.Collective
			}
			if ann.Event != "" {
				m.Event = ann.Event
			}
			moves[e.idx] = m
			moveCache[key] = moves
			changedMonths[key] = true
			applied++
		}
	}

	for key := range changedMonths {
		y, m := key[:4], key[5:7]
		if err := saveMoves(dataDir, y, m, kind, moveCache[key]); err != nil {
			fmt.Printf("  %s✗ %s: %v%s\n", Fmt.Red, key, err, Fmt.Reset)
		}
	}

	UpdateSyncSource("nostr-"+kind.labelPl, false)

	fmt.Printf("\n%s✓ Applied %s across %s%s\n\n",
		Fmt.Green, Pluralize(applied, kind.label+" annotation", ""), Pluralize(len(changedMonths), "month", ""), Fmt.Reset)
	return nil
}

func printMovesSyncNostrHelp(label string) {
	f := Fmt
	fmt.Printf(`
%schb %s sync nostr%s — Fetch %s annotations from Nostr

%sUSAGE%s
  %schb %s sync nostr%s

%sDESCRIPTION%s
  Queries every configured Nostr relay for kind 1111 annotations on the
  URIs of your cached %s (odoo:<host>:<db>:account.move:<id>) and merges
  them into the local %s.json files.

  Incremental: only fetches events newer than the last successful run.
`,
		f.Bold, label, f.Reset, label,
		f.Bold, f.Reset,
		f.Cyan, label, f.Reset,
		f.Bold, f.Reset,
		label,
		label,
	)
}
