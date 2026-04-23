package cmd

import (
	"fmt"
)

// GenerateInvoices applies the priority chain to cached invoice months,
// filling in category / collective / event where the annotation pass (Nostr
// + user categorize) left gaps. Backfills from local rules.
func GenerateInvoices(args []string) error {
	return generateMovesWithRules(moveKindInvoice, args)
}

// GenerateBills does the same for vendor bills.
func GenerateBills(args []string) error {
	return generateMovesWithRules(moveKindBill, args)
}

// generateMovesWithRules walks every cached month for the given move kind and
// runs the priority chain:
//
//  1. Existing move.Category / Collective / Event from a prior pass (Nostr
//     annotation or user categorize) — never overwritten.
//  2. Odoo analytic category — already in move.Category from sync; preserved.
//  3. Local rules (rules.json) — only applied when the move still has no
//     collective / category after steps 1-2.
//
// The final invoices.json / bills.json becomes the "ready to consume" view.
func generateMovesWithRules(kind moveKind, args []string) error {
	_ = args // reserved for future filters (--year, --month, --dry-run)

	settings, err := LoadSettings()
	if err != nil {
		return fmt.Errorf("load settings: %w", err)
	}
	categorizer := NewCategorizer(settings)

	direction := "CREDIT"
	if kind.isBill {
		direction = "DEBIT"
	}

	dataDir := DataDir()
	changedMonths := map[string]int{}

	err = walkMoveMonths(dataDir, kind, func(year, month string) error {
		moves, err := loadMoves(dataDir, year, month, kind)
		if err != nil || len(moves) == 0 {
			return nil
		}
		partners := loadMovePartners(dataDir, year, month, kind)

		changed := 0
		for i, m := range moves {
			if m.Category != "" && m.Collective != "" {
				continue // fully annotated
			}
			tx := TransactionEntry{
				Provider:     "odoo",
				Currency:     m.Currency,
				Type:         direction,
				Counterparty: partners[m.ID],
				Metadata:     map[string]interface{}{"description": firstNonEmptyStr(m.Title, partners[m.ID])},
			}
			if m.Category == "" {
				if cat := categorizer.Categorize(tx); cat != "" {
					m.Category = cat
				}
			}
			if m.Collective == "" {
				if col := categorizer.CollectiveFor(tx); col != "" {
					m.Collective = col
				}
			}
			if m.Category != moves[i].Category || m.Collective != moves[i].Collective {
				moves[i] = m
				changed++
			}
		}
		if changed > 0 {
			if err := saveMoves(dataDir, year, month, kind, moves); err != nil {
				fmt.Printf("  %s✗ %s-%s: %v%s\n", Fmt.Red, year, month, err, Fmt.Reset)
				return nil
			}
			changedMonths[year+"-"+month] = changed
		}
		return nil
	})
	if err != nil {
		return err
	}

	total := 0
	for _, n := range changedMonths {
		total += n
	}
	fmt.Printf("  %s✓ generate %s: %d %s updated across %d month(s)%s\n",
		Fmt.Green, kind.labelPl, total, kind.labelPl, len(changedMonths), Fmt.Reset)
	return nil
}
