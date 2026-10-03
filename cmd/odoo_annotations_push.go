package cmd

// `chb odoo annotations push` — consolidate trusted Nostr annotations into
// Odoo, so Odoo is the single source of truth for analytic accounts.
//
//   - bills and credit notes, customer invoices: `chb bills|invoices push`
//     (analytic_distribution on the move lines);
//   - KBC journals (owned by Odoo): `chb odoo journals <id> categorize`
//     (analytic_distribution on the statement lines);
//   - journals chb pushes into (Stripe, on-chain, Monerium): the hourly
//     `chb odoo sync` already categorises their lines from the
//     transactions' categories, annotations included.
//
// Every annotation event is applied once (ledger in
// latest/providers/odoo/<db>/annotations-applied.json): a later change in
// Odoo wins until someone annotates again. Odoo's own categorisation goes
// back to Nostr with `chb nostr push`. See docs/annotations.md.

import "fmt"

func OdooAnnotationsCommand(args []string) error {
	if len(args) == 0 || HasFlag(args, "--help", "-h") || args[0] != "push" {
		fmt.Print(`
chb odoo annotations push — apply trusted Nostr annotations to Odoo

USAGE
  chb odoo annotations push [YYYY[/MM]] [--dry-run] [--yes] [--verbose]

Writes the category/collective of trusted annotations not yet applied as
analytic distributions on bills, credit notes, invoices (and posted expense
claims) and on KBC journal lines. Preview first; --yes applies without
asking (cron). Run 'chb pull' first so the annotation caches are fresh.
`)
		return nil
	}
	rest := append(append([]string{}, args[1:]...), "--annotations-only")
	dryRun := HasFlag(rest, "--dry-run")
	if err := MovesPushCommand(moveKindBill, rest); err != nil {
		return fmt.Errorf("bills: %w", err)
	}
	if err := MovesPushCommand(moveKindInvoice, rest); err != nil {
		return fmt.Errorf("invoices: %w", err)
	}
	creds, err := ResolveOdooCredentials()
	if err != nil {
		return err
	}
	uid, err := odooAuth(creds.URL, creds.DB, creds.Login, creds.Password)
	if err != nil || uid == 0 {
		return fmt.Errorf("Odoo authentication failed: %v", err)
	}
	configs := LoadAccountConfigs()
	for i := range configs {
		acc := &configs[i]
		if acc.Provider != "kbcbrussels" || acc.OdooJournalID == 0 || acc.ArchivedAt != "" {
			continue
		}
		fmt.Printf("\n%s▶ KBC journal #%d (%s)%s\n", Fmt.Bold, acc.OdooJournalID, acc.Slug, Fmt.Reset)
		if err := categorizeOdooJournalAnnotations(creds, uid, acc.OdooJournalID, acc, dryRun, HasFlag(rest, "--yes", "-y"), HasFlag(rest, "--verbose", "-v")); err != nil {
			Warnf("⚠ journal #%d: %v", acc.OdooJournalID, err)
		}
	}
	return nil
}
