package cmd

// Matching bank lines with vendor bills and customer invoices beyond Odoo's
// own move names:
//
//   - the vendor's reference: a bill's `ref` / `payment_reference`
//     compared digit for digit with the numbers in the bank text, leading
//     zeros ignored (KBC's "007603556405" ↔ Proximus' ref "7603556405");
//   - every partner holding the line's IBAN, through their commercial
//     partner: a duplicate contact ("Proximus" vs "Proximus SA de droit
//     public") no longer hides the open bills;
//   - a guard for the automatic categorisation: a line whose party has an
//     open document of the same amount is left on the suspense account for
//     bill matching instead of being booked to an expense or income account
//     (which would book it twice).

import (
	"regexp"
	"sort"
	"strings"
)

var longDigitRun = regexp.MustCompile(`\d{6,}`)

// statementLineDigitRefs: the digit runs of the bank text (6+ digits),
// leading zeros stripped.
func statementLineDigitRefs(line odooStatementLineForReconcile) []string {
	seen := map[string]bool{}
	var out []string
	for _, m := range longDigitRun.FindAllString(line.PaymentRef+" "+line.Narration, -1) {
		d := strings.TrimLeft(m, "0")
		if len(d) >= 6 && !seen[d] {
			seen[d] = true
			out = append(out, d)
		}
	}
	sort.Strings(out)
	return out
}

func refDigits(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return strings.TrimLeft(b.String(), "0")
}

// findOpenMoveCandidatesByVendorRef: open documents of the line's amount
// whose ref or payment reference equals a digit run of the bank text.
func findOpenMoveCandidatesByVendorRef(creds *OdooCredentials, uid int, line odooStatementLineForReconcile) ([]odooMoveCandidate, error) {
	refs := statementLineDigitRefs(line)
	if len(refs) == 0 {
		return nil, nil
	}
	absAmount := line.Amount
	if absAmount < 0 {
		absAmount = -absAmount
	}
	if absAmount < 0.005 {
		return nil, nil
	}
	moveTypes := []interface{}{"out_invoice"}
	if line.Amount < 0 {
		moveTypes = []interface{}{"in_invoice"}
	}
	domain := append([]interface{}{
		[]interface{}{"state", "=", "posted"},
		[]interface{}{"move_type", "in", moveTypes},
		[]interface{}{"payment_state", "not in", []interface{}{"paid", "in_payment", "reversed"}},
		[]interface{}{"amount_residual", ">=", roundCents(absAmount - 0.01)},
		[]interface{}{"amount_residual", "<=", roundCents(absAmount + 0.01)},
	}, vendorRefDomain(refs)...)
	rows, err := odooSearchReadAllMaps(creds, uid, "account.move", domain,
		[]string{"id", "name", "invoice_date", "date", "move_type", "partner_id", "amount_residual", "ref", "payment_reference"},
		"invoice_date desc, id desc")
	if err != nil {
		return nil, err
	}
	want := map[string]bool{}
	for _, r := range refs {
		want[r] = true
	}
	var kept []map[string]interface{}
	for _, row := range rows {
		if want[refDigits(odooString(row["ref"]))] || want[refDigits(odooString(row["payment_reference"]))] {
			kept = append(kept, row)
		}
	}
	return parseOdooMoveCandidates(kept), nil
}

// relatedCommercialPartnerIDs: the commercial partners of partnerID and of
// every partner holding the line's bank account.
func relatedCommercialPartnerIDs(creds *OdooCredentials, uid int, line odooStatementLineForReconcile, partnerID int) []int {
	partners := map[int]bool{}
	if partnerID > 0 {
		partners[partnerID] = true
	}
	if line.PartnerBankID > 0 {
		if rows, err := odooReadMapsByIDs(creds, uid, "res.partner.bank", []int{line.PartnerBankID}, []string{"sanitized_acc_number"}); err == nil && len(rows) > 0 {
			if acc := odooString(rows[0]["sanitized_acc_number"]); acc != "" {
				if banks, err := odooSearchReadAllMaps(creds, uid, "res.partner.bank",
					[]interface{}{[]interface{}{"sanitized_acc_number", "=", acc}}, []string{"partner_id"}, "id"); err == nil {
					for _, b := range banks {
						if id := odooFieldID(b["partner_id"]); id > 0 {
							partners[id] = true
						}
					}
				}
			}
		}
	}
	if len(partners) == 0 {
		return nil
	}
	ids := make([]int, 0, len(partners))
	for id := range partners {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	out := map[int]bool{}
	if rows, err := odooReadMapsByIDs(creds, uid, "res.partner", ids, []string{"commercial_partner_id"}); err == nil {
		for _, r := range rows {
			if c := odooFieldID(r["commercial_partner_id"]); c > 0 {
				out[c] = true
			}
		}
	}
	if len(out) == 0 { // could not read them: use the partners themselves
		for _, id := range ids {
			out[id] = true
		}
	}
	res := make([]int, 0, len(out))
	for id := range out {
		res = append(res, id)
	}
	sort.Ints(res)
	return res
}

// lineHasOpenDocument reports whether the line's party has an open
// invoice/bill of the line's amount (dated within three months before the
// line to one month after), or one matching its vendor reference.
func lineHasOpenDocument(creds *OdooCredentials, uid int, line odooStatementLineForReconcile) (bool, string) {
	if c, err := findOpenMoveCandidatesByVendorRef(creds, uid, line); err == nil && len(c) > 0 {
		return true, candidateDisplayName(c[0])
	}
	partners := relatedCommercialPartnerIDs(creds, uid, line, line.PartnerID)
	if len(partners) == 0 {
		return false, ""
	}
	c, err := findOpenMoveCandidatesForPartners(creds, uid, line, partners, 1)
	if err != nil || len(c) == 0 {
		return false, ""
	}
	return true, candidateDisplayName(c[0])
}

// holdLinesWithOpenDocuments splits lineIDs into those safe to categorise
// and those left for bill/invoice matching. Read errors keep the line
// categorisable (the previous behaviour).
func holdLinesWithOpenDocuments(creds *OdooCredentials, uid int, lineIDs []int) (keep []int, held map[int]string) {
	held = map[int]string{}
	if len(lineIDs) == 0 {
		return lineIDs, held
	}
	rows, err := odooReadMapsByIDs(creds, uid, "account.bank.statement.line", lineIDs, statementLineReconcileFields())
	if err != nil {
		return lineIDs, held
	}
	byID := map[int]odooStatementLineForReconcile{}
	for _, l := range parseStatementLineRows(rows) {
		byID[l.ID] = l
	}
	for _, id := range lineIDs {
		l, ok := byID[id]
		if !ok {
			keep = append(keep, id)
			continue
		}
		if has, doc := lineHasOpenDocument(creds, uid, l); has {
			held[id] = doc
			continue
		}
		keep = append(keep, id)
	}
	return keep, held
}

// applyOdooMappingAccountUnlessDocument is applyOdooMappingAccount for the
// automatic categorisation (push, categorise, KBC merge): lines whose
// party has an open invoice/bill of the same amount stay on the suspense
// account for bill matching (`chb odoo journals <id> reconcile -i`).
func applyOdooMappingAccountUnlessDocument(creds *OdooCredentials, uid int, lineIDs []int, accountCode string, progress ...*statusLine) error {
	keep, held := holdLinesWithOpenDocuments(creds, uid, lineIDs)
	ids := make([]int, 0, len(held))
	for id := range held {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	for _, id := range ids {
		Warnf("  %s⚠ line #%d not booked to %s: open document %s of the same amount — left for bill matching%s",
			Fmt.Yellow, id, accountCode, held[id], Fmt.Reset)
	}
	if len(keep) == 0 {
		return nil
	}
	return applyOdooMappingAccount(creds, uid, keep, accountCode, progress...)
}

// vendorRefDomain: (ref ilike r1) OR (payment_reference ilike r1) OR … in
// Odoo's prefix notation (n leaves need n-1 "|"), narrowed client-side to
// exact digit equality.
func vendorRefDomain(refs []string) []interface{} {
	var leaves []interface{}
	for _, r := range refs {
		leaves = append(leaves, []interface{}{"ref", "ilike", r}, []interface{}{"payment_reference", "ilike", r})
	}
	out := make([]interface{}, 0, 2*len(leaves))
	for i := 1; i < len(leaves); i++ {
		out = append(out, "|")
	}
	return append(out, leaves...)
}
