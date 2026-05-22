package cmd

import (
	"fmt"
	"strings"
)

// txReconcileCandidate + findInvoiceCandidatesForTx have been
// replaced by Suggestion + SuggestForTx in cmd/reconcile_suggest.go,
// which adds two-pass widening (open → all-posted) so the picker
// can offer unreconcile + reattach for already-paid invoices/bills.
// candidateMatchesTxPartner below is still used by the new suggester.

// findOdooLineForTx scans every linked Odoo journal's cache for the
// bank statement line whose unique_import_id matches the tx's. Used
// by the [r] reconcile flow to resolve a TransactionEntry back to its
// counterpart on Odoo's side.
//
// Returns (line, journalID, true) on success; ok=false when nothing
// matches OR when the caller didn't pass an importID. Callers should
// distinguish "no match" from "already reconciled" via line.IsReconciled.
func findOdooLineForTx(importID string) (OdooCacheLine, int, bool) {
	if importID == "" {
		return OdooCacheLine{}, 0, false
	}
	target := strings.ToLower(importID)
	canon := strings.ToLower(CanonicalizeImportID(importID))
	for _, jid := range allLinkedOdooJournalIDs() {
		lines, ok := loadLatestOdooJournalLinesCache(jid)
		if !ok {
			continue
		}
		for _, ln := range lines {
			if strings.ToLower(ln.UniqueImportID) == target ||
				(canon != "" && strings.ToLower(ln.UniqueImportID) == canon) {
				return ln, jid, true
			}
		}
	}
	return OdooCacheLine{}, 0, false
}

// candidateMatchesTxPartner is the inverse of bankLineMatchesPartner:
// we have the tx's counterparty name (free text, e.g. "INFLIGHTS BV")
// and the candidate's PartnerName from Odoo's partner index. Token
// match in either direction — any token from one appearing in the
// other counts as a partner match. Empty inputs return false (the
// "no signal" answer).
func candidateMatchesTxPartner(c reconcileCandidate, txCounterparty string, txTokens []string) bool {
	if c.PartnerName == "" || (txCounterparty == "" && len(txTokens) == 0) {
		return false
	}
	candHay := strings.ToLower(c.PartnerName)
	for _, t := range txTokens {
		if strings.Contains(candHay, t) {
			return true
		}
	}
	candTokens := partnerNameTokens(c.PartnerName)
	txHay := strings.ToLower(txCounterparty)
	for _, t := range candTokens {
		if strings.Contains(txHay, t) {
			return true
		}
	}
	return false
}

// attachTxToInvoiceCandidate is the per-cursor-row apply action.
// Resolves Odoo credentials, authenticates, calls the same
// reconcileStatementLineWithMove flow the journal-side -i and the
// invoice-side [r] reconcile use. On success, patches the local
// journal-lines cache (IsReconciled = true) so the same line doesn't
// reappear in candidate searches.
func attachTxToInvoiceCandidate(line OdooCacheLine, journalID int, cand reconcileCandidate) error {
	creds, err := ResolveOdooCredentials()
	if err != nil {
		return err
	}
	uid, err := odooAuth(creds.URL, creds.DB, creds.Login, creds.Password)
	if err != nil || uid == 0 {
		return fmt.Errorf("Odoo authentication failed: %v", err)
	}
	stmtLine := odooStatementLineForReconcile{
		ID:     line.ID,
		MoveID: line.MoveID,
		Amount: line.Amount,
	}
	moveCand := odooMoveCandidate{
		ID:             cand.ID,
		Name:           cand.Number,
		PartnerID:      cand.PartnerID,
		PartnerName:    cand.PartnerName,
		AmountResidual: cand.Residual,
	}
	if err := reconcileStatementLineWithMove(creds, uid, stmtLine, moveCand); err != nil {
		return err
	}
	if cached, ok := loadLatestOdooJournalLinesCache(journalID); ok {
		for i := range cached {
			if cached[i].ID == line.ID {
				cached[i].IsReconciled = true
				_, _ = writeOdooJournalLinesCacheFile(journalID, cached)
				break
			}
		}
	}
	return nil
}
