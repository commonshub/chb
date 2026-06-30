package cmd

import (
	"fmt"
	"strings"
)

// matchedMoveRef names one move an already-reconciled line is matched against.
type matchedMoveRef struct {
	moveID int
	name   string
}

// reconciledLineState describes whether a move line is already reconciled and,
// if so, which moves it is matched against — so we can tell the operator where
// to look (and offer to break the stale match and re-attach).
type reconciledLineState struct {
	lineID     int
	reconciled bool
	matches    []matchedMoveRef
}

func (s reconciledLineState) matchNames() string {
	if len(s.matches) == 0 {
		return ""
	}
	parts := make([]string, 0, len(s.matches))
	for _, m := range s.matches {
		if m.name != "" {
			parts = append(parts, m.name)
		}
	}
	return strings.Join(parts, ", ")
}

// reconciledLineMatches reports whether a single account.move.line is already
// reconciled and resolves the counterpart moves it is matched against by walking
// its account.partial.reconcile links.
func reconciledLineMatches(creds *OdooCredentials, uid, lineID int) (reconciledLineState, error) {
	state := reconciledLineState{lineID: lineID}
	rows, err := odooSearchReadAllMaps(creds, uid, "account.move.line",
		[]interface{}{[]interface{}{"id", "=", lineID}},
		[]string{"id", "reconciled", "matched_debit_ids", "matched_credit_ids"}, "")
	if err != nil {
		return state, err
	}
	if len(rows) == 0 {
		return state, nil
	}
	r := rows[0]
	state.reconciled = odooBool(r["reconciled"])

	var partialIDs []int
	for _, key := range []string{"matched_debit_ids", "matched_credit_ids"} {
		if arr, ok := r[key].([]interface{}); ok {
			for _, v := range arr {
				if id := odooInt(v); id > 0 {
					partialIDs = append(partialIDs, id)
				}
			}
		}
	}
	if len(partialIDs) == 0 {
		return state, nil
	}

	// partial.reconcile → the two move lines it bridges; the one that isn't us
	// is the counterpart. Best-effort: a read failure leaves matches empty but
	// still reports reconciled=true.
	partials, err := odooSearchReadAllMaps(creds, uid, "account.partial.reconcile",
		[]interface{}{[]interface{}{"id", "in", intsToInterfaces(partialIDs)}},
		[]string{"id", "debit_move_id", "credit_move_id"}, "")
	if err != nil {
		return state, nil
	}
	counterpartLines := map[int]bool{}
	for _, p := range partials {
		for _, k := range []string{"debit_move_id", "credit_move_id"} {
			if id := odooFieldID(p[k]); id > 0 && id != lineID {
				counterpartLines[id] = true
			}
		}
	}
	if len(counterpartLines) == 0 {
		return state, nil
	}
	lineIDs := make([]int, 0, len(counterpartLines))
	for id := range counterpartLines {
		lineIDs = append(lineIDs, id)
	}
	lines, err := odooSearchReadAllMaps(creds, uid, "account.move.line",
		[]interface{}{[]interface{}{"id", "in", intsToInterfaces(lineIDs)}},
		[]string{"id", "move_id"}, "")
	if err != nil {
		return state, nil
	}
	seen := map[int]bool{}
	for _, l := range lines {
		mid := odooFieldID(l["move_id"])
		if mid > 0 && !seen[mid] {
			seen[mid] = true
			state.matches = append(state.matches, matchedMoveRef{moveID: mid, name: odooFieldName(l["move_id"])})
		}
	}
	return state, nil
}

// unreconcileLines drops every reconciliation on the given move lines via Odoo's
// official remove_move_reconcile (which also resets the related moves' payment
// state). Used to break a stale match before re-attaching.
func unreconcileLines(creds *OdooCredentials, uid int, lineIDs []int) error {
	if len(lineIDs) == 0 {
		return nil
	}
	_, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.move.line", "remove_move_reconcile", []interface{}{lineIDs}, nil)
	return err
}

// alreadyReconciledAttachError builds a human-actionable error for the case where
// the bank counterpart (or the invoice's A/R line) is already reconciled
// elsewhere, so a fresh reconcile against `move` is refused by Odoo. It names
// the conflicting move(s) and gives clickable Odoo URLs plus the re-attach hint.
func alreadyReconciledAttachError(creds *OdooCredentials, kindLabel string, line odooStatementLineForReconcile, move odooMoveCandidate, states []reconciledLineState) error {
	var b strings.Builder
	fmt.Fprintf(&b, "bank line #%d is already matched to another invoice/bill, so it can't be attached to %s",
		line.ID, moveDisplayName(move))

	for _, st := range states {
		if !st.reconciled {
			continue
		}
		if names := st.matchNames(); names != "" {
			fmt.Fprintf(&b, "\n      ↳ already matched with %s", names)
			for _, m := range st.matches {
				fmt.Fprintf(&b, "\n        %s", OdooWebURL(creds.URL, "account.move", m.moveID))
			}
		} else {
			fmt.Fprintf(&b, "\n      ↳ line #%d is reconciled (open it to see the match)", st.lineID)
		}
	}

	// Where to look / what to open.
	fmt.Fprintf(&b, "\n      target %s: %s", kindLabel, OdooWebURL(creds.URL, "account.move", move.ID))
	if line.MoveID > 0 {
		fmt.Fprintf(&b, "\n      bank entry:   %s", OdooWebURL(creds.URL, "account.move", line.MoveID))
	}
	fmt.Fprintf(&b, "\n      to re-attach (break the stale match and link it here), run: chb %s reconcile -i  →  pick this %s with [r]",
		kindPluralLabel(kindLabel), kindLabel)
	return fmt.Errorf("%s", b.String())
}

func moveDisplayName(move odooMoveCandidate) string {
	if move.Name != "" {
		return move.Name
	}
	return fmt.Sprintf("move #%d", move.ID)
}

// diagnoseReconcileOffenders inspects the two lines a failed reconcile tried to
// match and returns their reconciliation state plus the ids of those already
// reconciled (the ones blocking the reconcile).
func diagnoseReconcileOffenders(creds *OdooCredentials, uid int, lineIDs ...int) (states []reconciledLineState, offenders []int) {
	for _, id := range lineIDs {
		if id == 0 {
			continue
		}
		st, err := reconciledLineMatches(creds, uid, id)
		if err != nil {
			continue
		}
		states = append(states, st)
		if st.reconciled {
			offenders = append(offenders, id)
		}
	}
	return states, offenders
}

// printReattachNote tells the operator the stale match we just broke before
// re-attaching, naming the move it was wrongly reconciled against when known.
func printReattachNote(line odooStatementLineForReconcile, move odooMoveCandidate, states []reconciledLineState) {
	names := ""
	for _, st := range states {
		if st.reconciled {
			if n := st.matchNames(); n != "" {
				names = n
				break
			}
		}
	}
	if names != "" {
		fmt.Printf("  %s↻ broke stale match (was on %s) and re-attached bank line #%d to %s%s\n",
			Fmt.Yellow, names, line.ID, moveDisplayName(move), Fmt.Reset)
		return
	}
	fmt.Printf("  %s↻ unreconciled stale match and re-attached bank line #%d to %s%s\n",
		Fmt.Yellow, line.ID, moveDisplayName(move), Fmt.Reset)
}

// moveKindLabelFromOdoo returns "bill" for vendor moves and "invoice" otherwise.
// Read live because the batch-path odooMoveCandidate doesn't carry move_type.
func moveKindLabelFromOdoo(creds *OdooCredentials, uid, moveID int) string {
	rows, err := odooSearchReadAllMaps(creds, uid, "account.move",
		[]interface{}{[]interface{}{"id", "=", moveID}}, []string{"id", "move_type"}, "")
	if err == nil && len(rows) > 0 {
		if strings.HasPrefix(odooString(rows[0]["move_type"]), "in_") {
			return "bill"
		}
	}
	return "invoice"
}

func kindPluralLabel(label string) string {
	if label == "bill" {
		return "bills"
	}
	return "invoices"
}
