package cmd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
)

// handleStatementLineCrossJournalConflicts pauses sync when newly-pushed
// statement lines collide with existing lines in *other* Odoo journals
// (Odoo enforces a database-wide unique constraint on
// `account.bank.statement.line.unique_import_id`). It explains the
// conflict, prints a link to the conflicting journal, and offers the user
// three resolution paths:
//
//  1. Empty the conflicting journal (e.g. it's deprecated)
//  2. Rename the conflicting imports in place
//     (`odoo:journals:<id>:<original>`) to free up the references
//  3. Exit and let the user investigate
//
// Returns a non-nil error to abort the current sync run. The caller is
// expected to surface it and let the user re-run sync; we don't retry
// in-line so the user can verify the action before importing.
func handleStatementLineCrossJournalConflicts(creds *OdooCredentials, uid int, currentJournalID int, failures []statementLineCreateFailure) error {
	conflicts := map[int][]string{}
	for _, f := range failures {
		if f.ConflictJournalID > 0 && f.ConflictJournalID != currentJournalID {
			conflicts[f.ConflictJournalID] = append(conflicts[f.ConflictJournalID], f.ImportID)
		}
	}
	if len(conflicts) == 0 {
		return nil
	}

	if !isInteractiveTTY() {
		return fmt.Errorf("cross-journal import-id conflicts detected (run `chb odoo journals %d sync --transactions` interactively to resolve)", currentJournalID)
	}

	ids := make([]int, 0, len(conflicts))
	for id := range conflicts {
		ids = append(ids, id)
	}
	sort.Ints(ids)

	reader := bufio.NewReader(os.Stdin)

	for _, otherJournalID := range ids {
		importIDs := conflicts[otherJournalID]
		journalName := OdooJournalName(otherJournalID)
		if journalName == "" {
			if name, err := FetchAndCacheOdooJournalName(creds, uid, otherJournalID); err == nil && name != "" {
				journalName = name
			} else {
				journalName = fmt.Sprintf("journal #%d", otherJournalID)
			}
		}
		url := OdooBankReconciliationURL(creds.URL, otherJournalID)

		fmt.Println()
		Warnf("  %s⚠ %s in journal '%s' (#%d) already use %s being pushed to journal #%d%s",
			Fmt.Yellow,
			Pluralize(len(importIDs), "reference", ""),
			journalName, otherJournalID,
			Pluralize(len(importIDs), "import id", ""),
			currentJournalID,
			Fmt.Reset)
		fmt.Printf("    %sInspect: %s%s\n", Fmt.Dim, url, Fmt.Reset)
		examples := importIDs
		if len(examples) > 3 {
			examples = examples[:3]
		}
		fmt.Printf("    %sExample %s: %s%s\n",
			Fmt.Dim,
			Pluralize(len(examples), "reference", ""),
			strings.Join(examples, ", "),
			Fmt.Reset)
		fmt.Println()
		fmt.Printf("  How do you want to proceed?\n")
		fmt.Printf("    1. Empty journal '%s' (#%d)\n", journalName, otherJournalID)
		fmt.Printf("    2. Update all conflicting references (prepend 'odoo:journals:%d:')\n", otherJournalID)
		fmt.Printf("    3. Exit\n")
		fmt.Printf("  %sChoice [1/2/3]: %s", Fmt.Bold, Fmt.Reset)

		choice, _ := reader.ReadString('\n')
		switch strings.TrimSpace(choice) {
		case "1":
			if err := emptyOdooJournal(creds, uid, otherJournalID, false); err != nil {
				return fmt.Errorf("empty journal #%d: %v", otherJournalID, err)
			}
		case "2":
			renamed, err := renameOdooImportIDsInJournal(creds, uid, otherJournalID, importIDs)
			if err != nil {
				return fmt.Errorf("rename references in journal #%d: %v", otherJournalID, err)
			}
			fmt.Printf("  %s✓ Renamed %s in journal '%s' (#%d)%s\n",
				Fmt.Green, Pluralize(renamed, "reference", ""), journalName, otherJournalID, Fmt.Reset)
		case "3", "":
			return fmt.Errorf("aborted: %s in journal '%s' (#%d) conflict with this sync",
				Pluralize(len(importIDs), "reference", ""), journalName, otherJournalID)
		default:
			return fmt.Errorf("invalid choice %q", strings.TrimSpace(choice))
		}
	}

	fmt.Printf("\n  %s✓ Conflicts resolved. Re-run sync to import the freed references.%s\n\n",
		Fmt.Green, Fmt.Reset)
	return fmt.Errorf("cross-journal conflicts resolved; re-run sync to continue")
}

// renameOdooImportIDsInJournal prefixes each line's unique_import_id with
// `odoo:journals:<journalID>:` so the originals are freed up for use in
// another journal. The lines themselves stay in place; only the import id
// is rewritten so it no longer collides under Odoo's unique constraint.
func renameOdooImportIDsInJournal(creds *OdooCredentials, uid int, journalID int, importIDs []string) (int, error) {
	if len(importIDs) == 0 {
		return 0, nil
	}
	const chunkSize = 200
	type row struct {
		ID             int    `json:"id"`
		UniqueImportID string `json:"unique_import_id"`
	}
	var rows []row
	for start := 0; start < len(importIDs); start += chunkSize {
		end := start + chunkSize
		if end > len(importIDs) {
			end = len(importIDs)
		}
		chunk := make([]interface{}, end-start)
		for i, s := range importIDs[start:end] {
			chunk[i] = s
		}
		data, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.bank.statement.line", "search_read",
			[]interface{}{[]interface{}{
				[]interface{}{"journal_id", "=", journalID},
				[]interface{}{"unique_import_id", "in", chunk},
			}},
			map[string]interface{}{
				"fields": []string{"id", "unique_import_id"},
				"limit":  0,
			})
		if err != nil {
			return 0, fmt.Errorf("search lines: %v", err)
		}
		var batch []row
		if err := json.Unmarshal(data, &batch); err != nil {
			return 0, fmt.Errorf("parse lines: %v", err)
		}
		rows = append(rows, batch...)
	}
	prefix := fmt.Sprintf("odoo:journals:%d:", journalID)
	updated := 0
	for _, r := range rows {
		newID := prefix + r.UniqueImportID
		if _, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.bank.statement.line", "write",
			[]interface{}{[]interface{}{r.ID}, map[string]interface{}{
				"unique_import_id": newID,
			}}, nil); err != nil {
			return updated, fmt.Errorf("update line #%d: %v", r.ID, err)
		}
		updated++
	}
	return updated, nil
}

func isInteractiveTTY() bool {
	if quietOdooContext() {
		return false
	}
	info, err := os.Stdin.Stat()
	if err != nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) != 0
}
