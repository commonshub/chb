package cmd

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	odoosource "github.com/CommonsHub/chb/providers/odoo"
)

const odooJournalLinesSchemaVersion = 1

type OdooJournalLinesFile struct {
	SchemaVersion int             `json:"schemaVersion"`
	Provider      string          `json:"provider"`
	FetchedAt     string          `json:"fetchedAt"`
	JournalID     int             `json:"journalId"`
	Count         int             `json:"count"`
	Lines         []OdooCacheLine `json:"lines"`
}

type OdooCacheLine struct {
	ID             int                    `json:"id"`
	MoveID         int                    `json:"moveId,omitempty"`
	PartnerID      int                    `json:"partnerId,omitempty"`
	AccountID      int                    `json:"accountId,omitempty"`
	CounterpartID  int                    `json:"counterpartId,omitempty"`
	Date           string                 `json:"date,omitempty"`
	PaymentRef     string                 `json:"paymentRef,omitempty"`
	UniqueImportID string                 `json:"uniqueImportId,omitempty"`
	Amount         float64                `json:"amount"`
	IsReconciled   bool                   `json:"isReconciled,omitempty"`
	Narration      string                 `json:"narration,omitempty"`
	Metadata       map[string]interface{} `json:"metadata,omitempty"`
}

func writeOdooJournalLinesCache(creds *OdooCredentials, uid int, journalID int) (int, error) {
	// Watermark-based incremental refresh: fetch only lines whose
	// write_date is newer than the last cached max, merge into the
	// existing cache, and stamp the new watermark on the cursor.
	// Falls back to a full fetch when no cursor exists (first run)
	// or when the cache is missing. A 700-line journal with one new
	// line drops from "page all 700 rows" to "fetch 1 row".
	cur := LoadSyncCursor(SyncCursorKeyForOdooJournal(journalID))
	existing, haveCache := loadLatestOdooJournalLinesCache(journalID)
	if !haveCache || cur.LastWriteDate == "" {
		return writeOdooJournalLinesCacheFullRefetch(creds, uid, journalID, &cur)
	}

	newLines, maxWriteDate, err := fetchOdooJournalLinesSince(creds, uid, journalID, cur.LastWriteDate)
	if err != nil {
		return 0, err
	}

	// Merge: replace any existing line with the same id, otherwise
	// append. Handles edits (write_date bumped) and new arrivals; the
	// post-merge aggregate check below catches deletes the watermark
	// fetch can't see.
	merged := existing
	if len(newLines) > 0 {
		byID := make(map[int]OdooCacheLine, len(existing)+len(newLines))
		for _, l := range existing {
			byID[l.ID] = l
		}
		for _, l := range newLines {
			byID[l.ID] = l
		}
		merged = make([]OdooCacheLine, 0, len(byID))
		for _, l := range byID {
			merged = append(merged, l)
		}
	}

	// Verify against the server-side aggregate. A `write_date > X`
	// filter can't see deletions — Odoo simply omits the row from the
	// result set — so a line deleted between pulls stays in the local
	// cache and drifts the count/balance by 1 each cycle. That used to
	// surface later as a confusing "out of sync, run `chb odoo pull`
	// first" error on the next push, even though pull had just run.
	// One cheap read_group RPC tells us whether the merge result
	// matches Odoo; when it doesn't, full re-fetch is the only safe
	// path because we have no way to learn which id was deleted.
	if journalCacheMatchesOdoo(creds, uid, journalID, merged) {
		if len(newLines) == 0 {
			return len(existing), nil
		}
		count, err := writeOdooJournalLinesCacheFile(journalID, merged)
		if err != nil {
			return 0, err
		}
		cur.LastWriteDate = maxWriteDate
		cur.Count = count
		_ = SaveSyncCursor(cur)
		return count, nil
	}
	return writeOdooJournalLinesCacheFullRefetch(creds, uid, journalID, &cur)
}

// writeOdooJournalLinesCacheFullRefetch does the full paginated fetch +
// cursor reset. Used on first run (no cache) and as the fallback path
// when the incremental refresh's aggregate check detects drift (the
// canonical signal of a deletion the watermark missed).
func writeOdooJournalLinesCacheFullRefetch(creds *OdooCredentials, uid int, journalID int, cur *SyncCursor) (int, error) {
	lines, maxWriteDate, err := fetchOdooJournalLinesForCacheFull(creds, uid, journalID)
	if err != nil {
		return 0, err
	}
	count, err := writeOdooJournalLinesCacheFile(journalID, lines)
	if err != nil {
		return 0, err
	}
	cur.LastWriteDate = maxWriteDate
	cur.Count = count
	_ = SaveSyncCursor(*cur)
	return count, nil
}

// journalCacheMatchesOdoo returns true when the in-memory `lines`
// slice has the same count and total balance as Odoo's server-side
// aggregate for the journal. Sums cents as int64 locally so float64
// accumulation drift doesn't false-positive against Odoo's exact
// NUMERIC sum (same trick verifyOdooJournalCacheFresh uses).
func journalCacheMatchesOdoo(creds *OdooCredentials, uid int, journalID int, lines []OdooCacheLine) bool {
	liveCount, liveBalance, err := odooJournalAggregate(creds, uid, journalID)
	if err != nil {
		// Aggregation failed — bias to "matches" so a transient
		// Odoo error doesn't trigger a full-fetch storm.
		return true
	}
	var cents int64
	for _, l := range lines {
		cents += int64(math.Round(l.Amount * 100))
	}
	cachedBalance := float64(cents) / 100.0
	return len(lines) == liveCount && cachedBalance == liveBalance
}

// fetchOdooJournalLinesSince returns lines on the journal whose
// write_date is strictly newer than sinceWriteDate (passed in Odoo's
// "2006-01-02 15:04:05" format). Also returns the max write_date seen
// in the result set, which the caller stamps onto the cursor.
func fetchOdooJournalLinesSince(creds *OdooCredentials, uid int, journalID int, sinceWriteDate string) ([]OdooCacheLine, string, error) {
	rows, err := odooSearchReadAllMapsLabeled(creds, uid, "account.bank.statement.line",
		[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
			[]interface{}{"write_date", ">", sinceWriteDate},
		},
		[]string{"id", "partner_id", "move_id", "unique_import_id", "date", "payment_ref", "amount", "narration", "is_reconciled", "write_date"},
		"write_date asc, id asc",
		fmt.Sprintf("Odoo journal #%d updates", journalID))
	if err != nil {
		return nil, "", err
	}
	if len(rows) == 0 {
		return nil, sinceWriteDate, nil
	}
	moveIDs := make([]int, 0, len(rows))
	for _, row := range rows {
		if moveID := odooFieldID(row["move_id"]); moveID > 0 {
			moveIDs = append(moveIDs, moveID)
		}
	}
	counterpartByMoveID, _ := fetchCounterpartMoveLinesByMoveID(creds, uid, moveIDs)

	maxWrite := sinceWriteDate
	lines := make([]OdooCacheLine, 0, len(rows))
	for _, row := range rows {
		narration := odooString(row["narration"])
		moveID := odooFieldID(row["move_id"])
		counterpart := counterpartByMoveID[moveID]
		line := OdooCacheLine{
			ID:             odooInt(row["id"]),
			MoveID:         moveID,
			PartnerID:      odooFieldID(row["partner_id"]),
			AccountID:      counterpart.AccountID,
			CounterpartID:  counterpart.LineID,
			Date:           odooString(row["date"]),
			PaymentRef:     odooString(row["payment_ref"]),
			UniqueImportID: odooString(row["unique_import_id"]),
			Amount:         odooFloat(row["amount"]),
			IsReconciled:   odooBool(row["is_reconciled"]),
			Narration:      narration,
			Metadata:       parseOdooLineNarration(narration),
		}
		if line.UniqueImportID == "" {
			line.UniqueImportID = metaString(line.Metadata, "uniqueImportId")
		}
		if wd := odooString(row["write_date"]); wd > maxWrite {
			maxWrite = wd
		}
		lines = append(lines, line)
	}
	return lines, maxWrite, nil
}

// odooJournalAggregate is a one-RPC server-side aggregation of a journal's
// statement lines. Used by the freshness check — replaces what used to be a
// full search_read of every line just to compute count + sum.
//
// On a 3,600-line Stripe journal this is ~50× faster than fetching every
// row, and the bytes-on-the-wire shrinks from ~hundreds of KB to a single
// JSON object.
func odooJournalAggregate(creds *OdooCredentials, uid int, journalID int) (count int, balance float64, err error) {
	result, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement.line", "read_group",
		[]interface{}{
			[]interface{}{[]interface{}{"journal_id", "=", journalID}},
			[]string{"journal_id", "amount:sum"},
			[]string{"journal_id"},
		},
		map[string]interface{}{"lazy": false})
	if err != nil {
		return 0, 0, err
	}
	var groups []struct {
		Amount float64 `json:"amount"`
		Count  int     `json:"__count"`
	}
	if err := json.Unmarshal(result, &groups); err != nil {
		return 0, 0, fmt.Errorf("parse read_group: %v", err)
	}
	if len(groups) == 0 {
		// Empty journal — Odoo returns no group rows. Treat as 0/0.
		return 0, 0, nil
	}
	return groups[0].Count, roundCents(groups[0].Amount), nil
}

// verifyOdooJournalCacheFresh reads the local journal-lines cache and asks
// Odoo for the journal's current count + balance (via one read_group RPC).
// Returns a clear error when they diverge. Push paths call this before any
// writes: a stale cache means we're planning against out-of-date target
// state, which would silently create duplicates or write against deleted
// lines. The error suggests `chb odoo pull` as the fix.
func verifyOdooJournalCacheFresh(creds *OdooCredentials, uid int, journalID int) error {
	Progress("verifying cache freshness")
	file, ok := loadLatestOdooJournalLinesFile(journalID)
	if !ok {
		return fmt.Errorf("no local cache for journal #%d — run `chb odoo pull` first", journalID)
	}
	// If the cache was just refreshed (e.g. by the pull phase of `chb sync`),
	// trust it and skip the live read_group RPC — that round-trip is what made
	// this step appear to hang.
	if t, perr := time.Parse(time.RFC3339, file.FetchedAt); perr == nil && time.Since(t) < journalCacheTrustWindow {
		return nil
	}
	cached := file.Lines
	// Sum cents as int64 — float64 accumulation drifts by ±1 cent over
	// hundreds of lines, which would falsely flag the cache as stale
	// against Odoo's exact server-side NUMERIC sum.
	var cachedCents int64
	for _, ln := range cached {
		cachedCents += int64(math.Round(ln.Amount * 100))
	}
	cachedBalance := float64(cachedCents) / 100.0

	liveCount, liveBalance, err := odooJournalAggregate(creds, uid, journalID)
	if err != nil {
		return fmt.Errorf("could not read journal #%d state from Odoo: %v", journalID, err)
	}

	if len(cached) != liveCount || cachedBalance != liveBalance {
		return fmt.Errorf("journal #%d is out of sync with local cache "+
			"(Odoo: %d lines / %s, local: %d lines / %s) — run `chb odoo pull` first",
			journalID,
			liveCount, fmtEUR(liveBalance),
			len(cached), fmtEUR(cachedBalance))
	}
	return nil
}

func writeOdooJournalLinesCacheFile(journalID int, lines []OdooCacheLine) (int, error) {
	sort.SliceStable(lines, func(i, j int) bool {
		if lines[i].Date == lines[j].Date {
			return lines[i].ID < lines[j].ID
		}
		return lines[i].Date < lines[j].Date
	})
	now := time.Now().In(BrusselsTZ())
	file := OdooJournalLinesFile{
		SchemaVersion: odooJournalLinesSchemaVersion,
		Provider:      odoosource.Source,
		FetchedAt:     time.Now().UTC().Format(time.RFC3339),
		JournalID:     journalID,
		Count:         len(lines),
		Lines:         lines,
	}
	name := journalLinesCacheName(journalID)
	if err := odoosource.WriteJSON(DataDir(), "latest", "", file, "journals", name); err != nil {
		return 0, err
	}
	if err := odoosource.WriteJSON(DataDir(), now.Format("2006"), now.Format("01"), file, "journals", name); err != nil {
		return 0, err
	}
	return len(lines), nil
}

// fetchOdooJournalLinesForCacheFull does the same paginated full fetch
// as fetchOdooJournalLinesForCache but also returns the max write_date
// across the result set so the caller can seed the watermark cursor.
func fetchOdooJournalLinesForCacheFull(creds *OdooCredentials, uid int, journalID int) ([]OdooCacheLine, string, error) {
	rows, err := odooSearchReadAllMapsLabeled(creds, uid, "account.bank.statement.line",
		[]interface{}{[]interface{}{"journal_id", "=", journalID}},
		[]string{"id", "partner_id", "move_id", "unique_import_id", "date", "payment_ref", "amount", "narration", "is_reconciled", "write_date"},
		"date asc, id asc",
		fmt.Sprintf("Odoo journal #%d lines", journalID))
	if err != nil {
		return nil, "", err
	}
	moveIDs := make([]int, 0, len(rows))
	for _, row := range rows {
		if moveID := odooFieldID(row["move_id"]); moveID > 0 {
			moveIDs = append(moveIDs, moveID)
		}
	}
	counterpartByMoveID, _ := fetchCounterpartMoveLinesByMoveID(creds, uid, moveIDs)
	maxWrite := ""
	lines := make([]OdooCacheLine, 0, len(rows))
	for _, row := range rows {
		narration := odooString(row["narration"])
		moveID := odooFieldID(row["move_id"])
		counterpart := counterpartByMoveID[moveID]
		line := OdooCacheLine{
			ID:             odooInt(row["id"]),
			MoveID:         moveID,
			PartnerID:      odooFieldID(row["partner_id"]),
			AccountID:      counterpart.AccountID,
			CounterpartID:  counterpart.LineID,
			Date:           odooString(row["date"]),
			PaymentRef:     odooString(row["payment_ref"]),
			UniqueImportID: odooString(row["unique_import_id"]),
			Amount:         odooFloat(row["amount"]),
			IsReconciled:   odooBool(row["is_reconciled"]),
			Narration:      narration,
			Metadata:       parseOdooLineNarration(narration),
		}
		if line.UniqueImportID == "" {
			line.UniqueImportID = metaString(line.Metadata, "uniqueImportId")
		}
		if wd := odooString(row["write_date"]); wd > maxWrite {
			maxWrite = wd
		}
		lines = append(lines, line)
	}
	return lines, maxWrite, nil
}

func fetchOdooJournalLinesForCache(creds *OdooCredentials, uid int, journalID int) ([]OdooCacheLine, error) {
	rows, err := odooSearchReadAllMapsLabeled(creds, uid, "account.bank.statement.line",
		[]interface{}{[]interface{}{"journal_id", "=", journalID}},
		[]string{"id", "partner_id", "move_id", "unique_import_id", "date", "payment_ref", "amount", "narration", "is_reconciled"},
		"date asc, id asc",
		fmt.Sprintf("Odoo journal #%d lines", journalID))
	if err != nil {
		return nil, err
	}
	lines := make([]OdooCacheLine, 0, len(rows))
	moveIDs := make([]int, 0, len(rows))
	for _, row := range rows {
		if moveID := odooFieldID(row["move_id"]); moveID > 0 {
			moveIDs = append(moveIDs, moveID)
		}
	}
	counterpartByMoveID, _ := fetchCounterpartMoveLinesByMoveID(creds, uid, moveIDs)
	for _, row := range rows {
		narration := odooString(row["narration"])
		moveID := odooFieldID(row["move_id"])
		counterpart := counterpartByMoveID[moveID]
		line := OdooCacheLine{
			ID:             odooInt(row["id"]),
			MoveID:         moveID,
			PartnerID:      odooFieldID(row["partner_id"]),
			AccountID:      counterpart.AccountID,
			CounterpartID:  counterpart.LineID,
			Date:           odooString(row["date"]),
			PaymentRef:     odooString(row["payment_ref"]),
			UniqueImportID: odooString(row["unique_import_id"]),
			Amount:         odooFloat(row["amount"]),
			IsReconciled:   odooBool(row["is_reconciled"]),
			Narration:      narration,
			Metadata:       parseOdooLineNarration(narration),
		}
		if line.UniqueImportID == "" {
			line.UniqueImportID = metaString(line.Metadata, "uniqueImportId")
		}
		lines = append(lines, line)
	}
	return lines, nil
}

func loadLatestOdooJournalLinesCache(journalID int) ([]OdooCacheLine, bool) {
	file, ok := loadLatestOdooJournalLinesFile(journalID)
	if !ok {
		return nil, false
	}
	return file.Lines, true
}

// loadLatestOdooJournalLinesFile returns the full cache file (including
// FetchedAt) for a journal, or (_, false) when missing/unparseable.
func loadLatestOdooJournalLinesFile(journalID int) (OdooJournalLinesFile, bool) {
	path := odoosource.Path(DataDir(), "latest", "", "journals", journalLinesCacheName(journalID))
	data, err := os.ReadFile(path)
	if err != nil {
		return OdooJournalLinesFile{}, false
	}
	var file OdooJournalLinesFile
	if err := json.Unmarshal(data, &file); err != nil || file.JournalID != journalID {
		return OdooJournalLinesFile{}, false
	}
	return file, true
}

// journalCacheTrustWindow is how recently the journal-lines cache must have been
// fetched for the pre-push freshness check to trust it WITHOUT a live Odoo
// round-trip. A combined `chb sync` refreshes the cache (pull phase) seconds
// before pushing, so this skips a redundant — and potentially slow — read_group
// RPC that otherwise made the KBC/journal push appear to hang at "verifying
// cache freshness". A standalone push against an old cache still verifies live.
const journalCacheTrustWindow = 10 * time.Minute

func updateOdooJournalLinesCachePartners(journalID int, partnersByLineID map[int]int) error {
	if len(partnersByLineID) == 0 {
		return nil
	}
	lines, ok := loadLatestOdooJournalLinesCache(journalID)
	if !ok {
		return nil
	}
	changed := false
	for i := range lines {
		if partnerID, ok := partnersByLineID[lines[i].ID]; ok && partnerID > 0 && lines[i].PartnerID != partnerID {
			lines[i].PartnerID = partnerID
			changed = true
		}
	}
	if !changed {
		return nil
	}
	_, err := writeOdooJournalLinesCacheFile(journalID, lines)
	return err
}

func updateOdooJournalLinesCacheMetadata(journalID int, updatesByLineID map[int]stripeOdooDesiredLine) error {
	if len(updatesByLineID) == 0 {
		return nil
	}
	lines, ok := loadLatestOdooJournalLinesCache(journalID)
	if !ok {
		return nil
	}
	changed := false
	for i := range lines {
		update, ok := updatesByLineID[lines[i].ID]
		if !ok {
			continue
		}
		if update.PaymentRef != "" && lines[i].PaymentRef != update.PaymentRef {
			lines[i].PaymentRef = update.PaymentRef
			changed = true
		}
		if update.Narration != "" && lines[i].Narration != update.Narration {
			lines[i].Narration = update.Narration
			lines[i].Metadata = parseOdooLineNarration(update.Narration)
			changed = true
		}
	}
	if !changed {
		return nil
	}
	_, err := writeOdooJournalLinesCacheFile(journalID, lines)
	return err
}

func updateOdooJournalLinesCacheAccounts(journalID int, accountsByMoveID map[int]int) error {
	if len(accountsByMoveID) == 0 {
		return nil
	}
	lines, ok := loadLatestOdooJournalLinesCache(journalID)
	if !ok {
		return nil
	}
	changed := false
	for i := range lines {
		if accountID, ok := accountsByMoveID[lines[i].MoveID]; ok && accountID > 0 && lines[i].AccountID != accountID {
			lines[i].AccountID = accountID
			changed = true
		}
	}
	if !changed {
		return nil
	}
	_, err := writeOdooJournalLinesCacheFile(journalID, lines)
	return err
}

func updateOdooJournalLinesCacheCounterparts(journalID int, counterpartsByMoveID map[int]counterpartMoveLineInfo) error {
	if len(counterpartsByMoveID) == 0 {
		return nil
	}
	lines, ok := loadLatestOdooJournalLinesCache(journalID)
	if !ok {
		return nil
	}
	changed := false
	for i := range lines {
		info, ok := counterpartsByMoveID[lines[i].MoveID]
		if !ok {
			continue
		}
		if info.AccountID > 0 && lines[i].AccountID != info.AccountID {
			lines[i].AccountID = info.AccountID
			changed = true
		}
		if info.LineID > 0 && lines[i].CounterpartID != info.LineID {
			lines[i].CounterpartID = info.LineID
			changed = true
		}
	}
	if !changed {
		return nil
	}
	_, err := writeOdooJournalLinesCacheFile(journalID, lines)
	return err
}

func parseOdooLineNarration(narration string) map[string]interface{} {
	if narration == "" {
		return nil
	}
	var meta map[string]interface{}
	if err := json.Unmarshal([]byte(narration), &meta); err != nil {
		return nil
	}
	return meta
}

func journalLinesCacheName(journalID int) string {
	return strconv.Itoa(journalID) + ".json"
}

func odooJournalLinesCachePath(journalID int) string {
	return filepath.Join("providers", "odoo", "journals", journalLinesCacheName(journalID))
}
