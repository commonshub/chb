package cmd

import (
	"encoding/json"
	"fmt"
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
	Narration      string                 `json:"narration,omitempty"`
	Metadata       map[string]interface{} `json:"metadata,omitempty"`
}

func writeOdooJournalLinesCache(creds *OdooCredentials, uid int, journalID int) (int, error) {
	lines, err := fetchOdooJournalLinesForCache(creds, uid, journalID)
	if err != nil {
		return 0, err
	}
	return writeOdooJournalLinesCacheFile(journalID, lines)
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

func fetchOdooJournalLinesForCache(creds *OdooCredentials, uid int, journalID int) ([]OdooCacheLine, error) {
	rows, err := odooSearchReadAllMapsLabeled(creds, uid, "account.bank.statement.line",
		[]interface{}{[]interface{}{"journal_id", "=", journalID}},
		[]string{"id", "partner_id", "move_id", "unique_import_id", "date", "payment_ref", "amount", "narration"},
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
	path := odoosource.Path(DataDir(), "latest", "", "journals", journalLinesCacheName(journalID))
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}
	var file OdooJournalLinesFile
	if err := json.Unmarshal(data, &file); err != nil || file.JournalID != journalID {
		return nil, false
	}
	return file.Lines, true
}

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
