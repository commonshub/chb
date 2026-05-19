package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	odoosource "github.com/CommonsHub/chb/providers/odoo"
)

const odooPartnersSchemaVersion = 2

type OdooPartnersFile struct {
	SchemaVersion int                   `json:"schemaVersion"`
	Provider      string                `json:"provider"`
	FetchedAt     string                `json:"fetchedAt"`
	Count         int                   `json:"count"`
	Partners      []OdooPartner         `json:"partners"`
	Categories    []OdooPartnerCategory `json:"categories,omitempty"`
}

type OdooPartner struct {
	ID                  int    `json:"id"`
	Name                string `json:"name,omitempty"`
	Email               string `json:"email,omitempty"`
	CommercialPartnerID int    `json:"commercialPartnerId,omitempty"`
	ParentID            int    `json:"parentId,omitempty"`
	IsCompany           bool   `json:"isCompany,omitempty"`
	Active              bool   `json:"active"`
	VAT                 string `json:"vat,omitempty"`
	Phone               string `json:"phone,omitempty"`
	Mobile              string `json:"mobile,omitempty"`
	WriteDate           string `json:"writeDate,omitempty"`
	CategoryIDs         []int  `json:"categoryIds,omitempty"`
}

type OdooPartnerCategory struct {
	ID   int    `json:"id"`
	Name string `json:"name,omitempty"`
}

type odooPartnerIndex struct {
	byEmail     map[string][]OdooPartner
	byName      map[string][]OdooPartner
	byID        map[int]OdooPartner
	categoryIDs map[string]int
}

// OdooPartnersSync fetches Odoo partners into both latest/providers/odoo and
// the current monthly providers/odoo archive. This lets dry-runs summarize
// partner matches without per-line Odoo partner RPCs.
func OdooPartnersSync(args []string) (int, error) {
	return odooPartnersSync(args, true)
}

func refreshOdooPartnersCache(args []string) (int, error) {
	return odooPartnersSync(args, false)
}

func odooPartnersSync(args []string, printSummary bool) (int, error) {
	_ = args
	creds, err := ResolveOdooCredentials()
	if err != nil {
		return 0, err
	}
	uid, err := odooAuth(creds.URL, creds.DB, creds.Login, creds.Password)
	if err != nil {
		return 0, err
	}
	if uid == 0 {
		return 0, fmt.Errorf("Odoo authentication failed")
	}

	odooLog("\n%s👥 Syncing Odoo partners%s\n", Fmt.Bold, Fmt.Reset)
	rows, err := odooSearchReadAllMapsLabeled(creds, uid, "res.partner", []interface{}{}, []string{
		"id", "name", "email", "commercial_partner_id", "parent_id", "is_company",
		"active", "vat", "phone", "mobile", "write_date", "category_id",
	}, "id asc", "Odoo partners")
	if err != nil {
		if printSummary {
			odooSyncLine("partners", odooItemSyncStatus(0, "partner", fmt.Sprintf("issue: %v", err)))
		}
		return 0, err
	}
	categoryRows, err := odooSearchReadAllMaps(creds, uid, "res.partner.category", []interface{}{}, []string{
		"id", "name",
	}, "id asc")
	if err != nil {
		if printSummary {
			odooSyncLine("partners", odooItemSyncStatus(0, "partner", fmt.Sprintf("issue: %v", err)))
		}
		return 0, err
	}

	partners := make([]OdooPartner, 0, len(rows))
	for _, row := range rows {
		partners = append(partners, OdooPartner{
			ID:                  odooInt(row["id"]),
			Name:                odooString(row["name"]),
			Email:               odooString(row["email"]),
			CommercialPartnerID: odooFieldID(row["commercial_partner_id"]),
			ParentID:            odooFieldID(row["parent_id"]),
			IsCompany:           odooBool(row["is_company"]),
			Active:              odooBoolDefault(row["active"], true),
			VAT:                 odooString(row["vat"]),
			Phone:               odooString(row["phone"]),
			Mobile:              odooString(row["mobile"]),
			WriteDate:           odooString(row["write_date"]),
			CategoryIDs:         odooIDList(row["category_id"]),
		})
	}
	sort.Slice(partners, func(i, j int) bool { return partners[i].ID < partners[j].ID })
	categories := make([]OdooPartnerCategory, 0, len(categoryRows))
	for _, row := range categoryRows {
		categories = append(categories, OdooPartnerCategory{
			ID:   odooInt(row["id"]),
			Name: odooString(row["name"]),
		})
	}
	sort.Slice(categories, func(i, j int) bool { return categories[i].ID < categories[j].ID })

	now := time.Now().In(BrusselsTZ())
	file := OdooPartnersFile{
		SchemaVersion: odooPartnersSchemaVersion,
		Provider:      odoosource.Source,
		FetchedAt:     time.Now().UTC().Format(time.RFC3339),
		Count:         len(partners),
		Partners:      partners,
		Categories:    categories,
	}
	if err := odoosource.WriteJSON(DataDir(), "latest", "", file, odoosource.PartnersFile); err != nil {
		return 0, err
	}
	year, month := now.Format("2006"), now.Format("01")
	if err := odoosource.WriteJSON(DataDir(), year, month, file, odoosource.PartnersFile); err != nil {
		return 0, err
	}
	if printSummary {
		odooSyncLine("partners", odooItemSyncStatus(len(partners), "partner", ""))
	}
	return len(partners), nil
}

func loadLatestOdooPartnerIndex(dataDir string) *odooPartnerIndex {
	return loadOdooPartnerIndex(odoosource.Path(dataDir, "latest", "", odoosource.PartnersFile))
}

func loadLatestOdooPartnersFile(dataDir string) (OdooPartnersFile, bool) {
	data, err := os.ReadFile(odoosource.Path(dataDir, "latest", "", odoosource.PartnersFile))
	if err != nil {
		return OdooPartnersFile{}, false
	}
	var file OdooPartnersFile
	if err := json.Unmarshal(data, &file); err != nil {
		return OdooPartnersFile{}, false
	}
	return file, true
}

func loadOdooPartnerIndex(path string) *odooPartnerIndex {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var file OdooPartnersFile
	if err := json.Unmarshal(data, &file); err != nil {
		return nil
	}
	idx := &odooPartnerIndex{
		byEmail:     map[string][]OdooPartner{},
		byName:      map[string][]OdooPartner{},
		byID:        map[int]OdooPartner{},
		categoryIDs: map[string]int{},
	}
	for _, category := range file.Categories {
		if category.ID > 0 {
			idx.categoryIDs[normalizePartnerName(category.Name)] = category.ID
		}
	}
	for _, partner := range file.Partners {
		if !partner.Active {
			continue
		}
		if partner.ID > 0 {
			idx.byID[partner.ID] = partner
		}
		if email := normalizePartnerEmail(partner.Email); email != "" {
			idx.byEmail[email] = append(idx.byEmail[email], partner)
		}
		if name := normalizePartnerName(partner.Name); name != "" {
			idx.byName[name] = append(idx.byName[name], partner)
		}
	}
	return idx
}

func localPartnerHasCollectiveTag(idx *odooPartnerIndex, partnerID int, collective string) bool {
	if idx == nil || partnerID <= 0 {
		return false
	}
	tagName := normalizePartnerName(odooPartnerCollectiveTagName(collective))
	tagID := idx.categoryIDs[tagName]
	if tagID <= 0 {
		return false
	}
	partner, ok := idx.byID[partnerID]
	if !ok {
		return false
	}
	for _, id := range partner.CategoryIDs {
		if id == tagID {
			return true
		}
	}
	return false
}

func resolveOdooPartnerFromLocalIndex(idx *odooPartnerIndex, name, email string, cache map[string]int, st *syncStats) int {
	if idx == nil || (name == "" && email == "") {
		return 0
	}
	cacheKey := normalizePartnerEmail(email)
	if cacheKey == "" {
		cacheKey = normalizePartnerName(name)
	}
	if cacheKey == "" {
		return 0
	}
	if id, ok := cache[cacheKey]; ok {
		return id
	}

	if emailKey := normalizePartnerEmail(email); emailKey != "" {
		matches := idx.byEmail[emailKey]
		if len(matches) == 1 {
			cache[cacheKey] = matches[0].ID
			if st != nil {
				st.PartnersMatched++
			}
			return matches[0].ID
		}
		if len(matches) > 1 {
			selected := oldestOdooPartner(matches)
			cache[cacheKey] = selected.ID
			if st != nil {
				st.PartnersMatched++
				st.recordPartnerMergeSuggestion(name, email, selected.ID, odooPartnerIDs(matches))
			}
			return selected.ID
		}
	}

	if nameKey := normalizePartnerName(name); nameKey != "" {
		matches := idx.byName[nameKey]
		if len(matches) == 1 {
			cache[cacheKey] = matches[0].ID
			if st != nil {
				st.PartnersMatched++
			}
			return matches[0].ID
		}
		if len(matches) > 1 {
			selected := oldestOdooPartner(matches)
			cache[cacheKey] = selected.ID
			if st != nil {
				st.PartnersMatched++
				st.recordPartnerMergeSuggestion(name, email, selected.ID, odooPartnerIDs(matches))
			}
			return selected.ID
		}
	}

	cache[cacheKey] = 0
	if st != nil && strings.TrimSpace(name) != "" {
		st.PartnersCreated++
	}
	return 0
}

func lookupOdooPartnerFromLocalIndex(idx *odooPartnerIndex, name, email string) (id int, ambiguous bool) {
	if idx == nil || (name == "" && email == "") {
		return 0, false
	}
	if emailKey := normalizePartnerEmail(email); emailKey != "" {
		matches := idx.byEmail[emailKey]
		if len(matches) == 1 {
			return matches[0].ID, false
		}
		if len(matches) > 1 {
			return oldestOdooPartner(matches).ID, true
		}
	}
	if nameKey := normalizePartnerName(name); nameKey != "" {
		matches := idx.byName[nameKey]
		if len(matches) == 1 {
			return matches[0].ID, false
		}
		if len(matches) > 1 {
			return oldestOdooPartner(matches).ID, true
		}
	}
	return 0, false
}

func localPartnerIndexCandidateIDs(idx *odooPartnerIndex, name, email string) []int {
	if idx == nil {
		return nil
	}
	if emailKey := normalizePartnerEmail(email); emailKey != "" {
		if matches := idx.byEmail[emailKey]; len(matches) > 1 {
			return odooPartnerIDs(matches)
		}
	}
	if nameKey := normalizePartnerName(name); nameKey != "" {
		if matches := idx.byName[nameKey]; len(matches) > 1 {
			return odooPartnerIDs(matches)
		}
	}
	return nil
}

func oldestOdooPartner(matches []OdooPartner) OdooPartner {
	if len(matches) == 0 {
		return OdooPartner{}
	}
	selected := matches[0]
	for _, partner := range matches[1:] {
		if partner.ID > 0 && (selected.ID == 0 || partner.ID < selected.ID) {
			selected = partner
		}
	}
	return selected
}

func odooPartnerIDs(matches []OdooPartner) []int {
	ids := make([]int, 0, len(matches))
	seen := map[int]bool{}
	for _, partner := range matches {
		if partner.ID <= 0 || seen[partner.ID] {
			continue
		}
		seen[partner.ID] = true
		ids = append(ids, partner.ID)
	}
	sort.Ints(ids)
	return ids
}

func normalizePartnerEmail(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

func normalizePartnerName(s string) string {
	return strings.ToLower(strings.Join(strings.Fields(s), " "))
}

func odooBoolDefault(v interface{}, fallback bool) bool {
	if b, ok := v.(bool); ok {
		return b
	}
	return fallback
}
