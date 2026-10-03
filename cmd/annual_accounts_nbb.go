package cmd

// The NBB register (Central Balance Sheet Office): which annual accounts of
// the association are published. Read-only, once a day, during `chb pull`.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	aa "github.com/CommonsHub/chb/providers/annualaccounts"
)

const defaultEnterpriseNumber = "0804.505.132"

// nbbDepositsURL lists an enterprise's published deposits.
var nbbDepositsURL = "https://consult.cbso.nbb.be/api/rs-consult/published-deposits?page=0&size=50&sort=periodEndDate,desc&enterpriseNumber="

// NBBRegisterFile is latest/providers/annual-accounts/nbb-register.json.
type NBBRegisterFile struct {
	CheckedAt        string       `json:"checkedAt"`
	EnterpriseNumber string       `json:"enterpriseNumber"`
	Deposits         []NBBDeposit `json:"deposits"`
}

type NBBDeposit struct {
	ID                  string `json:"id"`
	Reference           string `json:"reference"`
	PeriodStartDate     string `json:"periodStartDate"`
	PeriodEndDate       string `json:"periodEndDate"`
	DepositDate         string `json:"depositDate"`
	GeneralAssemblyDate string `json:"generalAssemblyApprovalDate"`
	ModelName           string `json:"modelName"`
	Language            string `json:"language"`
	Type                string `json:"type"`
}

func nbbRegisterPath(dataDir string) string {
	return filepath.Join(dataDir, "latest", "providers", aa.Source, "nbb-register.json")
}

func digitsOnly(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// nbbEnterpriseURL is the association's page on the NBB consult site.
func nbbEnterpriseURL(number string) string {
	return "https://consult.cbso.nbb.be/consult-enterprise/" + digitsOnly(number)
}

func loadNBBRegister(dataDir string) *NBBRegisterFile {
	data, err := os.ReadFile(nbbRegisterPath(dataDir))
	if err != nil {
		return nil
	}
	var r NBBRegisterFile
	if json.Unmarshal(data, &r) != nil {
		return nil
	}
	return &r
}

// refreshNBBRegister fetches the published deposits at most once a day
// (force: always). Returns how many deposits the register lists, or -1
// when it was not refreshed.
func refreshNBBRegister(dataDir string, force bool) (int, error) {
	number := defaultEnterpriseNumber
	for _, f := range loadAnnualFilings(dataDir) {
		if f.EnterpriseNumber != "" {
			number = f.EnterpriseNumber
		}
	}
	if prev := loadNBBRegister(dataDir); prev != nil && !force {
		if t, err := time.Parse(time.RFC3339, prev.CheckedAt); err == nil && time.Since(t) < 23*time.Hour && prev.EnterpriseNumber == number {
			return -1, nil
		}
	}
	client := &http.Client{Timeout: 30 * time.Second}
	req, _ := http.NewRequest("GET", nbbDepositsURL+digitsOnly(number), nil)
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	var body struct {
		Content []NBBDeposit `json:"content"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return 0, err
	}
	out := NBBRegisterFile{CheckedAt: time.Now().UTC().Format(time.RFC3339), EnterpriseNumber: number, Deposits: body.Content}
	if out.Deposits == nil {
		out.Deposits = []NBBDeposit{}
	}
	data, _ := json.MarshalIndent(out, "", "  ")
	return len(out.Deposits), writeDataFile(nbbRegisterPath(dataDir), data)
}

// depositFor finds the register deposit of a fiscal period (by end date).
func (r *NBBRegisterFile) depositFor(periodEnd string) *NBBDeposit {
	if r == nil {
		return nil
	}
	var best *NBBDeposit
	for i := range r.Deposits {
		d := &r.Deposits[i]
		if strings.HasPrefix(d.PeriodEndDate, periodEnd) && (best == nil || d.DepositDate > best.DepositDate) {
			best = d
		}
	}
	return best
}
