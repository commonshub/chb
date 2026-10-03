package cmd

// Chart of accounts and yearly ledger balances from Odoo (read-only), part
// of the Odoo pull. Raw archives:
//
//	latest/providers/odoo/<db>/accounts-chart.json      every account.account (+ groups, labels per language)
//	YYYY/12/providers/odoo/<db>/ledger-balances.json    per account: opening, debit, credit, closing for calendar year YYYY
//
// Opening balances: balance-sheet accounts (classes 1–5) carry everything
// posted before 1 January; income and expense accounts (classes 6–7) start
// at the beginning of the fiscal year that contains 1 January (annual
// accounts periods; 1 January by default — FY "2023" ran from 1 July 2023,
// so 2024 opens with July–December 2023). cmd/ledger_generate.go publishes.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"time"

	odoosource "github.com/CommonsHub/chb/providers/odoo"
)

type ChartAccount struct {
	ID        int               `json:"id"`
	Code      string            `json:"code"`
	Name      string            `json:"name"`
	Labels    map[string]string `json:"labels,omitempty"` // per Odoo language (fr_BE, nl_BE, en_GB)
	Type      string            `json:"type"`
	Reconcile bool              `json:"reconcile"`
	Active    bool              `json:"active"`
	GroupCode string            `json:"groupCode,omitempty"` // Odoo account.group prefix, e.g. "6130"
	GroupName string            `json:"groupName,omitempty"`
}

type ChartFile struct {
	FetchedAt string         `json:"fetchedAt"`
	Languages []string       `json:"languages"`
	Accounts  []ChartAccount `json:"accounts"`
}

type LedgerRow struct {
	AccountID int     `json:"accountId"`
	Code      string  `json:"code"`
	Name      string  `json:"name"`
	Opening   float64 `json:"opening"`
	Debit     float64 `json:"debit"`
	Credit    float64 `json:"credit"`
	Closing   float64 `json:"closing"`
}

type LedgerFile struct {
	Year        string      `json:"year"`
	FetchedAt   string      `json:"fetchedAt"`
	PeriodStart string      `json:"periodStart"`
	PeriodEnd   string      `json:"periodEnd"`
	FiscalStart string      `json:"fiscalStart"` // where income/expense openings start
	Accounts    []LedgerRow `json:"accounts"`
}

func chartRawPath(dataDir string) string {
	return filepath.Join(dataDir, "latest", odoosource.RelPath("accounts-chart.json"))
}

func ledgerRawPath(dataDir, year string) string {
	return odoosource.Path(dataDir, year, "12", "ledger-balances.json")
}

// fiscalStartFor: the start of the fiscal year containing 1 January of
// year, from the annual accounts periods (default: 1 January).
func fiscalStartFor(dataDir, year string) string {
	jan1 := year + "-01-01"
	for _, f := range loadAnnualFilings(dataDir) {
		if f.PeriodStart <= jan1 && f.PeriodEnd >= jan1 {
			return f.PeriodStart
		}
	}
	return jan1
}

func odooReadGroupByAccount(creds *OdooCredentials, uid int, domain []interface{}) (map[int][3]float64, error) {
	res, err := odooExec(creds.URL, creds.DB, uid, creds.Password, "account.move.line", "read_group",
		[]interface{}{domain, []string{"debit:sum", "credit:sum", "balance:sum"}, []string{"account_id"}},
		map[string]interface{}{"lazy": false})
	if err != nil {
		return nil, err
	}
	var groups []map[string]interface{}
	if err := json.Unmarshal(res, &groups); err != nil {
		return nil, err
	}
	out := map[int][3]float64{}
	for _, g := range groups {
		id := odooFieldID(g["account_id"])
		if id == 0 {
			continue
		}
		out[id] = [3]float64{odooFloat(g["debit"]), odooFloat(g["credit"]), odooFloat(g["balance"])}
	}
	return out, nil
}

// syncChartAndLedger fetches the chart and every year's ledger balances.
// Returns (accounts in the chart, years written).
func syncChartAndLedger(creds *OdooCredentials, uid int, dataDir string) (int, int, error) {
	langs := []string{}
	if res, err := odooExec(creds.URL, creds.DB, uid, creds.Password, "res.lang", "search_read",
		[]interface{}{[]interface{}{[]interface{}{"active", "=", true}}}, map[string]interface{}{"fields": []string{"code"}}); err == nil {
		var rows []map[string]interface{}
		_ = json.Unmarshal(res, &rows)
		for _, r := range rows {
			langs = append(langs, odooString(r["code"]))
		}
	}
	sort.Strings(langs)
	fields := []string{"code", "name", "account_type", "reconcile", "active", "group_id"}
	domain := []interface{}{[]interface{}{"active", "in", []interface{}{true, false}}}
	rows, err := odooSearchReadAllMaps(creds, uid, "account.account", domain, fields, "code")
	if err != nil {
		return 0, 0, err
	}
	groupPrefix := map[int]string{}
	if res, err := odooExec(creds.URL, creds.DB, uid, creds.Password, "account.group", "search_read",
		[]interface{}{[]interface{}{}}, map[string]interface{}{"fields": []string{"code_prefix_start", "name"}}); err == nil {
		var gs []map[string]interface{}
		_ = json.Unmarshal(res, &gs)
		for _, g := range gs {
			groupPrefix[odooInt(g["id"])] = odooString(g["code_prefix_start"])
		}
	}
	chart := ChartFile{Languages: langs}
	byID := map[int]*ChartAccount{}
	for _, r := range rows {
		a := ChartAccount{
			ID: odooInt(r["id"]), Code: odooString(r["code"]), Name: odooString(r["name"]),
			Type: odooString(r["account_type"]), Reconcile: odooBool(r["reconcile"]), Active: odooBool(r["active"]),
			GroupCode: groupPrefix[odooFieldID(r["group_id"])], GroupName: odooFieldName(r["group_id"]),
		}
		chart.Accounts = append(chart.Accounts, a)
	}
	for i := range chart.Accounts {
		byID[chart.Accounts[i].ID] = &chart.Accounts[i]
	}
	for _, lang := range langs {
		res, err := odooExec(creds.URL, creds.DB, uid, creds.Password, "account.account", "search_read",
			[]interface{}{domain}, map[string]interface{}{"fields": []string{"name"}, "context": map[string]interface{}{"lang": lang}})
		if err != nil {
			continue
		}
		var ls []map[string]interface{}
		_ = json.Unmarshal(res, &ls)
		for _, l := range ls {
			if a := byID[odooInt(l["id"])]; a != nil {
				if a.Labels == nil {
					a.Labels = map[string]string{}
				}
				a.Labels[lang] = odooString(l["name"])
			}
		}
	}
	if err := writeIfChanged(chartRawPath(dataDir), chart, func(v interface{}) { v.(*ChartFile).FetchedAt = "" }, &ChartFile{}, func() { chart.FetchedAt = time.Now().UTC().Format(time.RFC3339) }); err != nil {
		return 0, 0, err
	}

	// Years with posted entries.
	res, err := odooExec(creds.URL, creds.DB, uid, creds.Password, "account.move.line", "read_group",
		[]interface{}{[]interface{}{[]interface{}{"parent_state", "=", "posted"}}, []string{"date:min"}, []string{}},
		map[string]interface{}{"lazy": false})
	if err != nil {
		return len(chart.Accounts), 0, err
	}
	var minRow []map[string]interface{}
	_ = json.Unmarshal(res, &minRow)
	first := time.Now().Year()
	if len(minRow) > 0 {
		if d := odooString(minRow[0]["date"]); len(d) >= 4 {
			if y, err := strconv.Atoi(d[:4]); err == nil {
				first = y
			}
		}
	}
	written := 0
	for y := first; y <= time.Now().Year(); y++ {
		year := strconv.Itoa(y)
		start, end := year+"-01-01", year+"-12-31"
		fiscal := fiscalStartFor(dataDir, year)
		posted := []interface{}{"parent_state", "=", "posted"}
		period, err := odooReadGroupByAccount(creds, uid, []interface{}{posted, []interface{}{"date", ">=", start}, []interface{}{"date", "<=", end}})
		if err != nil {
			return len(chart.Accounts), written, err
		}
		before, err := odooReadGroupByAccount(creds, uid, []interface{}{posted, []interface{}{"date", "<", start}})
		if err != nil {
			return len(chart.Accounts), written, err
		}
		plBefore := map[int][3]float64{}
		if fiscal < start {
			if plBefore, err = odooReadGroupByAccount(creds, uid, []interface{}{posted, []interface{}{"date", ">=", fiscal}, []interface{}{"date", "<", start}}); err != nil {
				return len(chart.Accounts), written, err
			}
		}
		lf := LedgerFile{Year: year, PeriodStart: start, PeriodEnd: end, FiscalStart: fiscal}
		ids := map[int]bool{}
		for id := range period {
			ids[id] = true
		}
		for id := range before {
			ids[id] = true
		}
		for id := range ids {
			a := byID[id]
			if a == nil {
				continue
			}
			opening := before[id][2]
			if len(a.Code) > 0 && (a.Code[0] == '6' || a.Code[0] == '7') {
				opening = plBefore[id][2]
			}
			p := period[id]
			row := LedgerRow{AccountID: id, Code: a.Code, Name: a.Name, Opening: round2(opening), Debit: round2(p[0]), Credit: round2(p[1])}
			row.Closing = round2(row.Opening + p[2])
			if row.Opening == 0 && row.Debit == 0 && row.Credit == 0 {
				continue
			}
			lf.Accounts = append(lf.Accounts, row)
		}
		sort.Slice(lf.Accounts, func(i, j int) bool { return lf.Accounts[i].Code < lf.Accounts[j].Code })
		if len(lf.Accounts) == 0 {
			continue
		}
		changed, err := writeLedgerIfChanged(ledgerRawPath(dataDir, year), lf)
		if err != nil {
			return len(chart.Accounts), written, err
		}
		if changed {
			written++
		}
	}
	return len(chart.Accounts), written, nil
}

func writeLedgerIfChanged(path string, lf LedgerFile) (bool, error) {
	if data, err := os.ReadFile(path); err == nil {
		var prev LedgerFile
		if json.Unmarshal(data, &prev) == nil {
			prev.FetchedAt = ""
			if reflect.DeepEqual(prev, lf) {
				return false, nil
			}
		}
	}
	lf.FetchedAt = time.Now().UTC().Format(time.RFC3339)
	data, _ := json.MarshalIndent(lf, "", "  ")
	return true, writeDataFile(path, data)
}

// writeIfChanged writes v (after stamp()) unless the file holds the same
// content once clear() blanks its volatile fields.
func writeIfChanged(path string, v interface{}, clear func(interface{}), prevPtr interface{}, stamp func()) error {
	if data, err := os.ReadFile(path); err == nil && json.Unmarshal(data, prevPtr) == nil {
		clear(prevPtr)
		cur, _ := json.Marshal(v)
		old, _ := json.Marshal(prevPtr)
		if string(cur) == string(old) {
			return nil
		}
	}
	stamp()
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return writeDataFile(path, data)
}

// OdooLedgerSync is the pull step (`chb odoo pull`, `chb pull`).
func OdooLedgerSync(args []string) (string, error) {
	creds, err := ResolveOdooCredentials()
	if err != nil {
		return "", err
	}
	uid, err := odooAuth(creds.URL, creds.DB, creds.Login, creds.Password)
	if err != nil || uid == 0 {
		return "", fmt.Errorf("Odoo authentication failed: %v", err)
	}
	n, years, err := syncChartAndLedger(creds, uid, DataDir())
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%d accounts, %s updated", n, Pluralize(years, "year", "")), nil
}
