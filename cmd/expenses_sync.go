package cmd

// Odoo expense reports (hr.expense) → YYYY/MM/providers/odoo/<db>/expenses.json.
//
// Expenses someone paid out of pocket and claims back. Once posted, Odoo
// books each one as a vendor bill to the employee (it is then in the bill
// cache too); this archive adds what the bill lacks — that it is a
// reimbursement, the expense product, and claims not posted yet. Raw, by
// expense date; cmd/odoo_public_generate.go turns it into expenses.json.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	odoosource "github.com/CommonsHub/chb/providers/odoo"
)

// OdooExpense is one hr.expense record as archived.
type OdooExpense struct {
	ID            int                        `json:"id"`
	Name          string                     `json:"name"`
	Date          string                     `json:"date"`
	State         string                     `json:"state"`
	PaymentMode   string                     `json:"paymentMode,omitempty"` // own_account = reimburse the employee
	Product       string                     `json:"product,omitempty"`
	Quantity      float64                    `json:"quantity,omitempty"`
	UnitPrice     float64                    `json:"unitPrice,omitempty"`
	Total         float64                    `json:"totalAmount"`
	TotalCurrency float64                    `json:"totalAmountCurrency,omitempty"`
	TaxAmount     float64                    `json:"taxAmount,omitempty"`
	Currency      string                     `json:"currency,omitempty"`
	EmployeeID    int                        `json:"employeeId,omitempty"`
	Employee      string                     `json:"employee,omitempty"`
	VendorID      int                        `json:"vendorId,omitempty"`
	Vendor        string                     `json:"vendor,omitempty"`
	AccountCode   string                     `json:"accountCode,omitempty"`
	AccountName   string                     `json:"accountName,omitempty"`
	MoveID        int                        `json:"moveId,omitempty"` // the bill Odoo posted for it
	Analytic      []OdooInvoiceAnalyticSplit `json:"analyticDistribution,omitempty"`
	WriteDate     string                     `json:"writeDate,omitempty"`
}

type OdooExpensesFile struct {
	Year      string        `json:"year"`
	Month     string        `json:"month"`
	Source    string        `json:"source"`
	FetchedAt string        `json:"fetchedAt"`
	Expenses  []OdooExpense `json:"expenses"`
}

var odooExpenseFields = []string{
	"id", "name", "date", "state", "payment_mode", "product_id", "quantity", "price_unit",
	"total_amount", "total_amount_currency", "tax_amount", "currency_id", "employee_id", "vendor_id",
	"account_id", "account_move_id", "analytic_distribution", "write_date",
}

// syncOdooExpenses fetches every expense report (a few dozen; cheap) and
// rewrites a month's archive only when its content changed.
func syncOdooExpenses(creds *OdooCredentials, uid int, dataDir string) (int, error) {
	rows, err := odooSearchReadAllMaps(creds, uid, "hr.expense", []interface{}{}, odooExpenseFields, "date, id")
	if err != nil {
		return 0, err
	}
	byMonth := map[string][]OdooExpense{}
	for _, r := range rows {
		e := OdooExpense{
			ID: odooInt(r["id"]), Name: odooString(r["name"]), Date: odooString(r["date"]),
			State: odooString(r["state"]), PaymentMode: odooString(r["payment_mode"]),
			Product: odooFieldName(r["product_id"]), Quantity: odooFloat(r["quantity"]),
			UnitPrice: odooFloat(r["price_unit"]), Total: odooFloat(r["total_amount"]),
			TotalCurrency: odooFloat(r["total_amount_currency"]), TaxAmount: odooFloat(r["tax_amount"]),
			Currency: odooFieldName(r["currency_id"]), EmployeeID: odooFieldID(r["employee_id"]),
			Employee: odooFieldName(r["employee_id"]), VendorID: odooFieldID(r["vendor_id"]),
			Vendor: odooFieldName(r["vendor_id"]), MoveID: odooFieldID(r["account_move_id"]),
			WriteDate: odooString(r["write_date"]),
		}
		e.AccountCode, e.AccountName = splitOdooAccountName(odooFieldName(r["account_id"]))
		if len(e.Date) < 7 {
			continue
		}
		byMonth[e.Date[:7]] = append(byMonth[e.Date[:7]], e)
	}
	written := 0
	months := make([]string, 0, len(byMonth))
	for ym := range byMonth {
		months = append(months, ym)
	}
	sort.Strings(months)
	for _, ym := range months {
		year, month := ym[:4], ym[5:]
		out := OdooExpensesFile{Year: year, Month: month, Source: "odoo", Expenses: byMonth[ym]}
		path := odoosource.Path(dataDir, year, month, odoosource.ExpensesFile)
		if prev, err := os.ReadFile(path); err == nil {
			var old OdooExpensesFile
			if json.Unmarshal(prev, &old) == nil {
				a, _ := json.Marshal(old.Expenses)
				b, _ := json.Marshal(out.Expenses)
				if bytes.Equal(a, b) {
					continue
				}
			}
		}
		out.FetchedAt = time.Now().UTC().Format(time.RFC3339)
		data, _ := json.MarshalIndent(out, "", "  ")
		if err := writeDataFile(path, data); err != nil {
			return written, fmt.Errorf("%s: %w", ym, err)
		}
		written++
	}
	return len(rows), nil
}

// loadAllOdooExpenses reads every month's expense archive, keyed by id.
func loadAllOdooExpenses(dataDir string) map[int]OdooExpense {
	out := map[int]OdooExpense{}
	for _, p := range globOdooMonthFiles(dataDir, odoosource.ExpensesFile) {
		data, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		var f OdooExpensesFile
		if json.Unmarshal(data, &f) != nil {
			continue
		}
		for _, e := range f.Expenses {
			out[e.ID] = e
		}
	}
	return out
}
