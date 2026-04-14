package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type OdooVendorBillsFile struct {
	SchemaVersion int                         `json:"schemaVersion,omitempty"`
	Year          string                      `json:"year"`
	Month         string                      `json:"month"`
	Source        string                      `json:"source"`
	Count         int                         `json:"count"`
	FetchedAt     string                      `json:"fetchedAt"`
	MaxWriteDate  string                      `json:"maxWriteDate,omitempty"`
	Bills         []OdooOutgoingInvoicePublic `json:"bills"`
}

type OdooVendorBillsPrivateFile struct {
	SchemaVersion int                          `json:"schemaVersion,omitempty"`
	Year          string                       `json:"year"`
	Month         string                       `json:"month"`
	Source        string                       `json:"source"`
	Count         int                          `json:"count"`
	FetchedAt     string                       `json:"fetchedAt"`
	MaxWriteDate  string                       `json:"maxWriteDate,omitempty"`
	Bills         []OdooOutgoingInvoicePrivate `json:"bills"`
}

func BillsSync(args []string) (int, error) {
	if HasFlag(args, "--help", "-h", "help") {
		printBillsSyncHelp()
		return 0, nil
	}

	creds, err := ResolveOdooCredentials()
	if err != nil {
		fmt.Printf("%s⚠ %v, skipping bills sync%s\n", Fmt.Yellow, err, Fmt.Reset)
		return 0, nil
	}

	force := HasFlag(args, "--force")
	posYear, posMonth, posFound := ParseYearMonthArg(args)
	now := time.Now().In(BrusselsTZ())

	var startMonth, endMonth string
	sinceMonth, isSince := ResolveSinceMonth(args, filepath.Join("finance", "odoo", "bills.json"))
	isFullSync := isSince
	lastSyncTime := LastSyncTime("bills")

	if isSince {
		startMonth = sinceMonth
		endMonth = fmt.Sprintf("%d-%02d", now.Year(), now.Month())
	} else if posFound {
		if posMonth != "" {
			startMonth = fmt.Sprintf("%s-%s", posYear, posMonth)
			endMonth = startMonth
		} else {
			startMonth = fmt.Sprintf("%s-01", posYear)
			endMonth = fmt.Sprintf("%s-12", posYear)
		}
	} else {
		startMonth = DefaultRecentStartMonth(now)
		endMonth = fmt.Sprintf("%d-%02d", now.Year(), now.Month())
	}

	fmt.Printf("\n%s🧾 Syncing Odoo vendor bills%s\n", Fmt.Bold, Fmt.Reset)
	fmt.Printf("%sURL: %s  DB: %s%s\n", Fmt.Dim, creds.URL, creds.DB, Fmt.Reset)
	fmt.Printf("%sMonth range: %s → %s%s\n\n", Fmt.Dim, startMonth, endMonth, Fmt.Reset)

	uid, err := odooAuth(creds.URL, creds.DB, creds.Login, creds.Password)
	if err != nil || uid == 0 {
		return 0, fmt.Errorf("Odoo authentication failed: %v", err)
	}

	startDate, endDate, err := invoiceSyncDateRange(startMonth, endMonth)
	if err != nil {
		return 0, err
	}

	incremental := !force && !isSince && !posFound && !lastSyncTime.IsZero()
	if incremental {
		fmt.Printf("  %sIncremental since %s%s\n", Fmt.Dim, lastSyncTime.In(BrusselsTZ()).Format(time.RFC3339), Fmt.Reset)
	}

	cachedByMonth := map[string]map[int]OdooOutgoingInvoice{}
	if incremental {
		cachedByMonth = loadCachedBillMonths(DataDir(), startMonth, endMonth)
	}

	rawBills, err := fetchVendorBillsFromOdoo(creds, uid, startDate, endDate, incremental, lastSyncTime)
	if err != nil {
		return 0, err
	}

	if incremental && len(rawBills) == 0 {
		fmt.Printf("  %s✓ Up to date%s\n\n", Fmt.Green, Fmt.Reset)
		UpdateSyncSource("bills", isFullSync)
		UpdateSyncActivity(isFullSync)
		return 0, nil
	}

	fmt.Printf("  %sFetched %d bill(s)%s\n", Fmt.Dim, len(rawBills), Fmt.Reset)

	enriched, err := enrichOutgoingInvoices(creds, uid, rawBills, true)
	if err != nil {
		return 0, err
	}

	monthsTouched := map[string]bool{}
	byMonth := map[string]map[int]OdooOutgoingInvoice{}
	if incremental {
		for ym, monthBills := range cachedByMonth {
			byMonth[ym] = monthBills
		}
	}

	for _, bill := range enriched {
		ym := invoiceYearMonth(bill)
		if ym == "" || ym < startMonth || ym > endMonth {
			continue
		}
		if byMonth[ym] == nil {
			byMonth[ym] = map[int]OdooOutgoingInvoice{}
		}
		byMonth[ym][bill.ID] = bill
		monthsTouched[ym] = true
	}

	monthsToWrite := expandMonthRange(startMonth, endMonth)
	if incremental {
		monthsToWrite = nil
		for ym := range monthsTouched {
			monthsToWrite = append(monthsToWrite, ym)
		}
		sort.Strings(monthsToWrite)
	}

	savedBills := 0
	for _, ym := range monthsToWrite {
		parts := strings.Split(ym, "-")
		if len(parts) != 2 {
			continue
		}
		year, month := parts[0], parts[1]
		var bills []OdooOutgoingInvoice
		for _, bill := range byMonth[ym] {
			bills = append(bills, bill)
		}
		sort.Slice(bills, func(i, j int) bool {
			if bills[i].InvoiceDate == bills[j].InvoiceDate {
				return bills[i].ID > bills[j].ID
			}
			return bills[i].InvoiceDate > bills[j].InvoiceDate
		})

		publicOut := OdooVendorBillsFile{
			SchemaVersion: odooDocumentsSchemaVersion,
			Year:          year,
			Month:         month,
			Source:        "odoo",
			Count:         len(bills),
			FetchedAt:     time.Now().UTC().Format(time.RFC3339),
			MaxWriteDate:  maxInvoiceWriteDate(bills),
			Bills:         buildPublicInvoices(bills),
		}
		privateOut := OdooVendorBillsPrivateFile{
			SchemaVersion: odooDocumentsSchemaVersion,
			Year:          year,
			Month:         month,
			Source:        "odoo",
			Count:         len(bills),
			FetchedAt:     publicOut.FetchedAt,
			MaxWriteDate:  publicOut.MaxWriteDate,
			Bills:         buildPrivateInvoices(bills),
		}

		if !force && isBillMonthCacheUnchanged(DataDir(), year, month, publicOut, privateOut) {
			fmt.Printf("  ⏭ %s: %d bill(s) unchanged\n", ym, len(bills))
			continue
		}

		data, _ := marshalIndentedNoHTMLEscape(publicOut)
		if err := writeMonthFile(DataDir(), year, month, filepath.Join("finance", "odoo", "bills.json"), data); err != nil {
			fmt.Printf("  %s✗ Failed to write %s public bills: %v%s\n", Fmt.Red, ym, err, Fmt.Reset)
			continue
		}
		privateData, _ := marshalIndentedNoHTMLEscape(privateOut)
		if err := writeMonthFile(DataDir(), year, month, filepath.Join("finance", "odoo", "private", "bills.json"), privateData); err != nil {
			fmt.Printf("  %s✗ Failed to write %s: %v%s\n", Fmt.Red, ym, err, Fmt.Reset)
			continue
		}

		fmt.Printf("  ✓ %s: %d bill(s)\n", ym, len(bills))
		savedBills += len(bills)
	}

	fmt.Printf("\n%s✓ Done!%s %d bill(s) synced\n\n", Fmt.Green, Fmt.Reset, savedBills)
	UpdateSyncSource("bills", isFullSync)
	UpdateSyncActivity(isFullSync)
	return savedBills, nil
}

func fetchVendorBillsFromOdoo(creds *OdooCredentials, uid int, startDate, endDate string, incremental bool, lastSyncTime time.Time) ([]map[string]interface{}, error) {
	domain := []interface{}{
		[]interface{}{"move_type", "in", []interface{}{"in_invoice", "in_refund"}},
		[]interface{}{"date", ">=", startDate},
		[]interface{}{"date", "<=", endDate},
	}
	if incremental && !lastSyncTime.IsZero() {
		domain = append(domain, []interface{}{"write_date", ">=", lastSyncTime.UTC().Format("2006-01-02 15:04:05")})
	}

	fields := []string{
		"id", "name", "ref", "move_type", "state", "payment_state",
		"invoice_date", "date", "invoice_date_due", "payment_reference",
		"amount_untaxed", "amount_tax", "amount_total", "amount_residual", "amount_total_signed",
		"currency_id", "partner_id", "commercial_partner_id", "partner_bank_id", "journal_id",
		"invoice_origin", "narration", "invoice_line_ids",
		"write_date", "create_date", "invoice_payments_widget",
	}

	return odooSearchReadAllMaps(creds, uid, "account.move", domain, fields, "date desc, id desc")
}

func loadCachedBillMonths(dataDir, startMonth, endMonth string) map[string]map[int]OdooOutgoingInvoice {
	result := map[string]map[int]OdooOutgoingInvoice{}
	for _, ym := range expandMonthRange(startMonth, endMonth) {
		parts := strings.Split(ym, "-")
		if len(parts) != 2 {
			continue
		}
		year, month := parts[0], parts[1]
		docs := loadCachedBillMonth(dataDir, year, month)
		if len(docs) == 0 {
			continue
		}
		result[ym] = map[int]OdooOutgoingInvoice{}
		for _, bill := range docs {
			result[ym][bill.ID] = bill
		}
	}
	return result
}

func isBillMonthCacheUnchanged(dataDir, year, month string, nextPublic OdooVendorBillsFile, nextPrivate OdooVendorBillsPrivateFile) bool {
	publicPath := filepath.Join(dataDir, year, month, "finance", "odoo", "bills.json")
	privatePath := filepath.Join(dataDir, year, month, "finance", "odoo", "private", "bills.json")

	publicData, err := os.ReadFile(publicPath)
	if err != nil {
		return false
	}
	privateData, err := os.ReadFile(privatePath)
	if err != nil {
		return false
	}

	var currentPublic OdooVendorBillsFile
	if json.Unmarshal(publicData, &currentPublic) != nil {
		return false
	}
	var currentPrivate OdooVendorBillsPrivateFile
	if json.Unmarshal(privateData, &currentPrivate) != nil {
		return false
	}
	return currentPublic.SchemaVersion == nextPublic.SchemaVersion &&
		currentPrivate.SchemaVersion == nextPrivate.SchemaVersion &&
		currentPublic.Count == nextPublic.Count &&
		currentPrivate.Count == nextPrivate.Count &&
		currentPublic.MaxWriteDate == nextPublic.MaxWriteDate &&
		currentPrivate.MaxWriteDate == nextPrivate.MaxWriteDate
}

func loadCachedBillMonth(dataDir, year, month string) []OdooOutgoingInvoice {
	publicPath := filepath.Join(dataDir, year, month, "finance", "odoo", "bills.json")
	privatePath := filepath.Join(dataDir, year, month, "finance", "odoo", "private", "bills.json")

	publicByID := map[int]OdooOutgoingInvoice{}
	privateByID := map[int]OdooOutgoingInvoice{}

	if data, err := os.ReadFile(publicPath); err == nil {
		var file OdooVendorBillsFile
		if json.Unmarshal(data, &file) == nil {
			for _, bill := range file.Bills {
				publicByID[bill.ID] = OdooOutgoingInvoice{
					ID:                    bill.ID,
					Title:                 bill.Title,
					State:                 bill.State,
					PaymentState:          bill.PaymentState,
					InvoiceDate:           bill.Date,
					Date:                  bill.Date,
					Sent:                  bill.Sent,
					SentAt:                bill.SentAt,
					UntaxedAmount:         bill.UntaxedAmount,
					VATAmount:             bill.VATAmount,
					TotalAmount:           bill.TotalAmount,
					Currency:              bill.Currency,
					Journal:               bill.Journal,
					LineItems:             bill.LineItems,
					ReconciledTransaction: bill.ReconciledTransaction,
					Category:              bill.Category,
					Categories:            bill.Categories,
					Tags:                  bill.Tags,
				}
			}
		}
	}

	if data, err := os.ReadFile(privatePath); err == nil {
		var file OdooVendorBillsPrivateFile
		if json.Unmarshal(data, &file) == nil {
			for _, bill := range file.Bills {
				privateByID[bill.ID] = OdooOutgoingInvoice{
					ID:                 bill.ID,
					Number:             bill.Number,
					Ref:                bill.Ref,
					MoveType:           bill.MoveType,
					State:              bill.State,
					PaymentState:       bill.PaymentState,
					InvoiceDate:        bill.InvoiceDate,
					Date:               bill.Date,
					DueDate:            bill.DueDate,
					Reference:          bill.Reference,
					InvoiceOrigin:      bill.InvoiceOrigin,
					ResidualAmount:     bill.ResidualAmount,
					TotalSignedAmount:  bill.TotalSignedAmount,
					Partner:            bill.Partner,
					PartnerBank:        bill.PartnerBank,
					Transactions:       bill.Transactions,
					Payments:           bill.Payments,
					Attachments:        bill.Attachments,
					WriteDate:          bill.WriteDate,
					CreateDate:         bill.CreateDate,
					InvoiceURL:         bill.InvoiceURL,
					PartnerDisplayName: bill.PartnerDisplayName,
				}
			}
		}
	}

	idSet := map[int]bool{}
	for id := range publicByID {
		idSet[id] = true
	}
	for id := range privateByID {
		idSet[id] = true
	}

	var docs []OdooOutgoingInvoice
	for _, id := range sortedIDSet(idSet) {
		doc := publicByID[id]
		privateDoc := privateByID[id]
		mergePrivateOdooDocument(&doc, privateDoc)
		if doc.ID == 0 {
			doc = privateDoc
		}
		docs = append(docs, doc)
	}
	return docs
}

func printBillsSyncHelp() {
	f := Fmt
	fmt.Printf(`
%schb bills sync%s — Fetch vendor bills from Odoo

%sUSAGE%s
  %schb bills sync%s [year[/month]] [options]

%sOPTIONS%s
  %s<year>%s               Sync all months of a year (e.g. 2025)
  %s<year/month>%s         Sync a specific month (e.g. 2025/03)
  %s--since%s YYYY/MM      Sync from a specific month to now
  %s--history%s            Sync bill history from the oldest cached month
  %s--force%s              Re-fetch and overwrite cached month files
  %s--help, -h%s           Show this help

%sDATA%s
  Saves monthly vendor bill snapshots to:
    ~/.chb/data/YYYY/MM/finance/odoo/bills.json
    ~/.chb/data/YYYY/MM/finance/odoo/private/bills.json

  Each bill includes:
  • public: date, status, payment status, amounts, title, line items, VAT, categories, tags, journal, reconciled transaction
  • private: vendor details, payable bank account, attachments

%sENVIRONMENT%s
  %sODOO_URL%s             Odoo instance URL
  %sODOO_LOGIN%s           Odoo login email
  %sODOO_PASSWORD%s        Odoo password or API key
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
	)
}
