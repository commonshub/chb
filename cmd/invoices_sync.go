package cmd

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const odooDocumentsSchemaVersion = 4

type OdooOutgoingInvoicesFile struct {
	SchemaVersion int                         `json:"schemaVersion,omitempty"`
	Year          string                      `json:"year"`
	Month         string                      `json:"month"`
	Source        string                      `json:"source"`
	Count         int                         `json:"count"`
	FetchedAt     string                      `json:"fetchedAt"`
	MaxWriteDate  string                      `json:"maxWriteDate,omitempty"`
	Invoices      []OdooOutgoingInvoicePublic `json:"invoices"`
}

type OdooOutgoingInvoicesPrivateFile struct {
	SchemaVersion int                          `json:"schemaVersion,omitempty"`
	Year          string                       `json:"year"`
	Month         string                       `json:"month"`
	Source        string                       `json:"source"`
	Count         int                          `json:"count"`
	FetchedAt     string                       `json:"fetchedAt"`
	MaxWriteDate  string                       `json:"maxWriteDate,omitempty"`
	Invoices      []OdooOutgoingInvoicePrivate `json:"invoices"`
}

var (
	odooModelFieldsCacheMu sync.Mutex
	odooModelFieldsCache   = map[string]map[string]bool{}
	odooLocalTxIndexMu     sync.Mutex
	odooLocalTxIndexCache  *odooLocalTxIndex
)

type OdooOutgoingInvoice struct {
	ID                    int                        `json:"id"`
	Number                string                     `json:"number,omitempty"`
	Title                 string                     `json:"title,omitempty"`
	Ref                   string                     `json:"ref,omitempty"`
	MoveType              string                     `json:"moveType,omitempty"`
	State                 string                     `json:"state,omitempty"`
	PaymentState          string                     `json:"paymentState,omitempty"`
	Sent                  bool                       `json:"sent,omitempty"`
	SentAt                string                     `json:"sentAt,omitempty"`
	InvoiceDate           string                     `json:"invoiceDate,omitempty"`
	Date                  string                     `json:"date,omitempty"`
	DueDate               string                     `json:"dueDate,omitempty"`
	Reference             string                     `json:"reference,omitempty"`
	InvoiceOrigin         string                     `json:"invoiceOrigin,omitempty"`
	UntaxedAmount         float64                    `json:"untaxedAmount"`
	VATAmount             float64                    `json:"vatAmount"`
	TotalAmount           float64                    `json:"totalAmount"`
	ResidualAmount        float64                    `json:"residualAmount,omitempty"`
	TotalSignedAmount     float64                    `json:"totalSignedAmount,omitempty"`
	Currency              string                     `json:"currency,omitempty"`
	Journal               OdooInvoiceJournal         `json:"journal"`
	Partner               OdooInvoicePartner         `json:"partner"`
	PartnerBank           *OdooInvoiceBankAccount    `json:"partnerBank,omitempty"`
	LineItems             []OdooInvoiceLineItem      `json:"lineItems"`
	Transactions          []OdooInvoiceTx            `json:"transactions,omitempty"`
	Payments              []OdooInvoicePayment       `json:"payments,omitempty"`
	Attachments           []OdooDocumentAttachment   `json:"attachments,omitempty"`
	ReconciledTransaction *OdooReconciledTransaction `json:"reconciledTransaction,omitempty"`
	Category              string                     `json:"category,omitempty"`
	Categories            []string                   `json:"categories,omitempty"`
	Tags                  []string                   `json:"tags,omitempty"`
	WriteDate             string                     `json:"writeDate,omitempty"`
	CreateDate            string                     `json:"createDate,omitempty"`
	InvoiceURL            string                     `json:"invoiceUrl,omitempty"`
	PartnerDisplayName    string                     `json:"partnerDisplayName,omitempty"`
}

type OdooOutgoingInvoicePublic struct {
	ID                    int                        `json:"id"`
	Title                 string                     `json:"title,omitempty"`
	State                 string                     `json:"state,omitempty"`
	PaymentState          string                     `json:"paymentState,omitempty"`
	Date                  string                     `json:"date,omitempty"`
	Sent                  bool                       `json:"sent,omitempty"`
	SentAt                string                     `json:"sentAt,omitempty"`
	UntaxedAmount         float64                    `json:"untaxedAmount"`
	VATAmount             float64                    `json:"vatAmount"`
	TotalAmount           float64                    `json:"totalAmount"`
	Currency              string                     `json:"currency,omitempty"`
	Journal               OdooInvoiceJournal         `json:"journal"`
	LineItems             []OdooInvoiceLineItem      `json:"lineItems"`
	ReconciledTransaction *OdooReconciledTransaction `json:"reconciledTransaction,omitempty"`
	Category              string                     `json:"category,omitempty"`
	Categories            []string                   `json:"categories,omitempty"`
	Tags                  []string                   `json:"tags,omitempty"`
}

type OdooOutgoingInvoicePrivate struct {
	ID                 int                      `json:"id"`
	Number             string                   `json:"number,omitempty"`
	Ref                string                   `json:"ref,omitempty"`
	MoveType           string                   `json:"moveType,omitempty"`
	State              string                   `json:"state,omitempty"`
	PaymentState       string                   `json:"paymentState,omitempty"`
	InvoiceDate        string                   `json:"invoiceDate,omitempty"`
	Date               string                   `json:"date,omitempty"`
	DueDate            string                   `json:"dueDate,omitempty"`
	Reference          string                   `json:"reference,omitempty"`
	InvoiceOrigin      string                   `json:"invoiceOrigin,omitempty"`
	ResidualAmount     float64                  `json:"residualAmount,omitempty"`
	TotalSignedAmount  float64                  `json:"totalSignedAmount,omitempty"`
	Partner            OdooInvoicePartner       `json:"partner"`
	PartnerBank        *OdooInvoiceBankAccount  `json:"partnerBank,omitempty"`
	Transactions       []OdooInvoiceTx          `json:"transactions,omitempty"`
	Payments           []OdooInvoicePayment     `json:"payments,omitempty"`
	Attachments        []OdooDocumentAttachment `json:"attachments,omitempty"`
	WriteDate          string                   `json:"writeDate,omitempty"`
	CreateDate         string                   `json:"createDate,omitempty"`
	InvoiceURL         string                   `json:"invoiceUrl,omitempty"`
	PartnerDisplayName string                   `json:"partnerDisplayName,omitempty"`
}

type OdooInvoiceJournal struct {
	ID   int    `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
}

type OdooInvoicePartner struct {
	ID          int    `json:"id,omitempty"`
	Name        string `json:"name,omitempty"`
	DisplayName string `json:"displayName,omitempty"`
	Email       string `json:"email,omitempty"`
	VAT         string `json:"vat,omitempty"`
	Phone       string `json:"phone,omitempty"`
	Mobile      string `json:"mobile,omitempty"`
	Street      string `json:"street,omitempty"`
	Street2     string `json:"street2,omitempty"`
	ZIP         string `json:"zip,omitempty"`
	City        string `json:"city,omitempty"`
	Country     string `json:"country,omitempty"`
	Website     string `json:"website,omitempty"`
	IsCompany   bool   `json:"isCompany,omitempty"`
	CompanyType string `json:"companyType,omitempty"`
}

type OdooInvoiceBankAccount struct {
	ID              int    `json:"id,omitempty"`
	AccountNumber   string `json:"accountNumber,omitempty"`
	SanitizedNumber string `json:"sanitizedNumber,omitempty"`
	BankName        string `json:"bankName,omitempty"`
	Currency        string `json:"currency,omitempty"`
	PartnerID       int    `json:"partnerId,omitempty"`
	PartnerName     string `json:"partnerName,omitempty"`
}

type OdooInvoiceLineItem struct {
	ID                   int                        `json:"id"`
	Title                string                     `json:"title,omitempty"`
	ProductID            int                        `json:"productId,omitempty"`
	ProductName          string                     `json:"productName,omitempty"`
	DisplayType          string                     `json:"displayType,omitempty"`
	Quantity             float64                    `json:"quantity,omitempty"`
	UnitPrice            float64                    `json:"unitPrice,omitempty"`
	SubtotalAmount       float64                    `json:"subtotalAmount,omitempty"`
	TotalAmount          float64                    `json:"totalAmount,omitempty"`
	Taxes                []OdooInvoiceTax           `json:"taxes,omitempty"`
	AnalyticDistribution []OdooInvoiceAnalyticSplit `json:"analyticDistribution,omitempty"`
	Category             string                     `json:"category,omitempty"`
	Categories           []string                   `json:"categories,omitempty"`
	Tags                 []string                   `json:"tags,omitempty"`
}

type OdooInvoiceTax struct {
	ID          int     `json:"id,omitempty"`
	Name        string  `json:"name,omitempty"`
	Description string  `json:"description,omitempty"`
	Amount      float64 `json:"amount,omitempty"`
	AmountType  string  `json:"amountType,omitempty"`
}

type OdooInvoiceAnalyticSplit struct {
	AccountID   int     `json:"accountId,omitempty"`
	AccountName string  `json:"accountName,omitempty"`
	AccountCode string  `json:"accountCode,omitempty"`
	Plan        string  `json:"plan,omitempty"`
	Percentage  float64 `json:"percentage,omitempty"`
	Category    string  `json:"category,omitempty"`
}

type OdooInvoiceTx struct {
	ID                int     `json:"id,omitempty"`
	Provider          string  `json:"provider,omitempty"`
	Reference         string  `json:"reference,omitempty"`
	ProviderReference string  `json:"providerReference,omitempty"`
	TxHash            string  `json:"txHash,omitempty"`
	Date              string  `json:"date,omitempty"`
	Amount            float64 `json:"amount,omitempty"`
	Currency          string  `json:"currency,omitempty"`
	State             string  `json:"state,omitempty"`
	Operation         string  `json:"operation,omitempty"`
}

type OdooInvoicePayment struct {
	PaymentID int     `json:"paymentId,omitempty"`
	MoveID    int     `json:"moveId,omitempty"`
	Date      string  `json:"date,omitempty"`
	Journal   string  `json:"journal,omitempty"`
	Reference string  `json:"reference,omitempty"`
	Amount    float64 `json:"amount,omitempty"`
	TxHash    string  `json:"txHash,omitempty"`
}

type OdooDocumentAttachment struct {
	ID         int    `json:"id,omitempty"`
	Name       string `json:"name,omitempty"`
	FileName   string `json:"fileName,omitempty"`
	Type       string `json:"type,omitempty"`
	MimeType   string `json:"mimeType,omitempty"`
	URL        string `json:"url,omitempty"`
	LocalPath  string `json:"localPath,omitempty"`
	Checksum   string `json:"checksum,omitempty"`
	Size       int64  `json:"size,omitempty"`
	Public     bool   `json:"public,omitempty"`
	CreateDate string `json:"createDate,omitempty"`
	WriteDate  string `json:"writeDate,omitempty"`
}

type OdooReconciledTransaction struct {
	Source       string  `json:"source,omitempty"`
	ID           string  `json:"id,omitempty"`
	Provider     string  `json:"provider,omitempty"`
	Reference    string  `json:"reference,omitempty"`
	TxHash       string  `json:"txHash,omitempty"`
	Date         string  `json:"date,omitempty"`
	Amount       float64 `json:"amount,omitempty"`
	Currency     string  `json:"currency,omitempty"`
	State        string  `json:"state,omitempty"`
	AccountSlug  string  `json:"accountSlug,omitempty"`
	AccountName  string  `json:"accountName,omitempty"`
	Counterparty string  `json:"counterparty,omitempty"`
}

type odooLocalTxIndex struct {
	byHash     map[string]TransactionEntry
	byID       map[string]TransactionEntry
	byChargeID map[string]TransactionEntry
}

func InvoicesSync(args []string) (int, error) {
	if HasFlag(args, "--help", "-h", "help") {
		printInvoicesSyncHelp()
		return 0, nil
	}

	creds, err := ResolveOdooCredentials()
	if err != nil {
		fmt.Printf("%s⚠ %v, skipping invoices sync%s\n", Fmt.Yellow, err, Fmt.Reset)
		return 0, nil
	}

	force := HasFlag(args, "--force")
	posYear, posMonth, posFound := ParseYearMonthArg(args)
	now := time.Now().In(BrusselsTZ())

	var startMonth, endMonth string
	sinceMonth, isSince := ResolveSinceMonth(args, filepath.Join("finance", "odoo", "invoices.json"))
	isFullSync := isSince
	lastSyncTime := LastSyncTime("invoices")

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

	fmt.Printf("\n%s🧾 Syncing Odoo invoices%s\n", Fmt.Bold, Fmt.Reset)
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
		cachedByMonth = loadCachedInvoiceMonths(DataDir(), startMonth, endMonth)
	}

	rawInvoices, err := fetchOutgoingInvoicesFromOdoo(creds, uid, startDate, endDate, incremental, lastSyncTime)
	if err != nil {
		return 0, err
	}

	if incremental && len(rawInvoices) == 0 {
		fmt.Printf("  %s✓ Up to date%s\n\n", Fmt.Green, Fmt.Reset)
		UpdateSyncSource("invoices", isFullSync)
		UpdateSyncActivity(isFullSync)
		return 0, nil
	}

	fmt.Printf("  %sFetched %d invoice(s)%s\n", Fmt.Dim, len(rawInvoices), Fmt.Reset)

	enriched, err := enrichOutgoingInvoices(creds, uid, rawInvoices, false)
	if err != nil {
		return 0, err
	}

	monthsTouched := map[string]bool{}
	byMonth := map[string]map[int]OdooOutgoingInvoice{}
	if incremental {
		for ym, monthInvoices := range cachedByMonth {
			byMonth[ym] = monthInvoices
		}
	}

	for _, inv := range enriched {
		ym := invoiceYearMonth(inv)
		if ym == "" || ym < startMonth || ym > endMonth {
			continue
		}
		if byMonth[ym] == nil {
			byMonth[ym] = map[int]OdooOutgoingInvoice{}
		}
		byMonth[ym][inv.ID] = inv
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

	savedInvoices := 0
	for _, ym := range monthsToWrite {
		parts := strings.Split(ym, "-")
		if len(parts) != 2 {
			continue
		}
		year, month := parts[0], parts[1]
		var invoices []OdooOutgoingInvoice
		for _, inv := range byMonth[ym] {
			invoices = append(invoices, inv)
		}
		sort.Slice(invoices, func(i, j int) bool {
			if invoices[i].InvoiceDate == invoices[j].InvoiceDate {
				return invoices[i].ID > invoices[j].ID
			}
			return invoices[i].InvoiceDate > invoices[j].InvoiceDate
		})

		publicOut := OdooOutgoingInvoicesFile{
			SchemaVersion: odooDocumentsSchemaVersion,
			Year:          year,
			Month:         month,
			Source:        "odoo",
			Count:         len(invoices),
			FetchedAt:     time.Now().UTC().Format(time.RFC3339),
			MaxWriteDate:  maxInvoiceWriteDate(invoices),
			Invoices:      buildPublicInvoices(invoices),
		}
		privateOut := OdooOutgoingInvoicesPrivateFile{
			SchemaVersion: odooDocumentsSchemaVersion,
			Year:          year,
			Month:         month,
			Source:        "odoo",
			Count:         len(invoices),
			FetchedAt:     publicOut.FetchedAt,
			MaxWriteDate:  publicOut.MaxWriteDate,
			Invoices:      buildPrivateInvoices(invoices),
		}

		if !force && isInvoiceMonthCacheUnchanged(DataDir(), year, month, publicOut, privateOut) {
			fmt.Printf("  ⏭ %s: %d invoice(s) unchanged\n", ym, len(invoices))
			continue
		}

		data, _ := marshalIndentedNoHTMLEscape(publicOut)
		if err := writeMonthFile(DataDir(), year, month, filepath.Join("finance", "odoo", "invoices.json"), data); err != nil {
			fmt.Printf("  %s✗ Failed to write %s public invoices: %v%s\n", Fmt.Red, ym, err, Fmt.Reset)
			continue
		}
		privateData, _ := marshalIndentedNoHTMLEscape(privateOut)
		if err := writeMonthFile(DataDir(), year, month, filepath.Join("finance", "odoo", "private", "invoices.json"), privateData); err != nil {
			fmt.Printf("  %s✗ Failed to write %s: %v%s\n", Fmt.Red, ym, err, Fmt.Reset)
			continue
		}

		fmt.Printf("  ✓ %s: %d invoice(s)\n", ym, len(invoices))
		savedInvoices += len(invoices)
	}

	fmt.Printf("\n%s✓ Done!%s %d invoice(s) synced\n\n", Fmt.Green, Fmt.Reset, savedInvoices)
	UpdateSyncSource("invoices", isFullSync)
	UpdateSyncActivity(isFullSync)
	return savedInvoices, nil
}

func invoiceSyncDateRange(startMonth, endMonth string) (string, string, error) {
	startParts := strings.Split(startMonth, "-")
	endParts := strings.Split(endMonth, "-")
	if len(startParts) != 2 || len(endParts) != 2 {
		return "", "", fmt.Errorf("invalid month range: %s → %s", startMonth, endMonth)
	}
	sy, err := strconv.Atoi(startParts[0])
	if err != nil {
		return "", "", err
	}
	sm, err := strconv.Atoi(startParts[1])
	if err != nil {
		return "", "", err
	}
	ey, err := strconv.Atoi(endParts[0])
	if err != nil {
		return "", "", err
	}
	em, err := strconv.Atoi(endParts[1])
	if err != nil {
		return "", "", err
	}
	start := time.Date(sy, time.Month(sm), 1, 0, 0, 0, 0, BrusselsTZ())
	end := time.Date(ey, time.Month(em)+1, 0, 0, 0, 0, 0, BrusselsTZ())
	return start.Format("2006-01-02"), end.Format("2006-01-02"), nil
}

func fetchOutgoingInvoicesFromOdoo(creds *OdooCredentials, uid int, startDate, endDate string, incremental bool, lastSyncTime time.Time) ([]map[string]interface{}, error) {
	domain := []interface{}{
		[]interface{}{"move_type", "in", []interface{}{"out_invoice", "out_refund"}},
		[]interface{}{"date", ">=", startDate},
		[]interface{}{"date", "<=", endDate},
	}
	if incremental && !lastSyncTime.IsZero() {
		domain = append(domain, []interface{}{"write_date", ">=", lastSyncTime.UTC().Format("2006-01-02 15:04:05")})
	}

	fields := []string{
		"id", "name", "ref", "move_type", "state", "payment_state", "is_move_sent",
		"invoice_date", "date", "invoice_date_due", "payment_reference",
		"amount_untaxed", "amount_tax", "amount_total", "amount_residual", "amount_total_signed",
		"currency_id", "partner_id", "commercial_partner_id", "journal_id",
		"invoice_origin", "narration", "invoice_line_ids",
		"write_date", "create_date", "invoice_payments_widget",
	}

	return odooSearchReadAllMaps(creds, uid, "account.move", domain, fields, "date desc, id desc")
}

func odooSearchReadAllMaps(creds *OdooCredentials, uid int, model string, domain []interface{}, fields []string, order string) ([]map[string]interface{}, error) {
	fields, err := odooFilterReadableFields(creds, uid, model, fields)
	if err != nil {
		return nil, err
	}

	const pageSize = 200
	var all []map[string]interface{}
	offset := 0
	for {
		result, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
			model, "search_read",
			[]interface{}{domain},
			map[string]interface{}{
				"fields": fields,
				"limit":  pageSize,
				"offset": offset,
				"order":  order,
			},
		)
		if err != nil {
			return nil, err
		}
		var rows []map[string]interface{}
		if err := json.Unmarshal(result, &rows); err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			break
		}
		all = append(all, rows...)
		if len(rows) < pageSize {
			break
		}
		offset += pageSize
		fmt.Printf("    %s%s: %d fetched%s\n", Fmt.Dim, model, len(all), Fmt.Reset)
	}
	return all, nil
}

func odooReadMapsByIDs(creds *OdooCredentials, uid int, model string, ids []int, fields []string) ([]map[string]interface{}, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	fields, err := odooFilterReadableFields(creds, uid, model, fields)
	if err != nil {
		return nil, err
	}
	const batchSize = 200
	var all []map[string]interface{}
	for i := 0; i < len(ids); i += batchSize {
		end := i + batchSize
		if end > len(ids) {
			end = len(ids)
		}
		batch := make([]interface{}, 0, end-i)
		for _, id := range ids[i:end] {
			batch = append(batch, id)
		}
		result, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
			model, "read",
			[]interface{}{batch},
			map[string]interface{}{"fields": fields},
		)
		if err != nil {
			return nil, err
		}
		var rows []map[string]interface{}
		if err := json.Unmarshal(result, &rows); err != nil {
			return nil, err
		}
		all = append(all, rows...)
	}
	return all, nil
}

func odooFilterReadableFields(creds *OdooCredentials, uid int, model string, fields []string) ([]string, error) {
	available, err := odooAvailableFields(creds, uid, model)
	if err != nil {
		return nil, fmt.Errorf("failed to inspect Odoo fields for %s: %w", model, err)
	}

	filtered := make([]string, 0, len(fields))
	var skipped []string
	for _, field := range fields {
		if available[field] {
			filtered = append(filtered, field)
			continue
		}
		skipped = append(skipped, field)
	}

	if len(skipped) > 0 {
		fmt.Printf("    %s%s: skipping unsupported fields: %s%s\n", Fmt.Dim, model, strings.Join(skipped, ", "), Fmt.Reset)
	}

	return filtered, nil
}

func odooAvailableFields(creds *OdooCredentials, uid int, model string) (map[string]bool, error) {
	cacheKey := creds.URL + "|" + creds.DB + "|" + model

	odooModelFieldsCacheMu.Lock()
	if fields, ok := odooModelFieldsCache[cacheKey]; ok {
		odooModelFieldsCacheMu.Unlock()
		return fields, nil
	}
	odooModelFieldsCacheMu.Unlock()

	result, err := odooExec(
		creds.URL,
		creds.DB,
		uid,
		creds.Password,
		model,
		"fields_get",
		[]interface{}{},
		map[string]interface{}{"attributes": []string{"type"}},
	)
	if err != nil {
		return nil, err
	}

	var meta map[string]map[string]interface{}
	if err := json.Unmarshal(result, &meta); err != nil {
		return nil, err
	}

	fields := make(map[string]bool, len(meta))
	for name := range meta {
		fields[name] = true
	}
	fields["id"] = true

	odooModelFieldsCacheMu.Lock()
	odooModelFieldsCache[cacheKey] = fields
	odooModelFieldsCacheMu.Unlock()

	return fields, nil
}

func enrichOutgoingInvoices(creds *OdooCredentials, uid int, rawInvoices []map[string]interface{}, includePartnerBank bool) ([]OdooOutgoingInvoice, error) {
	if len(rawInvoices) == 0 {
		return nil, nil
	}

	lineIDs := map[int]bool{}
	partnerIDs := map[int]bool{}
	bankIDs := map[int]bool{}
	invoiceIDs := make([]int, 0, len(rawInvoices))

	for _, inv := range rawInvoices {
		invoiceIDs = append(invoiceIDs, odooInt(inv["id"]))
		for _, id := range odooIDList(inv["invoice_line_ids"]) {
			lineIDs[id] = true
		}
		if id := odooFieldID(inv["partner_id"]); id > 0 {
			partnerIDs[id] = true
		}
		if id := odooFieldID(inv["commercial_partner_id"]); id > 0 {
			partnerIDs[id] = true
		}
		if includePartnerBank {
			if id := odooFieldID(inv["partner_bank_id"]); id > 0 {
				bankIDs[id] = true
			}
		}
	}

	lineRows, err := odooReadMapsByIDs(creds, uid, "account.move.line", sortedIDSet(lineIDs), []string{
		"id", "move_id", "product_id", "name", "quantity", "price_unit",
		"price_subtotal", "price_total", "tax_ids", "analytic_distribution", "display_type",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch invoice lines: %w", err)
	}

	analyticIDs := map[int]bool{}
	taxIDs := map[int]bool{}
	linesByMove := map[int][]map[string]interface{}{}
	for _, line := range lineRows {
		moveID := odooFieldID(line["move_id"])
		if moveID == 0 {
			continue
		}
		linesByMove[moveID] = append(linesByMove[moveID], line)
		if id := odooFieldID(line["analytic_account_id"]); id > 0 {
			analyticIDs[id] = true
		}
		for _, id := range analyticDistributionIDs(line["analytic_distribution"]) {
			analyticIDs[id] = true
		}
		for _, id := range odooIDList(line["tax_ids"]) {
			taxIDs[id] = true
		}
	}

	partnerRows, err := odooReadMapsByIDs(creds, uid, "res.partner", sortedIDSet(partnerIDs), []string{
		"id", "name", "display_name", "email", "vat", "phone",
		"street", "street2", "zip", "city", "country_id", "website",
		"is_company", "company_type",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch partners: %w", err)
	}

	bankRows := []map[string]interface{}(nil)
	if includePartnerBank {
		bankRows, err = odooReadMapsByIDs(creds, uid, "res.partner.bank", sortedIDSet(bankIDs), []string{
			"id", "acc_number", "sanitized_acc_number", "bank_id", "partner_id", "currency_id",
		})
		if err != nil {
			return nil, fmt.Errorf("failed to fetch bank accounts: %w", err)
		}
	}

	analyticRows, err := odooReadMapsByIDs(creds, uid, "account.analytic.account", sortedIDSet(analyticIDs), []string{
		"id", "name", "code", "plan_id",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch analytic accounts: %w", err)
	}

	taxRows, err := odooReadMapsByIDs(creds, uid, "account.tax", sortedIDSet(taxIDs), []string{
		"id", "name", "description", "amount", "amount_type",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch taxes: %w", err)
	}

	paymentTxRows, err := odooSearchReadAllMaps(creds, uid, "payment.transaction",
		[]interface{}{[]interface{}{"invoice_ids", "in", intsToInterfaces(invoiceIDs)}},
		[]string{"id", "provider_code", "provider_reference", "amount", "currency_id", "state", "invoice_ids", "reference", "create_date", "last_state_change", "operation"},
		"id desc")
	if err != nil {
		fmt.Printf("  %s⚠ Could not fetch payment transactions: %v%s\n", Fmt.Yellow, err, Fmt.Reset)
	}

	attachmentsByInvoiceID, err := fetchOdooDocumentAttachments(creds, uid, invoiceIDs)
	if err != nil {
		fmt.Printf("  %s⚠ Could not fetch document attachments: %v%s\n", Fmt.Yellow, err, Fmt.Reset)
	}
	sentAtByInvoiceID, err := fetchInvoiceSentDates(creds, uid, invoiceIDs)
	if err != nil {
		fmt.Printf("  %s⚠ Could not fetch invoice sent dates: %v%s\n", Fmt.Yellow, err, Fmt.Reset)
	}

	localTxIndex := loadOdooLocalTxIndex()

	partnersByID := map[int]map[string]interface{}{}
	for _, row := range partnerRows {
		partnersByID[odooInt(row["id"])] = row
	}
	banksByID := map[int]map[string]interface{}{}
	for _, row := range bankRows {
		banksByID[odooInt(row["id"])] = row
	}
	analyticsByID := map[int]map[string]interface{}{}
	for _, row := range analyticRows {
		analyticsByID[odooInt(row["id"])] = row
	}
	taxesByID := map[int]map[string]interface{}{}
	for _, row := range taxRows {
		taxesByID[odooInt(row["id"])] = row
	}

	txByInvoiceID := map[int][]OdooInvoiceTx{}
	for _, row := range paymentTxRows {
		tx := buildInvoiceTransaction(row)
		for _, invID := range odooIDList(row["invoice_ids"]) {
			txByInvoiceID[invID] = append(txByInvoiceID[invID], tx)
		}
	}

	var invoices []OdooOutgoingInvoice
	for _, raw := range rawInvoices {
		inv := OdooOutgoingInvoice{
			ID:                odooInt(raw["id"]),
			Number:            odooString(raw["name"]),
			Ref:               odooString(raw["ref"]),
			MoveType:          odooString(raw["move_type"]),
			State:             odooString(raw["state"]),
			PaymentState:      odooString(raw["payment_state"]),
			Sent:              odooBool(raw["is_move_sent"]),
			SentAt:            sentAtByInvoiceID[odooInt(raw["id"])],
			InvoiceDate:       odooString(raw["invoice_date"]),
			Date:              odooString(raw["date"]),
			DueDate:           odooString(raw["invoice_date_due"]),
			Reference:         odooString(raw["payment_reference"]),
			InvoiceOrigin:     odooString(raw["invoice_origin"]),
			UntaxedAmount:     odooFloat(raw["amount_untaxed"]),
			VATAmount:         odooFloat(raw["amount_tax"]),
			TotalAmount:       odooFloat(raw["amount_total"]),
			ResidualAmount:    odooFloat(raw["amount_residual"]),
			TotalSignedAmount: odooFloat(raw["amount_total_signed"]),
			Currency:          odooFieldName(raw["currency_id"]),
			Journal: OdooInvoiceJournal{
				ID:   odooFieldID(raw["journal_id"]),
				Name: odooFieldName(raw["journal_id"]),
			},
			WriteDate:  odooString(raw["write_date"]),
			CreateDate: odooString(raw["create_date"]),
			InvoiceURL: fmt.Sprintf("%s/web#id=%d&model=account.move&view_type=form", creds.URL, odooInt(raw["id"])),
		}

		partnerRow := partnersByID[odooFieldID(raw["partner_id"])]
		if partnerRow == nil {
			partnerRow = partnersByID[odooFieldID(raw["commercial_partner_id"])]
		}
		inv.Partner = buildInvoicePartner(partnerRow)
		inv.PartnerDisplayName = inv.Partner.DisplayName

		if includePartnerBank {
			if bankRow := banksByID[odooFieldID(raw["partner_bank_id"])]; bankRow != nil {
				bank := buildInvoiceBankAccount(bankRow)
				inv.PartnerBank = &bank
			}
		}

		inv.LineItems, inv.Categories, inv.Tags = buildInvoiceLineItems(linesByMove[inv.ID], analyticsByID, taxesByID)
		inv.Category = firstNonEmpty(uniqueSortedStrings(inv.Categories)...)
		inv.Transactions = txByInvoiceID[inv.ID]
		inv.Payments = parseInvoicePaymentsWidget(raw["invoice_payments_widget"])
		inv.Attachments = attachmentsByInvoiceID[inv.ID]
		inv.ReconciledTransaction = matchOdooReconciledTransaction(localTxIndex, inv.Transactions, inv.Payments)
		inv.Title = buildInvoiceTitle(inv)

		invoices = append(invoices, inv)
	}

	sort.Slice(invoices, func(i, j int) bool {
		if invoices[i].InvoiceDate == invoices[j].InvoiceDate {
			return invoices[i].ID > invoices[j].ID
		}
		return invoices[i].InvoiceDate > invoices[j].InvoiceDate
	})
	return invoices, nil
}

func buildInvoicePartner(row map[string]interface{}) OdooInvoicePartner {
	if row == nil {
		return OdooInvoicePartner{}
	}
	return OdooInvoicePartner{
		ID:          odooInt(row["id"]),
		Name:        odooString(row["name"]),
		DisplayName: firstNonEmpty(odooString(row["display_name"]), odooString(row["name"])),
		Email:       odooString(row["email"]),
		VAT:         odooString(row["vat"]),
		Phone:       odooString(row["phone"]),
		Mobile:      odooString(row["mobile"]),
		Street:      odooString(row["street"]),
		Street2:     odooString(row["street2"]),
		ZIP:         odooString(row["zip"]),
		City:        odooString(row["city"]),
		Country:     odooFieldName(row["country_id"]),
		Website:     odooString(row["website"]),
		IsCompany:   odooBool(row["is_company"]),
		CompanyType: odooString(row["company_type"]),
	}
}

func buildInvoiceBankAccount(row map[string]interface{}) OdooInvoiceBankAccount {
	return OdooInvoiceBankAccount{
		ID:              odooInt(row["id"]),
		AccountNumber:   odooString(row["acc_number"]),
		SanitizedNumber: odooString(row["sanitized_acc_number"]),
		BankName:        odooFieldName(row["bank_id"]),
		Currency:        odooFieldName(row["currency_id"]),
		PartnerID:       odooFieldID(row["partner_id"]),
		PartnerName:     odooFieldName(row["partner_id"]),
	}
}

func buildInvoiceLineItems(lines []map[string]interface{}, analyticsByID, taxesByID map[int]map[string]interface{}) ([]OdooInvoiceLineItem, []string, []string) {
	sort.Slice(lines, func(i, j int) bool { return odooInt(lines[i]["id"]) < odooInt(lines[j]["id"]) })

	var items []OdooInvoiceLineItem
	categorySet := map[string]bool{}
	tagSet := map[string]bool{}

	for _, row := range lines {
		item := OdooInvoiceLineItem{
			ID:             odooInt(row["id"]),
			Title:          odooString(row["name"]),
			ProductID:      odooFieldID(row["product_id"]),
			ProductName:    odooFieldName(row["product_id"]),
			DisplayType:    odooString(row["display_type"]),
			Quantity:       odooFloat(row["quantity"]),
			UnitPrice:      odooFloat(row["price_unit"]),
			SubtotalAmount: odooFloat(row["price_subtotal"]),
			TotalAmount:    odooFloat(row["price_total"]),
		}

		for _, taxID := range odooIDList(row["tax_ids"]) {
			if tax := taxesByID[taxID]; tax != nil {
				item.Taxes = append(item.Taxes, OdooInvoiceTax{
					ID:          taxID,
					Name:        odooString(tax["name"]),
					Description: odooString(tax["description"]),
					Amount:      odooFloat(tax["amount"]),
					AmountType:  odooString(tax["amount_type"]),
				})
				tagSet[odooString(tax["name"])] = true
			}
		}

		item.AnalyticDistribution = buildInvoiceAnalyticSplits(row, analyticsByID)
		for _, split := range item.AnalyticDistribution {
			if split.Category != "" {
				categorySet[split.Category] = true
				item.Categories = append(item.Categories, split.Category)
			}
			if split.AccountName != "" {
				tagSet[split.AccountName] = true
				item.Tags = append(item.Tags, split.AccountName)
			}
		}
		item.Categories = uniqueSortedStrings(item.Categories)
		item.Category = firstNonEmpty(item.Categories...)
		item.Tags = uniqueSortedStrings(item.Tags)
		items = append(items, item)
	}

	return items, sortedKeys(categorySet), sortedKeys(tagSet)
}

func buildInvoiceAnalyticSplits(line map[string]interface{}, analyticsByID map[int]map[string]interface{}) []OdooInvoiceAnalyticSplit {
	distribution := analyticDistributionMap(line["analytic_distribution"])
	if len(distribution) == 0 {
		if id := odooFieldID(line["analytic_account_id"]); id > 0 {
			distribution[id] = 100
		}
	}

	var splits []OdooInvoiceAnalyticSplit
	for _, id := range sortedMapIntKeys(distribution) {
		account := analyticsByID[id]
		split := OdooInvoiceAnalyticSplit{
			AccountID:   id,
			AccountName: odooString(account["name"]),
			AccountCode: odooString(account["code"]),
			Plan:        odooFieldName(account["plan_id"]),
			Percentage:  distribution[id],
		}
		split.Category = analyticCategory(split.AccountCode, split.AccountName)
		splits = append(splits, split)
	}
	return splits
}

func buildInvoiceTransaction(row map[string]interface{}) OdooInvoiceTx {
	providerRef := odooString(row["provider_reference"])
	ref := firstNonEmpty(providerRef, odooString(row["reference"]))
	return OdooInvoiceTx{
		ID:                odooInt(row["id"]),
		Provider:          firstNonEmpty(odooString(row["provider_code"]), inferTxProvider(ref)),
		Reference:         odooString(row["reference"]),
		ProviderReference: providerRef,
		TxHash:            extractTxHash(ref),
		Date:              firstNonEmpty(odooString(row["last_state_change"]), odooString(row["create_date"])),
		Amount:            odooFloat(row["amount"]),
		Currency:          odooFieldName(row["currency_id"]),
		State:             odooString(row["state"]),
		Operation:         odooString(row["operation"]),
	}
}

func parseInvoicePaymentsWidget(raw interface{}) []OdooInvoicePayment {
	var widget string
	switch v := raw.(type) {
	case string:
		widget = v
	case bool:
		if !v {
			return nil
		}
	}
	if widget == "" {
		return nil
	}

	var decoded struct {
		Content []map[string]interface{} `json:"content"`
	}
	if json.Unmarshal([]byte(widget), &decoded) != nil {
		return nil
	}

	var payments []OdooInvoicePayment
	for _, row := range decoded.Content {
		ref := firstNonEmpty(odooString(row["ref"]), odooString(row["name"]))
		payments = append(payments, OdooInvoicePayment{
			PaymentID: odooInt(firstNonNil(row["account_payment_id"], row["payment_id"])),
			MoveID:    odooInt(row["move_id"]),
			Date:      odooString(row["date"]),
			Journal:   odooString(row["journal_name"]),
			Reference: ref,
			Amount:    odooFloat(row["amount"]),
			TxHash:    extractTxHash(ref),
		})
	}
	return payments
}

func fetchOdooDocumentAttachments(creds *OdooCredentials, uid int, invoiceIDs []int) (map[int][]OdooDocumentAttachment, error) {
	if len(invoiceIDs) == 0 {
		return map[int][]OdooDocumentAttachment{}, nil
	}

	rows, err := odooSearchReadAllMaps(
		creds,
		uid,
		"ir.attachment",
		[]interface{}{
			[]interface{}{"res_model", "=", "account.move"},
			[]interface{}{"res_id", "in", intsToInterfaces(invoiceIDs)},
		},
		[]string{"id", "name", "mimetype", "file_size", "checksum", "public", "type", "url", "res_id", "create_date", "write_date", "access_token"},
		"id asc",
	)
	if err != nil {
		return nil, err
	}

	out := map[int][]OdooDocumentAttachment{}
	for _, row := range rows {
		resID := odooInt(row["res_id"])
		if resID == 0 {
			continue
		}
		out[resID] = append(out[resID], buildOdooDocumentAttachment(creds.URL, row))
	}
	for id := range out {
		sort.Slice(out[id], func(i, j int) bool {
			if out[id][i].WriteDate == out[id][j].WriteDate {
				return out[id][i].ID < out[id][j].ID
			}
			return out[id][i].WriteDate < out[id][j].WriteDate
		})
	}
	return out, nil
}

func fetchInvoiceSentDates(creds *OdooCredentials, uid int, invoiceIDs []int) (map[int]string, error) {
	if len(invoiceIDs) == 0 {
		return map[int]string{}, nil
	}

	rows, err := odooSearchReadAllMaps(
		creds,
		uid,
		"mail.message",
		[]interface{}{
			[]interface{}{"model", "=", "account.move"},
			[]interface{}{"res_id", "in", intsToInterfaces(invoiceIDs)},
			[]interface{}{"message_type", "=", "email"},
		},
		[]string{"id", "res_id", "date", "create_date"},
		"date desc, id desc",
	)
	if err != nil {
		return nil, err
	}

	out := map[int]string{}
	for _, row := range rows {
		resID := odooInt(row["res_id"])
		if resID == 0 {
			continue
		}
		date := firstNonEmpty(odooString(row["date"]), odooString(row["create_date"]))
		if date == "" {
			continue
		}
		if current := out[resID]; current == "" || date > current {
			out[resID] = date
		}
	}
	return out, nil
}

func buildOdooDocumentAttachment(odooURL string, row map[string]interface{}) OdooDocumentAttachment {
	att := OdooDocumentAttachment{
		ID:         odooInt(row["id"]),
		Name:       odooString(row["name"]),
		FileName:   odooString(row["name"]),
		Type:       odooString(row["type"]),
		MimeType:   odooString(row["mimetype"]),
		Checksum:   odooString(row["checksum"]),
		Size:       odooInt64(row["file_size"]),
		Public:     odooBool(row["public"]),
		CreateDate: odooString(row["create_date"]),
		WriteDate:  odooString(row["write_date"]),
	}
	att.URL = buildOdooAttachmentURL(odooURL, att.ID, odooString(row["type"]), odooString(row["url"]), odooString(row["access_token"]))
	return att
}

func buildOdooAttachmentURL(odooURL string, attachmentID int, attachmentType, rawURL, accessToken string) string {
	if attachmentType == "url" && rawURL != "" {
		return rawURL
	}
	if attachmentID == 0 || odooURL == "" {
		return rawURL
	}
	u := fmt.Sprintf("%s/web/content/%d?download=true", strings.TrimRight(odooURL, "/"), attachmentID)
	if accessToken != "" {
		u += "&access_token=" + accessToken
	}
	return u
}

func loadOdooLocalTxIndex() *odooLocalTxIndex {
	odooLocalTxIndexMu.Lock()
	if odooLocalTxIndexCache != nil {
		defer odooLocalTxIndexMu.Unlock()
		return odooLocalTxIndexCache
	}
	odooLocalTxIndexMu.Unlock()

	all := loadAllTransactions("")
	idx := &odooLocalTxIndex{
		byHash:     map[string]TransactionEntry{},
		byID:       map[string]TransactionEntry{},
		byChargeID: map[string]TransactionEntry{},
	}
	for _, tx := range all {
		if key := strings.ToLower(strings.TrimSpace(tx.TxHash)); key != "" {
			if _, ok := idx.byHash[key]; !ok {
				idx.byHash[key] = tx
			}
		}
		if key := strings.ToLower(strings.TrimSpace(tx.ID)); key != "" {
			if _, ok := idx.byID[key]; !ok {
				idx.byID[key] = tx
			}
		}
		if key := strings.ToLower(strings.TrimSpace(tx.StripeChargeID)); key != "" {
			if _, ok := idx.byChargeID[key]; !ok {
				idx.byChargeID[key] = tx
			}
		}
	}

	odooLocalTxIndexMu.Lock()
	odooLocalTxIndexCache = idx
	odooLocalTxIndexMu.Unlock()
	return idx
}

func matchOdooReconciledTransaction(idx *odooLocalTxIndex, txs []OdooInvoiceTx, payments []OdooInvoicePayment) *OdooReconciledTransaction {
	if idx != nil {
		for _, key := range candidateReconciledTxKeys(txs, payments) {
			if tx, ok := idx.byHash[key]; ok {
				reconciled := buildLocalReconciledTransaction(tx)
				return &reconciled
			}
			if tx, ok := idx.byID[key]; ok {
				reconciled := buildLocalReconciledTransaction(tx)
				return &reconciled
			}
			if tx, ok := idx.byChargeID[key]; ok {
				reconciled := buildLocalReconciledTransaction(tx)
				return &reconciled
			}
		}
	}

	for _, tx := range txs {
		if tx.Reference == "" && tx.ProviderReference == "" && tx.TxHash == "" {
			continue
		}
		reconciled := OdooReconciledTransaction{
			Source:    "odoo",
			ID:        fmt.Sprintf("odoo:payment.transaction:%d", tx.ID),
			Provider:  firstNonEmpty(tx.Provider, inferTxProvider(tx.ProviderReference), inferTxProvider(tx.Reference)),
			Reference: firstNonEmpty(tx.ProviderReference, tx.Reference),
			TxHash:    tx.TxHash,
			Date:      tx.Date,
			Amount:    tx.Amount,
			Currency:  tx.Currency,
			State:     tx.State,
		}
		return &reconciled
	}

	for _, payment := range payments {
		if payment.Reference == "" && payment.TxHash == "" {
			continue
		}
		reconciled := OdooReconciledTransaction{
			Source:    "odoo",
			ID:        fmt.Sprintf("odoo:payment:%d", payment.PaymentID),
			Provider:  inferTxProvider(payment.Reference),
			Reference: payment.Reference,
			TxHash:    payment.TxHash,
			Date:      payment.Date,
			Amount:    payment.Amount,
		}
		return &reconciled
	}

	return nil
}

func candidateReconciledTxKeys(txs []OdooInvoiceTx, payments []OdooInvoicePayment) []string {
	seen := map[string]bool{}
	var keys []string
	add := func(raw string) {
		key := strings.ToLower(strings.TrimSpace(raw))
		if key == "" || seen[key] {
			return
		}
		seen[key] = true
		keys = append(keys, key)
	}

	for _, tx := range txs {
		add(tx.TxHash)
		add(tx.ProviderReference)
		add(tx.Reference)
	}
	for _, payment := range payments {
		add(payment.TxHash)
		add(payment.Reference)
	}
	return keys
}

func buildLocalReconciledTransaction(tx TransactionEntry) OdooReconciledTransaction {
	date := ""
	if tx.Timestamp > 0 {
		date = time.Unix(tx.Timestamp, 0).UTC().Format(time.RFC3339)
	}
	return OdooReconciledTransaction{
		Source:       "local",
		ID:           tx.ID,
		Provider:     tx.Provider,
		Reference:    firstNonEmpty(tx.StripeChargeID, tx.TxHash, tx.ID),
		TxHash:       tx.TxHash,
		Date:         date,
		Amount:       tx.Amount,
		Currency:     tx.Currency,
		AccountSlug:  tx.AccountSlug,
		AccountName:  tx.AccountName,
		Counterparty: tx.Counterparty,
	}
}

func fetchOdooAttachmentContent(creds *OdooCredentials, uid, attachmentID int) ([]byte, error) {
	rows, err := odooReadMapsByIDs(creds, uid, "ir.attachment", []int{attachmentID}, []string{"datas"})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("attachment %d not found", attachmentID)
	}
	encoded := odooString(rows[0]["datas"])
	if encoded == "" {
		return nil, fmt.Errorf("attachment %d has no binary payload", attachmentID)
	}
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func buildInvoiceTitle(inv OdooOutgoingInvoice) string {
	for _, item := range inv.LineItems {
		if item.DisplayType == "" && item.Title != "" {
			return item.Title
		}
	}
	return firstNonEmpty(inv.Ref, inv.Reference, inv.InvoiceOrigin, inv.Number)
}

func invoiceYearMonth(inv OdooOutgoingInvoice) string {
	date := firstNonEmpty(inv.InvoiceDate, inv.Date)
	if len(date) >= 7 {
		return date[:7]
	}
	return ""
}

func loadCachedInvoiceMonths(dataDir, startMonth, endMonth string) map[string]map[int]OdooOutgoingInvoice {
	result := map[string]map[int]OdooOutgoingInvoice{}
	for _, ym := range expandMonthRange(startMonth, endMonth) {
		parts := strings.Split(ym, "-")
		if len(parts) != 2 {
			continue
		}
		year, month := parts[0], parts[1]
		docs := loadCachedInvoiceMonth(dataDir, year, month)
		if len(docs) == 0 {
			continue
		}
		result[ym] = map[int]OdooOutgoingInvoice{}
		for _, inv := range docs {
			result[ym][inv.ID] = inv
		}
	}
	return result
}

func isInvoiceMonthCacheUnchanged(dataDir, year, month string, nextPublic OdooOutgoingInvoicesFile, nextPrivate OdooOutgoingInvoicesPrivateFile) bool {
	currentPublicPath := filepath.Join(dataDir, year, month, "finance", "odoo", "invoices.json")
	currentPrivatePath := filepath.Join(dataDir, year, month, "finance", "odoo", "private", "invoices.json")

	publicData, err := os.ReadFile(currentPublicPath)
	if err != nil {
		return false
	}
	privateData, err := os.ReadFile(currentPrivatePath)
	if err != nil {
		return false
	}

	var currentPublic OdooOutgoingInvoicesFile
	if json.Unmarshal(publicData, &currentPublic) != nil {
		return false
	}
	var currentPrivate OdooOutgoingInvoicesPrivateFile
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

func maxInvoiceWriteDate(invoices []OdooOutgoingInvoice) string {
	maxWrite := ""
	for _, inv := range invoices {
		if inv.WriteDate > maxWrite {
			maxWrite = inv.WriteDate
		}
	}
	return maxWrite
}

func buildPublicInvoices(invoices []OdooOutgoingInvoice) []OdooOutgoingInvoicePublic {
	out := make([]OdooOutgoingInvoicePublic, 0, len(invoices))
	for _, inv := range invoices {
		out = append(out, OdooOutgoingInvoicePublic{
			ID:                    inv.ID,
			Title:                 inv.Title,
			State:                 inv.State,
			PaymentState:          inv.PaymentState,
			Date:                  firstNonEmpty(inv.InvoiceDate, inv.Date),
			Sent:                  inv.Sent,
			SentAt:                inv.SentAt,
			UntaxedAmount:         inv.UntaxedAmount,
			VATAmount:             inv.VATAmount,
			TotalAmount:           inv.TotalAmount,
			Currency:              inv.Currency,
			Journal:               inv.Journal,
			LineItems:             inv.LineItems,
			ReconciledTransaction: inv.ReconciledTransaction,
			Category:              inv.Category,
			Categories:            inv.Categories,
			Tags:                  inv.Tags,
		})
	}
	return out
}

func buildPrivateInvoices(invoices []OdooOutgoingInvoice) []OdooOutgoingInvoicePrivate {
	out := make([]OdooOutgoingInvoicePrivate, 0, len(invoices))
	for _, inv := range invoices {
		out = append(out, toPrivateInvoice(inv))
	}
	return out
}

func toPrivateInvoice(inv OdooOutgoingInvoice) OdooOutgoingInvoicePrivate {
	return OdooOutgoingInvoicePrivate{
		ID:                 inv.ID,
		Number:             inv.Number,
		Ref:                inv.Ref,
		MoveType:           inv.MoveType,
		State:              inv.State,
		PaymentState:       inv.PaymentState,
		InvoiceDate:        inv.InvoiceDate,
		Date:               inv.Date,
		DueDate:            inv.DueDate,
		Reference:          inv.Reference,
		InvoiceOrigin:      inv.InvoiceOrigin,
		ResidualAmount:     inv.ResidualAmount,
		TotalSignedAmount:  inv.TotalSignedAmount,
		Partner:            inv.Partner,
		PartnerBank:        inv.PartnerBank,
		Transactions:       inv.Transactions,
		Payments:           inv.Payments,
		Attachments:        inv.Attachments,
		WriteDate:          inv.WriteDate,
		CreateDate:         inv.CreateDate,
		InvoiceURL:         inv.InvoiceURL,
		PartnerDisplayName: inv.PartnerDisplayName,
	}
}

func privateInvoiceToInternal(inv *OdooOutgoingInvoicePrivate) *OdooOutgoingInvoice {
	if inv == nil {
		return &OdooOutgoingInvoice{}
	}
	return &OdooOutgoingInvoice{
		ID:                 inv.ID,
		Number:             inv.Number,
		Ref:                inv.Ref,
		MoveType:           inv.MoveType,
		State:              inv.State,
		PaymentState:       inv.PaymentState,
		InvoiceDate:        inv.InvoiceDate,
		Date:               inv.Date,
		DueDate:            inv.DueDate,
		Reference:          inv.Reference,
		InvoiceOrigin:      inv.InvoiceOrigin,
		ResidualAmount:     inv.ResidualAmount,
		TotalSignedAmount:  inv.TotalSignedAmount,
		Partner:            inv.Partner,
		PartnerBank:        inv.PartnerBank,
		Transactions:       inv.Transactions,
		Payments:           inv.Payments,
		Attachments:        inv.Attachments,
		WriteDate:          inv.WriteDate,
		CreateDate:         inv.CreateDate,
		InvoiceURL:         inv.InvoiceURL,
		PartnerDisplayName: inv.PartnerDisplayName,
	}
}

func loadCachedInvoiceMonth(dataDir, year, month string) []OdooOutgoingInvoice {
	publicPath := filepath.Join(dataDir, year, month, "finance", "odoo", "invoices.json")
	privatePath := filepath.Join(dataDir, year, month, "finance", "odoo", "private", "invoices.json")

	publicByID := map[int]OdooOutgoingInvoice{}
	privateByID := map[int]OdooOutgoingInvoice{}

	if data, err := os.ReadFile(publicPath); err == nil {
		var file OdooOutgoingInvoicesFile
		if json.Unmarshal(data, &file) == nil {
			for _, inv := range file.Invoices {
				publicByID[inv.ID] = OdooOutgoingInvoice{
					ID:                    inv.ID,
					Title:                 inv.Title,
					State:                 inv.State,
					PaymentState:          inv.PaymentState,
					InvoiceDate:           inv.Date,
					Date:                  inv.Date,
					Sent:                  inv.Sent,
					SentAt:                inv.SentAt,
					UntaxedAmount:         inv.UntaxedAmount,
					VATAmount:             inv.VATAmount,
					TotalAmount:           inv.TotalAmount,
					Currency:              inv.Currency,
					Journal:               inv.Journal,
					LineItems:             inv.LineItems,
					ReconciledTransaction: inv.ReconciledTransaction,
					Category:              inv.Category,
					Categories:            inv.Categories,
					Tags:                  inv.Tags,
				}
			}
		}
	}

	if data, err := os.ReadFile(privatePath); err == nil {
		var file OdooOutgoingInvoicesPrivateFile
		if json.Unmarshal(data, &file) == nil {
			for _, inv := range file.Invoices {
				privateByID[inv.ID] = OdooOutgoingInvoice{
					ID:                 inv.ID,
					Number:             inv.Number,
					Ref:                inv.Ref,
					MoveType:           inv.MoveType,
					State:              inv.State,
					PaymentState:       inv.PaymentState,
					InvoiceDate:        inv.InvoiceDate,
					Date:               inv.Date,
					DueDate:            inv.DueDate,
					Reference:          inv.Reference,
					InvoiceOrigin:      inv.InvoiceOrigin,
					ResidualAmount:     inv.ResidualAmount,
					TotalSignedAmount:  inv.TotalSignedAmount,
					Partner:            inv.Partner,
					PartnerBank:        inv.PartnerBank,
					Transactions:       inv.Transactions,
					Payments:           inv.Payments,
					Attachments:        inv.Attachments,
					WriteDate:          inv.WriteDate,
					CreateDate:         inv.CreateDate,
					InvoiceURL:         inv.InvoiceURL,
					PartnerDisplayName: inv.PartnerDisplayName,
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

func mergePrivateOdooDocument(dst *OdooOutgoingInvoice, src OdooOutgoingInvoice) {
	if dst.ID == 0 {
		dst.ID = src.ID
	}
	if src.Number != "" {
		dst.Number = src.Number
	}
	if src.Ref != "" {
		dst.Ref = src.Ref
	}
	if src.MoveType != "" {
		dst.MoveType = src.MoveType
	}
	if src.State != "" {
		dst.State = src.State
	}
	if src.PaymentState != "" {
		dst.PaymentState = src.PaymentState
	}
	if src.InvoiceDate != "" {
		dst.InvoiceDate = src.InvoiceDate
	}
	if src.Date != "" {
		dst.Date = src.Date
	}
	if src.DueDate != "" {
		dst.DueDate = src.DueDate
	}
	if src.Reference != "" {
		dst.Reference = src.Reference
	}
	if src.InvoiceOrigin != "" {
		dst.InvoiceOrigin = src.InvoiceOrigin
	}
	if src.ResidualAmount != 0 {
		dst.ResidualAmount = src.ResidualAmount
	}
	if src.TotalSignedAmount != 0 {
		dst.TotalSignedAmount = src.TotalSignedAmount
	}
	if src.Partner.ID != 0 || src.Partner.Name != "" {
		dst.Partner = src.Partner
	}
	if src.PartnerBank != nil {
		dst.PartnerBank = src.PartnerBank
	}
	if len(src.Transactions) > 0 {
		dst.Transactions = src.Transactions
	}
	if len(src.Payments) > 0 {
		dst.Payments = src.Payments
	}
	if len(src.Attachments) > 0 {
		dst.Attachments = src.Attachments
	}
	if src.WriteDate != "" {
		dst.WriteDate = src.WriteDate
	}
	if src.CreateDate != "" {
		dst.CreateDate = src.CreateDate
	}
	if src.InvoiceURL != "" {
		dst.InvoiceURL = src.InvoiceURL
	}
	if src.PartnerDisplayName != "" {
		dst.PartnerDisplayName = src.PartnerDisplayName
	}
}

func odooString(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func odooFloat(v interface{}) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int:
		return float64(n)
	case json.Number:
		f, _ := n.Float64()
		return f
	default:
		return 0
	}
}

func odooInt(v interface{}) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case json.Number:
		i, _ := n.Int64()
		return int(i)
	default:
		return 0
	}
}

func odooInt64(v interface{}) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int:
		return int64(n)
	case int64:
		return n
	case json.Number:
		i, _ := n.Int64()
		return i
	}
	return 0
}

func odooBool(v interface{}) bool {
	if b, ok := v.(bool); ok {
		return b
	}
	return false
}

func odooIDList(v interface{}) []int {
	arr, ok := v.([]interface{})
	if !ok {
		return nil
	}
	var ids []int
	for _, item := range arr {
		if id := odooInt(item); id > 0 {
			ids = append(ids, id)
		}
	}
	return ids
}

func analyticDistributionMap(v interface{}) map[int]float64 {
	raw, ok := v.(map[string]interface{})
	if !ok {
		return map[int]float64{}
	}
	out := map[int]float64{}
	for key, val := range raw {
		id, err := strconv.Atoi(key)
		if err != nil {
			continue
		}
		out[id] = odooFloat(val)
	}
	return out
}

func analyticDistributionIDs(v interface{}) []int {
	dist := analyticDistributionMap(v)
	return sortedMapIntKeys(dist)
}

func analyticCategory(code, name string) string {
	base := firstNonEmpty(strings.TrimSpace(code), strings.TrimSpace(name))
	if base == "" {
		return ""
	}
	base = strings.ToLower(base)
	base = strings.ReplaceAll(base, " ", "-")
	return base
}

func extractTxHash(s string) string {
	if s == "" {
		return ""
	}
	for _, part := range strings.FieldsFunc(s, func(r rune) bool {
		return r == ':' || r == '/' || r == ' ' || r == ',' || r == ';'
	}) {
		if strings.HasPrefix(part, "0x") && len(part) >= 18 {
			return part
		}
	}
	if strings.HasPrefix(s, "0x") && len(s) >= 18 {
		return s
	}
	return ""
}

func inferTxProvider(ref string) string {
	if strings.Contains(ref, "stripe") || strings.HasPrefix(ref, "ch_") || strings.HasPrefix(ref, "pi_") || strings.HasPrefix(ref, "txn_") || strings.HasPrefix(ref, "po_") || strings.HasPrefix(ref, "re_") {
		return "stripe"
	}
	if strings.Contains(ref, "gnosis:") {
		return "gnosis"
	}
	if strings.Contains(ref, "celo:") {
		return "celo"
	}
	if strings.Contains(ref, "monerium") {
		return "monerium"
	}
	return ""
}

func sortedIDSet(set map[int]bool) []int {
	var ids []int
	for id := range set {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	return ids
}

func intsToInterfaces(ids []int) []interface{} {
	out := make([]interface{}, 0, len(ids))
	for _, id := range ids {
		out = append(out, id)
	}
	return out
}

func sortedMapIntKeys[V any](m map[int]V) []int {
	var ids []int
	for id := range m {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	return ids
}

func uniqueSortedStrings(values []string) []string {
	set := map[string]bool{}
	for _, v := range values {
		if strings.TrimSpace(v) == "" {
			continue
		}
		set[v] = true
	}
	return sortedKeys(set)
}

func sortedKeys(set map[string]bool) []string {
	var out []string
	for k := range set {
		if k != "" {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func firstNonNil(values ...interface{}) interface{} {
	for _, v := range values {
		if v != nil {
			return v
		}
	}
	return nil
}

func printInvoicesSyncHelp() {
	f := Fmt
	fmt.Printf(`
%schb invoices sync%s — Fetch outgoing invoices from Odoo

%sUSAGE%s
  %schb invoices sync%s [year[/month]] [options]

%sOPTIONS%s
  %s<year>%s               Sync all months of a year (e.g. 2025)
  %s<year/month>%s         Sync a specific month (e.g. 2025/03)
  %s--since%s YYYY/MM      Sync from a specific month to now
  %s--history%s            Sync invoice history from the oldest cached month
  %s--force%s              Re-fetch and overwrite cached month files
  %s--help, -h%s           Show this help

%sDATA%s
  Saves monthly invoice snapshots to:
    ~/.chb/data/YYYY/MM/finance/odoo/invoices.json
    ~/.chb/data/YYYY/MM/finance/odoo/private/invoices.json

  Each invoice includes:
  • public: date, status, payment status, amounts, title, line items, VAT, categories, tags, journal, reconciled transaction
  • private: partner details and attachments

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
