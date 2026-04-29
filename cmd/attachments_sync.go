package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type attachmentSyncResult struct {
	Downloaded int
	Skipped    int
	Domains    map[string]int
}

func AttachmentsSync(args []string) (int, error) {
	if HasFlag(args, "--help", "-h", "help") {
		printAttachmentsSyncHelp()
		return 0, nil
	}

	creds, err := ResolveOdooCredentials()
	if err != nil {
		Warnf("%s⚠ %v, skipping attachments sync%s", Fmt.Yellow, err, Fmt.Reset)
		return 0, nil
	}

	dataDir := DataDir()
	force := HasFlag(args, "--force")
	startMonth, isHistory := ResolveSinceMonth(args, filepath.Join("finance", "odoo"))
	posYear, posMonth, posFound := ParseYearMonthArg(args)

	years := getAvailableYears(dataDir)
	if len(years) == 0 {
		Warnf("%s⚠ No data found. Run sync first.%s", Fmt.Yellow, Fmt.Reset)
		return 0, nil
	}

	uid, err := odooAuth(creds.URL, creds.DB, creds.Login, creds.Password)
	if err != nil || uid == 0 {
		return 0, fmt.Errorf("Odoo authentication failed: %v", err)
	}

	fmt.Printf("\n%s📎 Syncing Odoo attachments...%s\n", Fmt.Bold, Fmt.Reset)
	fmt.Printf("%sDATA_DIR: %s%s\n", Fmt.Dim, dataDir, Fmt.Reset)

	totalDownloaded := 0
	totalSkipped := 0
	domainCounts := map[string]int{}

	for _, scope := range collectAttachmentSyncScopes(dataDir, years, posYear, posMonth, posFound, startMonth, isHistory) {
		invoiceRes, err := syncInvoiceAttachmentMonth(dataDir, scope.Year, scope.Month, force, creds, uid)
		if err != nil {
			Warnf("  %s⚠ %s invoices: %v%s", Fmt.Yellow, scope.Label, err, Fmt.Reset)
		}
		billRes, err := syncBillAttachmentMonth(dataDir, scope.Year, scope.Month, force, creds, uid)
		if err != nil {
			Warnf("  %s⚠ %s bills: %v%s", Fmt.Yellow, scope.Label, err, Fmt.Reset)
		}

		totalDownloaded += invoiceRes.Downloaded + billRes.Downloaded
		totalSkipped += invoiceRes.Skipped + billRes.Skipped
		for domain, count := range invoiceRes.Domains {
			domainCounts[domain] += count
		}
		for domain, count := range billRes.Domains {
			domainCounts[domain] += count
		}
	}

	fmt.Printf("\n%s✅ Attachments sync complete%s\n", Fmt.Green, Fmt.Reset)
	if totalDownloaded > 0 || totalSkipped > 0 {
		fmt.Printf("  Odoo attachments: %d downloaded, %d skipped", totalDownloaded, totalSkipped)
		if summary := formatDomainSummary(domainCounts); summary != "" {
			fmt.Printf(" (%s)", summary)
		}
		fmt.Println()
	} else {
		fmt.Printf("  No attachments found to sync\n")
	}
	fmt.Println()

	UpdateSyncSource("attachments", isHistory)
	UpdateSyncActivity(isHistory)
	return totalDownloaded, nil
}

func collectAttachmentSyncScopes(dataDir string, years []string, posYear, posMonth string, posFound bool, startMonth string, isHistory bool) []imageSyncScope {
	var scopes []imageSyncScope
	seen := map[string]bool{}

	addScope := func(year, month, label string) {
		key := year + "/" + month
		if seen[key] {
			return
		}
		seen[key] = true
		scopes = append(scopes, imageSyncScope{Year: year, Month: month, Label: label})
	}

	if posFound || startMonth != "" {
		for _, year := range years {
			for _, month := range getAvailableMonths(dataDir, year) {
				ym := fmt.Sprintf("%s-%s", year, month)
				if posFound {
					if posMonth != "" && (year != posYear || month != posMonth) {
						continue
					}
					if posMonth == "" && year != posYear {
						continue
					}
				}
				if startMonth != "" && ym < startMonth {
					continue
				}
				addScope(year, month, year+"-"+month)
			}
		}
		return scopes
	}

	now := time.Now().In(BrusselsTZ())
	prev := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, BrusselsTZ()).AddDate(0, -1, 0)
	addScope(fmt.Sprintf("%d", prev.Year()), fmt.Sprintf("%02d", prev.Month()), prev.Format("2006-01"))
	addScope(fmt.Sprintf("%d", now.Year()), fmt.Sprintf("%02d", now.Month()), now.Format("2006-01"))
	return scopes
}

func syncInvoiceAttachmentMonth(dataDir, year, month string, force bool, creds *OdooCredentials, uid int) (attachmentSyncResult, error) {
	path := filepath.Join(dataDir, year, month, "finance", "odoo", "private", "invoices.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return attachmentSyncResult{}, nil
	}

	var file OdooOutgoingInvoicesPrivateFile
	if err := json.Unmarshal(data, &file); err != nil {
		return attachmentSyncResult{}, err
	}

	docs := make([]*OdooOutgoingInvoice, 0, len(file.Invoices))
	for i := range file.Invoices {
		docs = append(docs, privateInvoiceToInternal(&file.Invoices[i]))
	}
	res, changed, err := syncDocumentAttachments(dataDir, year, month, "invoices", docs, force, creds, uid)
	if err != nil {
		return res, err
	}
	if changed {
		for i := range docs {
			file.Invoices[i] = toPrivateInvoice(*docs[i])
		}
		updated, err := marshalIndentedNoHTMLEscape(file)
		if err == nil {
			if err := writeDataFile(path, updated); err != nil {
				return res, err
			}
		}
	}
	if res.Downloaded > 0 {
		fmt.Printf("  ✓ %s-%s invoice attachments: %d downloaded\n", year, month, res.Downloaded)
	}
	return res, nil
}

func syncBillAttachmentMonth(dataDir, year, month string, force bool, creds *OdooCredentials, uid int) (attachmentSyncResult, error) {
	path := filepath.Join(dataDir, year, month, "finance", "odoo", "private", "bills.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return attachmentSyncResult{}, nil
	}

	var file OdooVendorBillsPrivateFile
	if err := json.Unmarshal(data, &file); err != nil {
		return attachmentSyncResult{}, err
	}

	docs := make([]*OdooOutgoingInvoice, 0, len(file.Bills))
	for i := range file.Bills {
		docs = append(docs, privateInvoiceToInternal(&file.Bills[i]))
	}
	res, changed, err := syncDocumentAttachments(dataDir, year, month, "bills", docs, force, creds, uid)
	if err != nil {
		return res, err
	}
	if changed {
		for i := range docs {
			file.Bills[i] = toPrivateInvoice(*docs[i])
		}
		updated, err := marshalIndentedNoHTMLEscape(file)
		if err == nil {
			if err := writeDataFile(path, updated); err != nil {
				return res, err
			}
		}
	}
	if res.Downloaded > 0 {
		fmt.Printf("  ✓ %s-%s bill attachments: %d downloaded\n", year, month, res.Downloaded)
	}
	return res, nil
}

func syncDocumentAttachments(dataDir, year, month, docKind string, docs []*OdooOutgoingInvoice, force bool, creds *OdooCredentials, uid int) (attachmentSyncResult, bool, error) {
	res := attachmentSyncResult{Domains: map[string]int{}}
	changed := false

	for _, doc := range docs {
		for i := range doc.Attachments {
			att := &doc.Attachments[i]
			if att.ID == 0 && att.URL == "" {
				continue
			}

			if domain := hostFromURL(att.URL); domain != "" {
				res.Domains[domain]++
			}

			localRelPath := buildAttachmentLocalPath(year, month, docKind, doc.ID, *att)
			localAbsPath := filepath.Join(dataDir, filepath.FromSlash(localRelPath))
			if !force && att.LocalPath != "" {
				existingPath := filepath.Join(dataDir, filepath.FromSlash(att.LocalPath))
				if fileExists(existingPath) {
					if att.LocalPath != localRelPath {
						att.LocalPath = localRelPath
						changed = true
					}
					res.Skipped++
					continue
				}
			}
			if !force && fileExists(localAbsPath) {
				if att.LocalPath != localRelPath {
					att.LocalPath = localRelPath
					changed = true
				}
				res.Skipped++
				continue
			}

			if err := mkdirAllManagedData(filepath.Dir(localAbsPath)); err != nil {
				return res, changed, err
			}

			if err := downloadOdooAttachment(localAbsPath, *att, creds, uid); err != nil {
				Warnf("  %s⚠ Failed to download attachment %d: %v%s", Fmt.Yellow, att.ID, err, Fmt.Reset)
				continue
			}

			att.LocalPath = localRelPath
			changed = true
			res.Downloaded++
		}
	}

	return res, changed, nil
}

func downloadOdooAttachment(destPath string, att OdooDocumentAttachment, creds *OdooCredentials, uid int) error {
	if att.Type == "url" && att.URL != "" {
		return downloadFile(att.URL, destPath)
	}
	if att.ID == 0 {
		return fmt.Errorf("missing attachment ID")
	}
	data, err := fetchOdooAttachmentContent(creds, uid, att.ID)
	if err != nil {
		return err
	}
	return writeDataFile(destPath, data)
}

func buildAttachmentLocalPath(year, month, docKind string, docID int, att OdooDocumentAttachment) string {
	ext := strings.ToLower(filepath.Ext(firstNonEmpty(att.FileName, att.Name)))
	if ext == "" {
		ext = extFromURL(att.URL, "")
	}
	if ext == "" {
		ext = mimeDefaultExt(att.MimeType)
	}
	if ext == "" {
		ext = ".bin"
	}

	base := "attachment"
	if att.ID > 0 {
		base = fmt.Sprintf("%d", att.ID)
	}
	return filepath.ToSlash(filepath.Join(year, month, "finance", "odoo", "private", "attachments", docKind, fmt.Sprintf("%d", docID), base+ext))
}

func mimeDefaultExt(mimeType string) string {
	switch strings.ToLower(mimeType) {
	case "application/pdf":
		return ".pdf"
	case "image/jpeg":
		return ".jpg"
	case "image/png":
		return ".png"
	case "image/webp":
		return ".webp"
	case "text/plain":
		return ".txt"
	case "text/csv":
		return ".csv"
	default:
		return ""
	}
}

func printAttachmentsSyncHelp() {
	f := Fmt
	fmt.Printf(`
%schb attachments sync%s — Download invoice and bill attachments from Odoo to the local data directory

%sUSAGE%s
  %schb attachments sync%s [year[/month]] [options]

%sDESCRIPTION%s
  By default, processes the current month and previous month.
  With %s--history%s, processes all historical months with Odoo finance data.

  Reads:
    data/YYYY/MM/finance/odoo/private/invoices.json
    data/YYYY/MM/finance/odoo/private/bills.json

  Downloads listed attachment binaries or URL attachments and stores them under:
    data/YYYY/MM/finance/odoo/private/attachments/{invoices|bills}/{documentId}/{attachmentId}.{ext}

  Existing files are skipped unless --force is used.

%sOPTIONS%s
  %s<year>%s               Process all months of the given year (e.g. 2025)
  %s<year/month>%s         Process a specific month (e.g. 2025/11)
  %s--since%s <YYYY/MM>    Process from a specific month to now
  %s--history%s            Process all available months
  %s--force%s              Re-download even if files already exist
  %s--help, -h%s           Show this help

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
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
	)
}
