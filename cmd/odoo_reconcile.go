package cmd

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"net/http"
	"os"
	"strings"
	"time"
)

const odooInternalTransferAccountCode = "580000"

type odooReconcileStats struct {
	Scanned           int
	Reconciled        int
	InternalTransfers int
	Ambiguous         int
	NoPartner         int
	NoMatch           int
	Errors            int
	DryRun            bool
	Details           []string
	Rows              []odooReconcileReviewRow
}

type odooStatementLineForReconcile struct {
	ID             int
	Date           string
	Amount         float64
	PaymentRef     string
	UniqueImportID string
	PartnerID      int
	PartnerName    string
	PartnerBankID  int
	MoveID         int
	IsReconciled   bool
}

type odooMoveCandidate struct {
	ID             int
	Name           string
	InvoiceDate    string
	Date           string
	MoveType       string
	PartnerName    string
	AmountResidual float64
}

type odooLineReconcileResult struct {
	Reconciled        bool
	InternalTransfer  bool
	Ambiguous         bool
	NoPartner         bool
	NoMatch           bool
	Err               error
	Message           string
	CandidateCount    int
	CandidateMoveName string
}

type odooReconcileReviewRow struct {
	Date           string
	Counterparty   string
	Amount         float64
	PotentialMatch string
	Reason         string
}

func odooJournalReconcile(creds *OdooCredentials, uid int, journalID int, assumeYes, dryRun bool) error {
	lines, err := fetchJournalUnreconciledStatementLines(creds, uid, journalID)
	if err != nil {
		return err
	}
	stats := &odooReconcileStats{DryRun: dryRun}
	fmt.Printf("\n  %sReconciling journal #%d%s\n", Fmt.Bold, journalID, Fmt.Reset)
	fmt.Printf("  %s%d unreconciled statement line(s)%s\n\n", Fmt.Dim, len(lines), Fmt.Reset)
	if len(lines) == 0 {
		return nil
	}

	if !assumeYes && !dryRun {
		fmt.Printf("  %sThis will reconcile unambiguous matches and mark detected internal transfers on Odoo.%s [y/N] ",
			Fmt.Bold, Fmt.Reset)
		reader := bufio.NewReader(os.Stdin)
		resp, _ := reader.ReadString('\n')
		resp = strings.TrimSpace(strings.ToLower(resp))
		if resp != "y" && resp != "yes" {
			fmt.Println("  Aborted.")
			return nil
		}
	}

	for _, line := range lines {
		amountCandidates, amountErr := findOpenMoveCandidatesByAmountForStatementLine(creds, uid, line)
		result := tryReconcileStatementLine(creds, uid, line, dryRun)
		recordOdooReconcileResult(stats, line, result, amountCandidates, amountErr)
	}
	printOdooReconcileStats(stats)
	return nil
}

func reconcileCreatedStatementLine(creds *OdooCredentials, uid int, lineID int, dryRun bool, stats *syncStats) {
	if lineID == 0 || stats == nil || dryRun {
		return
	}
	line, err := readStatementLineForReconcile(creds, uid, lineID)
	if err != nil {
		stats.ReconcileErrors++
		stats.ReconcileDetails = append(stats.ReconcileDetails, fmt.Sprintf("line #%d: %v", lineID, err))
		return
	}
	result := tryReconcileStatementLine(creds, uid, line, false)
	recordSyncReconcileResult(stats, line, result)
}

func reconcileCreatedStatementLines(creds *OdooCredentials, uid int, lineIDs []int, dryRun bool, stats *syncStats) {
	status := newStatusLine()
	if !quietOdooContext() && len(lineIDs) > 1 {
		status.Update("Reconciling statement lines 0/%d", len(lineIDs))
		defer status.Clear()
	}
	for i, lineID := range lineIDs {
		if !quietOdooContext() && len(lineIDs) > 1 {
			status.Update("Reconciling statement lines %d/%d", i+1, len(lineIDs))
		}
		reconcileCreatedStatementLine(creds, uid, lineID, dryRun, stats)
	}
}

func reconcileCreatedStatementLinesByImportID(creds *OdooCredentials, uid int, importIDs []string, dryRun bool, stats *syncStats) {
	if len(importIDs) == 0 || dryRun || stats == nil {
		return
	}
	lines, err := fetchStatementLinesByImportID(creds, uid, importIDs)
	if err != nil {
		stats.ReconcileErrors++
		stats.ReconcileDetails = append(stats.ReconcileDetails, fmt.Sprintf("fetch created lines: %v", err))
		return
	}
	for _, line := range lines {
		result := tryReconcileStatementLine(creds, uid, line, false)
		recordSyncReconcileResult(stats, line, result)
	}
}

func markCreatedStatementLinesInternal(creds *OdooCredentials, uid int, lineIDs []int, dryRun bool, stats *syncStats) {
	if len(lineIDs) == 0 || dryRun || stats == nil {
		return
	}
	for _, lineID := range lineIDs {
		line, err := readStatementLineForReconcile(creds, uid, lineID)
		if err != nil {
			stats.ReconcileErrors++
			stats.ReconcileDetails = append(stats.ReconcileDetails, fmt.Sprintf("line #%d: %v", lineID, err))
			continue
		}
		if err := markStatementLineInternalTransfer(creds, uid, line, false); err != nil {
			stats.ReconcileErrors++
			stats.ReconcileDetails = append(stats.ReconcileDetails, fmt.Sprintf("line #%d internal transfer: %v", line.ID, err))
			continue
		}
		stats.InternalTransfers++
	}
}

func recordSyncReconcileResult(stats *syncStats, line odooStatementLineForReconcile, result odooLineReconcileResult) {
	switch {
	case result.Err != nil:
		stats.ReconcileErrors++
		stats.ReconcileDetails = append(stats.ReconcileDetails, formatOdooReconcileDetail(line, result))
	case result.InternalTransfer:
		stats.InternalTransfers++
	case result.Reconciled:
		stats.LinesReconciled++
	case result.Ambiguous:
		stats.ReconcileAmbiguous++
		stats.ReconcileDetails = append(stats.ReconcileDetails, formatOdooReconcileDetail(line, result))
	case result.NoPartner:
		stats.ReconcileNoPartner++
	case result.NoMatch:
		stats.ReconcileNoMatch++
	}
}

func recordOdooReconcileResult(stats *odooReconcileStats, line odooStatementLineForReconcile, result odooLineReconcileResult, amountCandidates []odooMoveCandidate, amountErr error) {
	stats.Scanned++
	stats.Rows = append(stats.Rows, odooReconcileReviewRow{
		Date:           line.Date,
		Counterparty:   odooStatementLineCounterparty(line),
		Amount:         line.Amount,
		PotentialMatch: formatOdooPotentialMatch(amountCandidates, amountErr),
		Reason:         odooReconcileReason(result, amountErr),
	})
	switch {
	case result.Err != nil:
		stats.Errors++
		stats.Details = append(stats.Details, formatOdooReconcileDetail(line, result))
	case result.InternalTransfer:
		stats.InternalTransfers++
		if stats.DryRun {
			stats.Details = append(stats.Details, formatOdooReconcileDetail(line, result))
		}
	case result.Reconciled:
		stats.Reconciled++
		if stats.DryRun {
			stats.Details = append(stats.Details, formatOdooReconcileDetail(line, result))
		}
	case result.Ambiguous:
		stats.Ambiguous++
		stats.Details = append(stats.Details, formatOdooReconcileDetail(line, result))
	case result.NoPartner:
		stats.NoPartner++
	case result.NoMatch:
		stats.NoMatch++
	}
}

func tryReconcileStatementLine(creds *OdooCredentials, uid int, line odooStatementLineForReconcile, dryRun bool) odooLineReconcileResult {
	if line.IsReconciled {
		return odooLineReconcileResult{}
	}
	if looksLikeInternalTransferLine(line) {
		if dryRun {
			return odooLineReconcileResult{InternalTransfer: true, Message: "would mark as internal transfer"}
		}
		if err := markStatementLineInternalTransfer(creds, uid, line, false); err != nil {
			return odooLineReconcileResult{Err: err, Message: "mark internal transfer"}
		}
		return odooLineReconcileResult{InternalTransfer: true, Message: "marked as internal transfer"}
	}

	partnerID, err := partnerIDForStatementLine(creds, uid, line, !dryRun)
	if err != nil {
		return odooLineReconcileResult{Err: err, Message: "lookup partner"}
	}
	if partnerID == 0 {
		return odooLineReconcileResult{NoPartner: true, Message: "no partner or partner bank account"}
	}

	candidates, err := findOpenMoveCandidatesForStatementLine(creds, uid, line, partnerID)
	if err != nil {
		return odooLineReconcileResult{Err: err, Message: "find matching invoice/bill"}
	}
	if len(candidates) == 0 {
		return odooLineReconcileResult{NoMatch: true, Message: "no matching open invoice/bill"}
	}
	if len(candidates) > 1 {
		return odooLineReconcileResult{
			Ambiguous:      true,
			CandidateCount: len(candidates),
			Message:        fmt.Sprintf("%d matching open invoices/bills", len(candidates)),
		}
	}
	candidate := candidates[0]
	if dryRun {
		return odooLineReconcileResult{
			Reconciled:        true,
			Message:           "would reconcile",
			CandidateMoveName: candidateDisplayName(candidate),
		}
	}
	if err := reconcileStatementLineWithMove(creds, uid, line, candidate); err != nil {
		return odooLineReconcileResult{Err: err, Message: fmt.Sprintf("reconcile with %s", candidateDisplayName(candidate))}
	}
	return odooLineReconcileResult{
		Reconciled:        true,
		Message:           "reconciled",
		CandidateMoveName: candidateDisplayName(candidate),
	}
}

func fetchJournalUnreconciledStatementLines(creds *OdooCredentials, uid int, journalID int) ([]odooStatementLineForReconcile, error) {
	rows, err := odooSearchReadAllMaps(creds, uid, "account.bank.statement.line",
		[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
			[]interface{}{"is_reconciled", "=", false},
		},
		statementLineReconcileFields(),
		"date asc, id asc",
	)
	if err != nil {
		return nil, fmt.Errorf("fetch unreconciled statement lines: %v", err)
	}
	return parseStatementLineRows(rows), nil
}

func fetchStatementLinesByImportID(creds *OdooCredentials, uid int, importIDs []string) ([]odooStatementLineForReconcile, error) {
	values := make([]interface{}, 0, len(importIDs))
	seen := map[string]bool{}
	for _, id := range importIDs {
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		values = append(values, id)
	}
	if len(values) == 0 {
		return nil, nil
	}
	rows, err := odooSearchReadAllMaps(creds, uid, "account.bank.statement.line",
		[]interface{}{[]interface{}{"unique_import_id", "in", values}},
		statementLineReconcileFields(),
		"date asc, id asc",
	)
	if err != nil {
		return nil, err
	}
	return parseStatementLineRows(rows), nil
}

func readStatementLineForReconcile(creds *OdooCredentials, uid int, lineID int) (odooStatementLineForReconcile, error) {
	rows, err := odooReadMapsByIDs(creds, uid, "account.bank.statement.line", []int{lineID}, statementLineReconcileFields())
	if err != nil {
		return odooStatementLineForReconcile{}, err
	}
	lines := parseStatementLineRows(rows)
	if len(lines) == 0 {
		return odooStatementLineForReconcile{}, fmt.Errorf("statement line #%d not found", lineID)
	}
	return lines[0], nil
}

func statementLineReconcileFields() []string {
	return []string{
		"id", "date", "amount", "payment_ref", "unique_import_id",
		"partner_id", "partner_bank_id", "move_id", "is_reconciled",
	}
}

func parseStatementLineRows(rows []map[string]interface{}) []odooStatementLineForReconcile {
	out := make([]odooStatementLineForReconcile, 0, len(rows))
	for _, row := range rows {
		out = append(out, odooStatementLineForReconcile{
			ID:             odooInt(row["id"]),
			Date:           odooString(row["date"]),
			Amount:         odooFloat(row["amount"]),
			PaymentRef:     odooString(row["payment_ref"]),
			UniqueImportID: odooString(row["unique_import_id"]),
			PartnerID:      odooFieldID(row["partner_id"]),
			PartnerName:    odooFieldName(row["partner_id"]),
			PartnerBankID:  odooFieldID(row["partner_bank_id"]),
			MoveID:         odooFieldID(row["move_id"]),
			IsReconciled:   odooBool(row["is_reconciled"]),
		})
	}
	return out
}

func partnerIDForStatementLine(creds *OdooCredentials, uid int, line odooStatementLineForReconcile, writeBack bool) (int, error) {
	if line.PartnerID > 0 {
		return line.PartnerID, nil
	}
	if line.PartnerBankID == 0 {
		return 0, nil
	}
	rows, err := odooReadMapsByIDs(creds, uid, "res.partner.bank", []int{line.PartnerBankID}, []string{"partner_id", "acc_number", "sanitized_acc_number"})
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}
	partnerID := odooFieldID(rows[0]["partner_id"])
	if partnerID > 0 && writeBack {
		_, _ = odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.bank.statement.line", "write",
			[]interface{}{[]interface{}{line.ID}, map[string]interface{}{"partner_id": partnerID}}, nil)
	}
	return partnerID, nil
}

func resolveOdooPartnerBankForTransaction(creds *OdooCredentials, uid int, tx TransactionEntry) (int, int) {
	if isEVMAddress(tx.Counterparty) {
		bankID, partnerID, err := resolveOdooCryptoPartnerBank(creds, uid, tx)
		if err == nil || partnerID > 0 {
			return bankID, partnerID
		}
	}
	accountNumber := transactionBankAccountNumber(tx)
	if accountNumber == "" {
		return 0, 0
	}
	bankID, partnerID, err := findOdooPartnerBankByAccountNumber(creds, uid, accountNumber)
	if err != nil {
		return 0, 0
	}
	return bankID, partnerID
}

func resolveOdooCryptoPartnerBank(creds *OdooCredentials, uid int, tx TransactionEntry) (int, int, error) {
	address := normalizeEVMAddress(tx.Counterparty)
	chain := transactionChain(tx)
	accountNumber := cryptoBankAccountNumber(chain, address)
	bankID, partnerID, err := findOdooPartnerBankByAccountNumber(creds, uid, accountNumber)
	if err != nil {
		return 0, 0, err
	}
	if bankID > 0 && partnerID > 0 {
		return bankID, partnerID, nil
	}
	// Backward compatibility for any manually-created partner banks that
	// stored the bare address before CHB started using chain-prefixed keys.
	bankID, partnerID, err = findOdooPartnerBankByAccountNumber(creds, uid, address)
	if err != nil {
		return 0, 0, err
	}
	if bankID > 0 && partnerID > 0 {
		return bankID, partnerID, nil
	}

	name := cryptoCounterpartyName(tx, chain, address)
	partnerID, err = createOdooPartner(creds, uid, name)
	if err != nil || partnerID == 0 {
		return 0, 0, err
	}
	bankID, err = createOdooPartnerBank(creds, uid, partnerID, accountNumber)
	if err != nil {
		return 0, 0, err
	}
	return bankID, partnerID, nil
}

func ensureOdooStatementLinePartnerBank(creds *OdooCredentials, uid int, journalID int, importID string, tx TransactionEntry) (bool, error) {
	if importID == "" || !isEVMAddress(tx.Counterparty) {
		return false, nil
	}
	bankID, partnerID, err := resolveOdooCryptoPartnerBank(creds, uid, tx)
	if err != nil && partnerID == 0 {
		return false, err
	}
	if bankID == 0 && partnerID == 0 {
		return false, nil
	}
	rows, err := odooSearchReadAllMaps(creds, uid, "account.bank.statement.line",
		[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
			[]interface{}{"unique_import_id", "=", importID},
		},
		[]string{"id", "partner_id", "partner_bank_id"},
		"id desc",
	)
	if err != nil {
		return false, err
	}
	if len(rows) == 0 {
		return false, nil
	}
	lineID := odooInt(rows[0]["id"])
	if lineID == 0 {
		return false, nil
	}
	update := map[string]interface{}{}
	if partnerID > 0 && odooFieldID(rows[0]["partner_id"]) != partnerID {
		update["partner_id"] = partnerID
	}
	if bankID > 0 && odooFieldID(rows[0]["partner_bank_id"]) != bankID {
		update["partner_bank_id"] = bankID
	}
	if len(update) == 0 {
		return false, nil
	}
	_, err = odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement.line", "write",
		[]interface{}{[]interface{}{lineID}, update}, nil)
	if err != nil {
		return false, err
	}
	return true, nil
}

func createOdooPartner(creds *OdooCredentials, uid int, name string) (int, error) {
	result, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"res.partner", "create",
		[]interface{}{[]interface{}{map[string]interface{}{"name": name}}}, nil)
	if err != nil {
		return 0, err
	}
	ids := parseOdooCreatedIDs(result)
	if len(ids) == 0 {
		return 0, fmt.Errorf("Odoo did not return a partner id")
	}
	return ids[0], nil
}

func createOdooPartnerBank(creds *OdooCredentials, uid int, partnerID int, accountNumber string) (int, error) {
	result, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"res.partner.bank", "create",
		[]interface{}{[]interface{}{map[string]interface{}{
			"partner_id": partnerID,
			"acc_number": accountNumber,
		}}}, nil)
	if err != nil {
		return 0, err
	}
	ids := parseOdooCreatedIDs(result)
	if len(ids) == 0 {
		return 0, fmt.Errorf("Odoo did not return a partner bank id")
	}
	return ids[0], nil
}

func cryptoCounterpartyName(tx TransactionEntry, chain, address string) string {
	if isZeroEVMAddress(address) {
		symbol := tx.Currency
		if symbol == "" {
			symbol = "Token"
		}
		return fmt.Sprintf("%s/%s Minter", chain, symbol)
	}
	if ens := resolveENSNameForAddress(address); ens != "" {
		return ens
	}
	return truncateAddr(address)
}

func transactionChain(tx TransactionEntry) string {
	if tx.Chain != nil && *tx.Chain != "" {
		return strings.ToLower(*tx.Chain)
	}
	if tx.Provider != "" {
		return strings.ToLower(tx.Provider)
	}
	return "ethereum"
}

func cryptoBankAccountNumber(chain, address string) string {
	return fmt.Sprintf("%s:%s", strings.ToLower(chain), normalizeEVMAddress(address))
}

func transactionBankAccountNumber(tx TransactionEntry) string {
	keys := []string{
		"iban", "IBAN", "bankAccount", "bank_account",
		"counterpartyIban", "counterparty_iban", "counterpartIban", "counterpart_iban",
	}
	for _, key := range keys {
		if value, ok := tx.Metadata[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	if ibanLikePattern.MatchString(tx.Counterparty) {
		return strings.TrimSpace(tx.Counterparty)
	}
	return ""
}

func isEVMAddress(value string) bool {
	value = strings.TrimPrefix(strings.TrimPrefix(strings.TrimSpace(value), "0x"), "0X")
	if len(value) != 40 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func normalizeEVMAddress(value string) string {
	return "0x" + strings.ToLower(strings.TrimPrefix(strings.TrimPrefix(strings.TrimSpace(value), "0x"), "0X"))
}

func isZeroEVMAddress(value string) bool {
	return normalizeEVMAddress(value) == "0x0000000000000000000000000000000000000000"
}

func findOdooPartnerBankByAccountNumber(creds *OdooCredentials, uid int, accountNumber string) (int, int, error) {
	normalized := normalizeBankAccountNumber(accountNumber)
	if normalized == "" {
		return 0, 0, nil
	}
	rows, err := odooSearchReadAllMaps(creds, uid, "res.partner.bank",
		[]interface{}{[]interface{}{"sanitized_acc_number", "=", normalized}},
		[]string{"id", "partner_id", "acc_number", "sanitized_acc_number"},
		"id asc",
	)
	if err != nil {
		return 0, 0, err
	}
	if len(rows) == 0 {
		rows, err = odooSearchReadAllMaps(creds, uid, "res.partner.bank",
			[]interface{}{[]interface{}{"acc_number", "ilike", accountNumber}},
			[]string{"id", "partner_id", "acc_number", "sanitized_acc_number"},
			"id asc",
		)
		if err != nil {
			return 0, 0, err
		}
	}
	if len(rows) != 1 {
		return 0, 0, nil
	}
	return odooInt(rows[0]["id"]), odooFieldID(rows[0]["partner_id"]), nil
}

func normalizeBankAccountNumber(value string) string {
	value = strings.ToUpper(strings.TrimSpace(value))
	replacer := strings.NewReplacer(" ", "", "-", "", ".", "", ":", "")
	return replacer.Replace(value)
}

func resolveENSNameForAddress(address string) string {
	rpcURL := defaultRPCForChainID(1)
	if rpcURL == "" || !isEVMAddress(address) {
		return ""
	}
	node := ensNamehash(strings.TrimPrefix(normalizeEVMAddress(address), "0x") + ".addr.reverse")
	resolverData := "0x0178b8bf" + hex.EncodeToString(node[:])
	resolverResult, err := ethCallHex(rpcURL, "0x00000000000C2E074eC69A0dFb2997BA6C7d2e1e", resolverData)
	if err != nil {
		return ""
	}
	resolver := evmAddressFromCallResult(resolverResult)
	if resolver == "" || isZeroEVMAddress(resolver) {
		return ""
	}
	nameData := "0x691f3431" + hex.EncodeToString(node[:])
	nameResult, err := ethCallHex(rpcURL, resolver, nameData)
	if err != nil {
		return ""
	}
	return decodeABIString(nameResult)
}

func ensNamehash(name string) [32]byte {
	var node [32]byte
	labels := strings.Split(strings.ToLower(strings.TrimSuffix(name, ".")), ".")
	for i := len(labels) - 1; i >= 0; i-- {
		labelHash := keccak256([]byte(labels[i]))
		buf := make([]byte, 0, 64)
		buf = append(buf, node[:]...)
		buf = append(buf, labelHash...)
		copy(node[:], keccak256(buf))
	}
	return node
}

func ethCallHex(rpcURL, to, data string) (string, error) {
	payload := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "eth_call",
		"params": []interface{}{
			map[string]string{"to": to, "data": data},
			"latest",
		},
	}
	body, _ := json.Marshal(payload)
	resp, err := http.Post(rpcURL, "application/json", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var result struct {
		Result string `json:"result"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}
	if result.Error != nil {
		return "", fmt.Errorf("%s", result.Error.Message)
	}
	if result.Result == "" || result.Result == "0x" {
		return "", nil
	}
	return result.Result, nil
}

func evmAddressFromCallResult(result string) string {
	result = strings.TrimPrefix(result, "0x")
	if len(result) < 40 {
		return ""
	}
	return normalizeEVMAddress(result[len(result)-40:])
}

func decodeABIString(result string) string {
	data, err := hex.DecodeString(strings.TrimPrefix(result, "0x"))
	if err != nil || len(data) < 64 {
		return ""
	}
	offset := new(big.Int).SetBytes(data[:32]).Int64()
	if offset < 0 || int(offset)+32 > len(data) {
		return ""
	}
	length := new(big.Int).SetBytes(data[offset : offset+32]).Int64()
	start := int(offset) + 32
	end := start + int(length)
	if length < 0 || end > len(data) {
		return ""
	}
	return string(data[start:end])
}

func findOpenMoveCandidatesForStatementLine(creds *OdooCredentials, uid int, line odooStatementLineForReconcile, partnerID int) ([]odooMoveCandidate, error) {
	return findOpenMoveCandidates(creds, uid, line, partnerID)
}

func findOpenMoveCandidatesByAmountForStatementLine(creds *OdooCredentials, uid int, line odooStatementLineForReconcile) ([]odooMoveCandidate, error) {
	return findOpenMoveCandidates(creds, uid, line, 0)
}

func findOpenMoveCandidates(creds *OdooCredentials, uid int, line odooStatementLineForReconcile, partnerID int) ([]odooMoveCandidate, error) {
	lineDate, err := time.Parse("2006-01-02", line.Date)
	if err != nil {
		lineDate = time.Now()
	}
	startDate := lineDate.AddDate(0, -3, 0).Format("2006-01-02")
	endDate := lineDate.Format("2006-01-02")
	absAmount := math.Abs(line.Amount)
	if absAmount < 0.005 {
		return nil, nil
	}

	moveTypes := []interface{}{"out_invoice"}
	if line.Amount < 0 {
		moveTypes = []interface{}{"in_invoice"}
	}
	minAmount := roundCents(absAmount - 0.01)
	maxAmount := roundCents(absAmount + 0.01)
	domain := []interface{}{
		[]interface{}{"state", "=", "posted"},
		[]interface{}{"move_type", "in", moveTypes},
		[]interface{}{"payment_state", "not in", []interface{}{"paid", "in_payment", "reversed"}},
		[]interface{}{"amount_residual", ">=", minAmount},
		[]interface{}{"amount_residual", "<=", maxAmount},
		[]interface{}{"invoice_date", ">=", startDate},
		[]interface{}{"invoice_date", "<=", endDate},
	}
	if partnerID > 0 {
		domain = append(domain, []interface{}{"partner_id", "=", partnerID})
	}
	rows, err := odooSearchReadAllMaps(creds, uid, "account.move",
		domain,
		[]string{"id", "name", "invoice_date", "date", "move_type", "partner_id", "amount_residual"},
		"invoice_date desc, id desc",
	)
	if err != nil {
		return nil, err
	}
	candidates := make([]odooMoveCandidate, 0, len(rows))
	for _, row := range rows {
		candidates = append(candidates, odooMoveCandidate{
			ID:             odooInt(row["id"]),
			Name:           odooString(row["name"]),
			InvoiceDate:    odooString(row["invoice_date"]),
			Date:           odooString(row["date"]),
			MoveType:       odooString(row["move_type"]),
			PartnerName:    odooFieldName(row["partner_id"]),
			AmountResidual: odooFloat(row["amount_residual"]),
		})
	}
	return candidates, nil
}

func reconcileStatementLineWithMove(creds *OdooCredentials, uid int, line odooStatementLineForReconcile, move odooMoveCandidate) error {
	if line.MoveID == 0 {
		return fmt.Errorf("statement line has no move")
	}
	absAmount := math.Abs(line.Amount)
	bankLines, err := openReconcilableMoveLines(creds, uid, line.MoveID, absAmount)
	if err != nil {
		return fmt.Errorf("bank move lines: %v", err)
	}
	invoiceLines, err := openReconcilableMoveLines(creds, uid, move.ID, absAmount)
	if err != nil {
		return fmt.Errorf("invoice/bill move lines: %v", err)
	}
	if len(bankLines) != 1 || len(invoiceLines) != 1 {
		return fmt.Errorf("expected one reconcilable line on each move, found bank=%d invoice=%d", len(bankLines), len(invoiceLines))
	}
	_, err = odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.move.line", "reconcile",
		[]interface{}{[]interface{}{bankLines[0], invoiceLines[0]}}, nil)
	return err
}

func openReconcilableMoveLines(creds *OdooCredentials, uid int, moveID int, absAmount float64) ([]int, error) {
	rows, err := odooSearchReadAllMaps(creds, uid, "account.move.line",
		[]interface{}{
			[]interface{}{"move_id", "=", moveID},
			[]interface{}{"reconciled", "=", false},
			[]interface{}{"account_id.reconcile", "=", true},
		},
		[]string{"id", "amount_residual", "amount_residual_currency", "balance", "debit", "credit"},
		"id asc",
	)
	if err != nil {
		return nil, err
	}
	var all []int
	var exact []int
	for _, row := range rows {
		id := odooInt(row["id"])
		if id == 0 {
			continue
		}
		all = append(all, id)
		residual := math.Abs(odooFloat(row["amount_residual"]))
		if residual < 0.005 {
			residual = math.Abs(odooFloat(row["amount_residual_currency"]))
		}
		if residual < 0.005 {
			residual = math.Abs(odooFloat(row["balance"]))
		}
		if math.Abs(residual-absAmount) <= 0.01 {
			exact = append(exact, id)
		}
	}
	if len(exact) > 0 {
		return exact, nil
	}
	return all, nil
}

func markStatementLineInternalTransfer(creds *OdooCredentials, uid int, line odooStatementLineForReconcile, dryRun bool) error {
	accountID, err := findInternalTransferAccountID(creds, uid)
	if err != nil {
		return err
	}
	if accountID == 0 {
		return fmt.Errorf("Odoo account %s not found", odooInternalTransferAccountCode)
	}
	if line.MoveID == 0 {
		return fmt.Errorf("statement line has no move")
	}
	if dryRun {
		return nil
	}
	counterpartID, err := findStatementCounterpartMoveLine(creds, uid, line)
	if err != nil {
		return err
	}
	if counterpartID == 0 {
		return fmt.Errorf("could not identify counterpart move line")
	}
	_, _ = odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.move", "button_draft",
		[]interface{}{[]interface{}{line.MoveID}}, nil)
	_, err = odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.move.line", "write",
		[]interface{}{[]interface{}{counterpartID}, map[string]interface{}{"account_id": accountID}}, nil)
	if err != nil {
		return err
	}
	_, err = odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.move", "action_post",
		[]interface{}{[]interface{}{line.MoveID}}, nil)
	return err
}

func findInternalTransferAccountID(creds *OdooCredentials, uid int) (int, error) {
	rows, err := odooSearchReadAllMaps(creds, uid, "account.account",
		[]interface{}{[]interface{}{"code", "=", odooInternalTransferAccountCode}},
		[]string{"id", "code", "name"},
		"id asc",
	)
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}
	return odooInt(rows[0]["id"]), nil
}

func findStatementCounterpartMoveLine(creds *OdooCredentials, uid int, line odooStatementLineForReconcile) (int, error) {
	rows, err := odooSearchReadAllMaps(creds, uid, "account.move.line",
		[]interface{}{[]interface{}{"move_id", "=", line.MoveID}},
		[]string{"id", "balance", "debit", "credit"},
		"id asc",
	)
	if err != nil {
		return 0, err
	}
	var candidates []int
	for _, row := range rows {
		id := odooInt(row["id"])
		balance := odooFloat(row["balance"])
		debit := odooFloat(row["debit"])
		credit := odooFloat(row["credit"])
		if line.Amount > 0 && (balance < -0.005 || credit > 0.005) {
			candidates = append(candidates, id)
		}
		if line.Amount < 0 && (balance > 0.005 || debit > 0.005) {
			candidates = append(candidates, id)
		}
	}
	if len(candidates) == 1 {
		return candidates[0], nil
	}
	return 0, nil
}

func looksLikeInternalTransferLine(line odooStatementLineForReconcile) bool {
	text := strings.ToLower(line.PaymentRef + " " + line.UniqueImportID)
	return strings.Contains(text, "stripe payout") ||
		strings.Contains(text, "auto payout") ||
		strings.Contains(text, "manual payout") ||
		strings.Contains(text, "internal transfer")
}

func formatOdooReconcileDetail(line odooStatementLineForReconcile, result odooLineReconcileResult) string {
	ref := line.PaymentRef
	if ref == "" {
		ref = line.UniqueImportID
	}
	if ref == "" {
		ref = fmt.Sprintf("line #%d", line.ID)
	}
	msg := result.Message
	if result.Err != nil {
		msg = fmt.Sprintf("%s: %v", msg, result.Err)
	}
	if result.CandidateMoveName != "" {
		msg = fmt.Sprintf("%s %s", msg, result.CandidateMoveName)
	}
	return fmt.Sprintf("%s %s %.2f: %s", line.Date, ref, line.Amount, msg)
}

func odooStatementLineCounterparty(line odooStatementLineForReconcile) string {
	if line.PartnerName != "" {
		return line.PartnerName
	}
	if line.PaymentRef != "" {
		return line.PaymentRef
	}
	if line.UniqueImportID != "" {
		return line.UniqueImportID
	}
	return fmt.Sprintf("line #%d", line.ID)
}

func formatOdooPotentialMatch(candidates []odooMoveCandidate, err error) string {
	if err != nil {
		return "lookup error"
	}
	if len(candidates) == 0 {
		return "-"
	}
	if len(candidates) == 1 {
		return candidateDisplayNameWithPartner(candidates[0])
	}
	return fmt.Sprintf("%d matches, e.g. %s", len(candidates), candidateDisplayNameWithPartner(candidates[0]))
}

func candidateDisplayNameWithPartner(candidate odooMoveCandidate) string {
	name := candidateDisplayName(candidate)
	if candidate.PartnerName != "" {
		name = fmt.Sprintf("%s (%s)", name, candidate.PartnerName)
	}
	return name
}

func odooReconcileReason(result odooLineReconcileResult, amountErr error) string {
	switch {
	case result.Err != nil:
		if result.Message != "" {
			return fmt.Sprintf("%s failed: %v", result.Message, result.Err)
		}
		return result.Err.Error()
	case result.InternalTransfer:
		return result.Message
	case result.Reconciled:
		if result.CandidateMoveName != "" {
			return fmt.Sprintf("%s %s", result.Message, result.CandidateMoveName)
		}
		return result.Message
	case result.Ambiguous:
		return result.Message
	case result.NoPartner:
		return result.Message
	case result.NoMatch:
		return result.Message
	}
	if amountErr != nil {
		return fmt.Sprintf("amount-only lookup failed: %v", amountErr)
	}
	return "already reconciled"
}

func printOdooReconcileRows(stats *odooReconcileStats) {
	if len(stats.Rows) == 0 {
		return
	}
	rows := make([][]string, 0, len(stats.Rows))
	for _, row := range stats.Rows {
		rows = append(rows, []string{
			row.Date,
			row.Counterparty,
			fmtEURSigned(row.Amount),
			row.PotentialMatch,
			row.Reason,
		})
	}
	printAlignedTable(
		[]string{"Date", "Counterparty", "Amount", "Potential match", "Reason"},
		rows,
		map[int]bool{2: true},
	)
}

func printOdooReconcileStats(stats *odooReconcileStats) {
	label := "reconciled"
	if stats.DryRun {
		label = "would reconcile"
	}
	printOdooReconcileRows(stats)
	fmt.Printf("\n  %s── Reconciliation summary ──%s\n", Fmt.Bold, Fmt.Reset)
	fmt.Printf("    Lines scanned:       %d\n", stats.Scanned)
	fmt.Printf("    Lines %s:    %d\n", label, stats.Reconciled)
	fmt.Printf("    Internal transfers:  %d\n", stats.InternalTransfers)
	fmt.Printf("    Ambiguous matches:   %d\n", stats.Ambiguous)
	fmt.Printf("    No partner:          %d\n", stats.NoPartner)
	fmt.Printf("    No match:            %d\n", stats.NoMatch)
	fmt.Printf("    Errors:              %d\n", stats.Errors)
	fmt.Println()
}

func candidateDisplayName(candidate odooMoveCandidate) string {
	if candidate.Name != "" {
		return candidate.Name
	}
	return fmt.Sprintf("#%d", candidate.ID)
}

func parseOdooCreatedIDs(raw json.RawMessage) []int {
	var ids []int
	if err := json.Unmarshal(raw, &ids); err == nil {
		return ids
	}
	var id int
	if err := json.Unmarshal(raw, &id); err == nil && id > 0 {
		return []int{id}
	}
	var f float64
	if err := json.Unmarshal(raw, &f); err == nil && f > 0 {
		return []int{int(f)}
	}
	return nil
}
