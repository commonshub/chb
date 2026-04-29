package stripe

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/CommonsHub/chb/sources"
)

func RelPath(elems ...string) string {
	parts := append([]string{"sources", Source}, elems...)
	return filepath.Join(parts...)
}

func Path(dataDir, year, month string, elems ...string) string {
	parts := []string{dataDir, year, month, RelPath(elems...)}
	return filepath.Join(parts...)
}

func TransactionCachePath(dataDir, year, month string) string {
	return Path(dataDir, year, month, BalanceTransactionsFile)
}

func TransactionCachePaths(dataDir, year, month string) []string {
	path := TransactionCachePath(dataDir, year, month)
	if fileExists(path) {
		return []string{path}
	}
	return nil
}

func LoadTransactionsSince(dataDir, accountID string, sinceUnix int64) ([]Transaction, error) {
	all, err := LoadTransactions(dataDir, accountID)
	if err != nil {
		return nil, err
	}
	var out []Transaction
	for _, tx := range all {
		if tx.Created > sinceUnix {
			out = append(out, tx)
		}
	}
	return out, nil
}

func LoadTransactions(dataDir, accountID string) ([]Transaction, error) {
	years, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, err
	}

	seen := map[string]bool{}
	foundCache := false
	var out []Transaction
	for _, y := range years {
		if !y.IsDir() || len(y.Name()) != 4 {
			continue
		}
		months, err := os.ReadDir(filepath.Join(dataDir, y.Name()))
		if err != nil {
			continue
		}
		for _, m := range months {
			if !m.IsDir() || len(m.Name()) != 2 {
				continue
			}
			cache, ok := LoadCache(TransactionCachePath(dataDir, y.Name(), m.Name()))
			if !ok {
				continue
			}
			if accountID != "" && cache.AccountID != "" && !strings.EqualFold(accountID, cache.AccountID) {
				continue
			}
			foundCache = true
			for _, tx := range cache.Transactions {
				if tx.ID == "" || seen[tx.ID] {
					continue
				}
				seen[tx.ID] = true
				out = append(out, tx)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Created == out[j].Created {
			return out[i].ID < out[j].ID
		}
		return out[i].Created < out[j].Created
	})
	if !foundCache {
		return nil, os.ErrNotExist
	}
	return out, nil
}

func LoadCache(path string) (CacheFile, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return CacheFile{}, false
	}
	var cache CacheFile
	if json.Unmarshal(data, &cache) != nil {
		return CacheFile{}, false
	}
	return cache, len(cache.Transactions) > 0
}

type FetchOptions struct {
	APIKey              string
	AccountID           string
	StartMonth          string
	EndMonth            string
	Limit               int
	CreatedAfter        *time.Time
	StopAtMonthBoundary bool
	DataDir             string
	Location            *time.Location
	Progress            sources.ProgressFunc
}

func FetchTransactions(opts FetchOptions) ([]Transaction, error) {
	tz := opts.Location
	if tz == nil {
		tz = time.Local
	}
	var allTxs []Transaction

	rangeStart, rangeEnd, err := monthRange(opts.StartMonth, opts.EndMonth, tz)
	if err != nil {
		return nil, err
	}
	createdGte := rangeStart.Unix()
	if opts.CreatedAfter != nil && !opts.CreatedAfter.IsZero() {
		if after := opts.CreatedAfter.Unix(); after > createdGte {
			createdGte = after
		}
	}
	createdLt := rangeEnd.Unix()

	pageSize := 100
	if opts.Limit > 0 && opts.Limit < pageSize {
		pageSize = opts.Limit
	}

	var startingAfter string
	page := 0
	for {
		page++
		url := fmt.Sprintf("https://api.stripe.com/v1/balance_transactions?limit=%d&created[gte]=%d&created[lt]=%d",
			pageSize, createdGte, createdLt)
		if startingAfter != "" {
			url += "&starting_after=" + startingAfter
		}

		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+opts.APIKey)
		if opts.AccountID != "" {
			req.Header.Set("Stripe-Account", opts.AccountID)
		}

		resp, err := httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("stripe API error: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode == 429 {
			if opts.Progress != nil {
				opts.Progress(sources.ProgressEvent{Source: Source, Step: "fetch_transactions", Detail: "rate_limited", Current: page})
			}
			time.Sleep(2 * time.Second)
			continue
		}
		if resp.StatusCode != 200 {
			return nil, fmt.Errorf("stripe API returned %d", resp.StatusCode)
		}

		var listResp ListResponse
		if err := json.NewDecoder(resp.Body).Decode(&listResp); err != nil {
			return nil, fmt.Errorf("failed to decode stripe response: %w", err)
		}

		allTxs = append(allTxs, listResp.Data...)
		if opts.Progress != nil {
			opts.Progress(sources.ProgressEvent{
				Source:  Source,
				Step:    "fetch_transactions",
				Detail:  "page",
				Current: page,
				Total:   len(allTxs),
			})
		}

		if opts.StopAtMonthBoundary && opts.DataDir != "" && len(allTxs) > 0 {
			oldestMonth := time.Unix(allTxs[len(allTxs)-1].Created, 0).In(tz).Format("2006-01")
			countSeen := 0
			for _, tx := range allTxs {
				if time.Unix(tx.Created, 0).In(tz).Format("2006-01") == oldestMonth {
					countSeen++
				}
			}
			parts := strings.Split(oldestMonth, "-")
			localCount := 0
			if len(parts) == 2 {
				localCount = LocalTransactionCount(TransactionCachePath(opts.DataDir, parts[0], parts[1]))
			}
			if localCount > 0 && countSeen == localCount {
				if opts.Progress != nil {
					opts.Progress(sources.ProgressEvent{
						Source: Source,
						Step:   "fetch_transactions",
						Detail: "stop_at_cached_month",
						Month:  oldestMonth,
						Total:  localCount,
					})
				}
				break
			}
		}

		if opts.Limit > 0 && len(allTxs) >= opts.Limit {
			allTxs = allTxs[:opts.Limit]
			break
		}
		if !listResp.HasMore || len(listResp.Data) == 0 {
			break
		}
		startingAfter = listResp.Data[len(listResp.Data)-1].ID
		time.Sleep(200 * time.Millisecond)
	}

	return allTxs, nil
}

func LocalTransactionCount(filePath string) int {
	cache, ok := LoadCache(filePath)
	if !ok {
		return 0
	}
	return len(cache.Transactions)
}

func StripSourceToID(source json.RawMessage) json.RawMessage {
	if source == nil {
		return nil
	}
	var obj struct {
		ID string `json:"id"`
	}
	if json.Unmarshal(source, &obj) == nil && obj.ID != "" {
		quoted, _ := json.Marshal(obj.ID)
		return quoted
	}
	return source
}

func EnrichTransaction(tx *Transaction) {
	if tx.Source == nil {
		return
	}

	var source struct {
		Object         string `json:"object"`
		ID             string `json:"id"`
		Description    string `json:"description"`
		BillingDetails struct {
			Name  string `json:"name"`
			Email string `json:"email"`
		} `json:"billing_details"`
		Customer interface{}            `json:"customer"`
		Metadata map[string]interface{} `json:"metadata"`
	}
	if json.Unmarshal(tx.Source, &source) != nil {
		return
	}

	if source.Object == "charge" || source.Object == "payment_intent" {
		tx.ChargeID = source.ID
		if custObj, ok := source.Customer.(map[string]interface{}); ok {
			if name, ok := custObj["name"].(string); ok && name != "" {
				tx.CustomerName = name
			}
			if email, ok := custObj["email"].(string); ok && email != "" {
				tx.CustomerEmail = email
			}
		}
		if tx.CustomerName == "" {
			tx.CustomerName = source.BillingDetails.Name
		}
		if tx.CustomerEmail == "" {
			tx.CustomerEmail = source.BillingDetails.Email
		}
		if tx.Description == "" && source.Description != "" {
			tx.Description = source.Description
		}
		if len(source.Metadata) > 0 && tx.Metadata == nil {
			tx.Metadata = map[string]interface{}{}
		}
		for k, v := range source.Metadata {
			tx.Metadata[k] = v
		}
	}

	if source.Object == "payout" {
		var po struct {
			ID                  string `json:"id"`
			Automatic           bool   `json:"automatic"`
			StatementDescriptor string `json:"statement_descriptor"`
			Description         string `json:"description"`
			ArrivalDate         int64  `json:"arrival_date"`
			Destination         *struct {
				Last4 string `json:"last4"`
			} `json:"destination"`
		}
		if json.Unmarshal(tx.Source, &po) == nil {
			tx.PayoutID = po.ID
			tx.PayoutAutomatic = po.Automatic
			tx.PayoutStatementDescriptor = po.StatementDescriptor
			tx.PayoutArrivalDate = po.ArrivalDate
			if po.Destination != nil {
				tx.PayoutBankLast4 = po.Destination.Last4
			}
			if tx.Description == "" {
				if po.StatementDescriptor != "" {
					tx.Description = po.StatementDescriptor
				} else if po.Description != "" {
					tx.Description = po.Description
				}
			}
		}
	}
}

func GroupTransactionsByMonth(txs []Transaction, tz *time.Location) map[string][]Transaction {
	if tz == nil {
		tz = time.Local
	}
	byMonth := make(map[string][]Transaction)
	for _, tx := range txs {
		t := time.Unix(tx.Created, 0).In(tz)
		ym := fmt.Sprintf("%d-%02d", t.Year(), t.Month())
		byMonth[ym] = append(byMonth[ym], tx)
	}
	return byMonth
}

func LatestCachedTransactionID(filePath string) string {
	cache, ok := LoadCache(filePath)
	if !ok || len(cache.Transactions) == 0 {
		return ""
	}
	return cache.Transactions[0].ID
}

func PeekLatest(apiKey, accountID, startMonth, endMonth string, tz *time.Location) (string, error) {
	if tz == nil {
		tz = time.Local
	}
	rangeStart, rangeEnd, err := monthRange(startMonth, endMonth, tz)
	if err != nil {
		return "", err
	}
	url := fmt.Sprintf("https://api.stripe.com/v1/balance_transactions?limit=1&created[gte]=%d&created[lt]=%d",
		rangeStart.Unix(), rangeEnd.Unix())
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	if accountID != "" {
		req.Header.Set("Stripe-Account", accountID)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("stripe API returned %d", resp.StatusCode)
	}
	var listResp ListResponse
	if err := json.NewDecoder(resp.Body).Decode(&listResp); err != nil {
		return "", err
	}
	if len(listResp.Data) == 0 {
		return "", nil
	}
	return listResp.Data[0].ID, nil
}

func monthRange(startMonth, endMonth string, tz *time.Location) (time.Time, time.Time, error) {
	startParts := strings.Split(startMonth, "-")
	if len(startParts) != 2 {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid start month: %s", startMonth)
	}
	startYear, _ := strconv.Atoi(startParts[0])
	startMon, _ := strconv.Atoi(startParts[1])
	rangeStart := time.Date(startYear, time.Month(startMon), 1, 0, 0, 0, 0, tz)

	endParts := strings.Split(endMonth, "-")
	if len(endParts) != 2 {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid end month: %s", endMonth)
	}
	endYear, _ := strconv.Atoi(endParts[0])
	endMon, _ := strconv.Atoi(endParts[1])
	rangeEnd := time.Date(endYear, time.Month(endMon)+1, 1, 0, 0, 0, 0, tz)
	return rangeStart, rangeEnd, nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
