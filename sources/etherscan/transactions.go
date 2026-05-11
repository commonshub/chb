package etherscan

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func FetchTokenTransfers(acc Account, apiKey string) ([]TokenTransfer, error) {
	baseURL := fmt.Sprintf("https://api.etherscan.io/v2/api?chainid=%d", acc.ChainID)

	var url string
	if acc.Address == "" || strings.EqualFold(acc.Address, acc.TokenAddress) {
		url = fmt.Sprintf("%s&module=account&action=tokentx&contractaddress=%s&startblock=0&endblock=99999999&sort=desc&apikey=%s",
			baseURL, acc.TokenAddress, apiKey)
	} else {
		url = fmt.Sprintf("%s&module=account&action=tokentx&contractaddress=%s&address=%s&startblock=0&endblock=99999999&sort=desc&apikey=%s",
			baseURL, acc.TokenAddress, acc.Address, apiKey)
	}

	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * time.Second)
		}

		resp, err := http.Get(url)
		if err != nil {
			lastErr = err
			continue
		}

		var result struct {
			Status  string          `json:"status"`
			Message string          `json:"message"`
			Result  json.RawMessage `json:"result"`
		}
		err = json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}

		if result.Status == "0" && result.Message != "No transactions found" {
			if strings.Contains(strings.ToLower(result.Message), "rate limit") {
				lastErr = fmt.Errorf("rate limited: %s", result.Message)
				time.Sleep(2 * time.Second)
				continue
			}
			return nil, fmt.Errorf("API error: %s", result.Message)
		}

		var transfers []TokenTransfer
		if err := json.Unmarshal(result.Result, &transfers); err != nil {
			return []TokenTransfer{}, nil
		}

		return transfers, nil
	}

	return nil, fmt.Errorf("failed after 3 attempts: %v", lastErr)
}

func PeekLatest(acc Account, apiKey string) (string, error) {
	baseURL := fmt.Sprintf("https://api.etherscan.io/v2/api?chainid=%d", acc.ChainID)
	var url string
	if acc.Address == "" || strings.EqualFold(acc.Address, acc.TokenAddress) {
		url = fmt.Sprintf("%s&module=account&action=tokentx&contractaddress=%s&startblock=0&endblock=99999999&page=1&offset=1&sort=desc&apikey=%s",
			baseURL, acc.TokenAddress, apiKey)
	} else {
		url = fmt.Sprintf("%s&module=account&action=tokentx&contractaddress=%s&address=%s&startblock=0&endblock=99999999&page=1&offset=1&sort=desc&apikey=%s",
			baseURL, acc.TokenAddress, acc.Address, apiKey)
	}
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var result struct {
		Status  string          `json:"status"`
		Message string          `json:"message"`
		Result  json.RawMessage `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}
	var transfers []TokenTransfer
	if err := json.Unmarshal(result.Result, &transfers); err != nil || len(transfers) == 0 {
		return "", nil
	}
	return transfers[0].Hash, nil
}

func GroupByMonth(transfers []TokenTransfer, tz *time.Location) map[string][]TokenTransfer {
	if tz == nil {
		tz = time.UTC
	}
	byMonth := make(map[string][]TokenTransfer)

	for _, tx := range transfers {
		ts, err := strconv.ParseInt(tx.TimeStamp, 10, 64)
		if err != nil {
			continue
		}
		t := time.Unix(ts, 0).In(tz)
		ym := fmt.Sprintf("%d-%02d", t.Year(), t.Month())
		byMonth[ym] = append(byMonth[ym], tx)
	}

	return byMonth
}

func ParseTokenValue(rawValue string, decimals int) float64 {
	val := new(big.Float)
	val.SetString(rawValue)
	divisor := new(big.Float).SetFloat64(math.Pow10(decimals))
	result := new(big.Float).Quo(val, divisor)
	f, _ := result.Float64()
	return f
}
