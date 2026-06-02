package etherscan

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// redactAPIKey hides the apikey query value so URLs are safe to log.
func redactAPIKey(url, apiKey string) string {
	if apiKey == "" {
		return url
	}
	return strings.ReplaceAll(url, apiKey, "***")
}

// resultString renders Etherscan's `result` field for an error message.
// On failures `result` is usually a human-readable string ("Invalid API Key",
// "Max rate limit reached", …); on success it's a JSON array. Either way this
// returns something printable.
func resultString(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	return strings.TrimSpace(string(raw))
}

// bodySnippet trims a response body for inclusion in an error/log line.
func bodySnippet(body []byte) string {
	s := strings.TrimSpace(string(body))
	const max = 600
	if len(s) > max {
		return s[:max] + "…(truncated)"
	}
	return s
}

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

		ctx := fmt.Sprintf("chain=%d contract=%s address=%s url=%s",
			acc.ChainID, acc.TokenAddress, acc.Address, redactAPIKey(url, apiKey))

		resp, err := http.Get(url)
		if err != nil {
			lastErr = fmt.Errorf("etherscan request failed (%s): %w", ctx, err)
			continue
		}

		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			lastErr = fmt.Errorf("etherscan: reading response body (HTTP %d, %s): %w", resp.StatusCode, ctx, readErr)
			continue
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			lastErr = fmt.Errorf("etherscan HTTP %d (%s): %s", resp.StatusCode, ctx, bodySnippet(body))
			// 429/5xx are worth a retry; other 4xx won't change on retry.
			if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
				time.Sleep(2 * time.Second)
				continue
			}
			return nil, lastErr
		}

		var result struct {
			Status  string          `json:"status"`
			Message string          `json:"message"`
			Result  json.RawMessage `json:"result"`
		}
		if err := json.Unmarshal(body, &result); err != nil {
			lastErr = fmt.Errorf("etherscan: decoding response (HTTP %d, %s): %v — body: %s",
				resp.StatusCode, ctx, err, bodySnippet(body))
			continue
		}

		if result.Status == "0" && result.Message != "No transactions found" {
			detail := resultString(result.Result)
			apiErr := fmt.Errorf("etherscan API error: status=%s message=%q result=%q (%s)",
				result.Status, result.Message, detail, ctx)
			// The actionable reason often lives in `result` ("Max rate limit
			// reached"), not just `message` ("NOTOK") — check both.
			if strings.Contains(strings.ToLower(result.Message+" "+detail), "rate limit") {
				lastErr = fmt.Errorf("rate limited: %w", apiErr)
				time.Sleep(2 * time.Second)
				continue
			}
			return nil, apiErr
		}

		var transfers []TokenTransfer
		if err := json.Unmarshal(result.Result, &transfers); err != nil {
			// Status was OK but `result` isn't a transfer array (e.g. the
			// "No transactions found" sentinel). Treat as empty, but surface
			// anything unexpected in the logs rather than silently dropping it.
			if result.Message != "No transactions found" {
				return []TokenTransfer{}, fmt.Errorf("etherscan: unexpected result payload (status=%s message=%q, %s): %s",
					result.Status, result.Message, ctx, bodySnippet(result.Result))
			}
			return []TokenTransfer{}, nil
		}

		return transfers, nil
	}

	return nil, fmt.Errorf("etherscan: failed after 3 attempts: %w", lastErr)
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
	ctx := fmt.Sprintf("chain=%d contract=%s address=%s url=%s",
		acc.ChainID, acc.TokenAddress, acc.Address, redactAPIKey(url, apiKey))

	resp, err := http.Get(url)
	if err != nil {
		return "", fmt.Errorf("etherscan peek request failed (%s): %w", ctx, err)
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return "", fmt.Errorf("etherscan peek: reading response body (HTTP %d, %s): %w", resp.StatusCode, ctx, readErr)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("etherscan peek HTTP %d (%s): %s", resp.StatusCode, ctx, bodySnippet(body))
	}
	var result struct {
		Status  string          `json:"status"`
		Message string          `json:"message"`
		Result  json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", fmt.Errorf("etherscan peek: decoding response (HTTP %d, %s): %v — body: %s",
			resp.StatusCode, ctx, err, bodySnippet(body))
	}
	if result.Status == "0" && result.Message != "No transactions found" {
		return "", fmt.Errorf("etherscan peek API error: status=%s message=%q result=%q (%s)",
			result.Status, result.Message, resultString(result.Result), ctx)
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
