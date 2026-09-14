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

type apiEnvelope struct {
	Status  string          `json:"status"`
	Message string          `json:"message"`
	Result  json.RawMessage `json:"result"`
}

// fetchFrom performs one tokentx query against ep, retrying transient
// failures. refused reports that the explorer declined on plan/quota grounds,
// in which case the caller may move on to the next explorer.
func fetchFrom(ep explorerEndpoint, acc Account, startBlock int64, extra string) (transfers []TokenTransfer, refused bool, err error) {
	url := ep.tokenTxURL(acc, startBlock, extra)
	ctx := fmt.Sprintf("%s chain=%d contract=%s address=%s url=%s",
		ep.Name, acc.ChainID, acc.TokenAddress, acc.Address, redactAPIKey(url, ep.APIKey))

	var lastErr error
	for attempt := 0; attempt < explorerMaxAttempts; attempt++ {
		if attempt > 0 {
			sleepFn(time.Duration(attempt) * time.Second)
		}
		ep.pace()

		resp, err := http.Get(url)
		if err != nil {
			lastErr = fmt.Errorf("%s request failed (%s): %w", ep.Name, ctx, err)
			continue
		}

		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			lastErr = fmt.Errorf("%s: reading response body (HTTP %d, %s): %w", ep.Name, resp.StatusCode, ctx, readErr)
			continue
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			lastErr = fmt.Errorf("%s HTTP %d (%s): %s", ep.Name, resp.StatusCode, ctx, bodySnippet(body))
			// 429/5xx are worth a retry; other 4xx won't change on retry.
			if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
				// Honour Retry-After when the explorer says how long; otherwise
				// double the wait each time (2s, 4s, 8s, …). Blockscout's
				// public instances throttle bursts, and asking again 2s later
				// three times in a row is exactly what got us throttled.
				sleepFn(retryDelay(resp.Header.Get("Retry-After"), attempt))
				continue
			}
			return nil, false, lastErr
		}

		var env apiEnvelope
		if err := json.Unmarshal(body, &env); err != nil {
			lastErr = fmt.Errorf("%s: decoding response (HTTP %d, %s): %v — body: %s",
				ep.Name, resp.StatusCode, ctx, err, bodySnippet(body))
			continue
		}

		if env.Status == "0" && !isEmptyResultMessage(env.Message) {
			detail := resultString(env.Result)
			apiErr := fmt.Errorf("%s API error: status=%s message=%q result=%q (%s)",
				ep.Name, env.Status, env.Message, detail, ctx)
			if isPlanRefusal(env.Message, detail) {
				return nil, true, apiErr
			}
			// The actionable reason often lives in `result` ("Max rate limit
			// reached"), not just `message` ("NOTOK") — check both.
			if strings.Contains(strings.ToLower(env.Message+" "+detail), "rate limit") {
				lastErr = fmt.Errorf("rate limited: %w", apiErr)
				sleepFn(retryDelay("", attempt))
				continue
			}
			return nil, false, apiErr
		}

		if err := json.Unmarshal(env.Result, &transfers); err != nil {
			// Status was OK but `result` isn't a transfer array (e.g. the
			// "No transactions found" sentinel). Treat as empty, but surface
			// anything unexpected rather than silently dropping it.
			if !isEmptyResultMessage(env.Message) {
				return []TokenTransfer{}, false, fmt.Errorf("%s: unexpected result payload (status=%s message=%q, %s): %s",
					ep.Name, env.Status, env.Message, ctx, bodySnippet(env.Result))
			}
			return []TokenTransfer{}, false, nil
		}
		return transfers, false, nil
	}

	return nil, false, fmt.Errorf("%s: failed after %d attempts: %w", ep.Name, explorerMaxAttempts, lastErr)
}

// queryExplorers runs one tokentx query against each explorer for the chain
// in turn, moving on only when an explorer refuses on plan/quota grounds.
func queryExplorers(acc Account, apiKey string, startBlock int64, extra string) ([]TokenTransfer, error) {
	eps := endpointsFor(acc.ChainID, apiKey)
	var lastErr error
	for i, ep := range eps {
		transfers, refused, err := fetchFrom(ep, acc, startBlock, extra)
		if err == nil {
			return transfers, nil
		}
		lastErr = err
		if !refused || i+1 >= len(eps) {
			return nil, err
		}
		if OnFallback != nil {
			OnFallback(acc.ChainID, ep.Name, eps[i+1].Name, resultReason(err))
		}
	}
	return nil, lastErr
}

// resultReason trims an API error down to the explorer's own words.
func resultReason(err error) string {
	s := err.Error()
	if i := strings.Index(s, "result="); i >= 0 {
		s = s[i+len("result="):]
		if j := strings.Index(s, " (etherscan chain="); j >= 0 {
			s = s[:j]
		} else if j := strings.Index(s, " (blockscout chain="); j >= 0 {
			s = s[:j]
		}
	}
	return strings.Trim(s, "\"")
}

// FetchTokenTransfers returns the full transfer history of the scope.
func FetchTokenTransfers(acc Account, apiKey string) ([]TokenTransfer, error) {
	return FetchTokenTransfersSince(acc, apiKey, 0)
}

// FetchTokenTransfersSince returns transfers from startBlock (inclusive)
// onwards, newest first. With startBlock = the newest cached block this is
// the hourly delta: one small request, usually empty.
func FetchTokenTransfersSince(acc Account, apiKey string, startBlock int64) ([]TokenTransfer, error) {
	if startBlock < 0 {
		startBlock = 0
	}
	return queryExplorers(acc, apiKey, startBlock, "")
}

// PeekLatest returns the hash of the newest transfer in the scope ("" if none).
func PeekLatest(acc Account, apiKey string) (string, error) {
	transfers, err := queryExplorers(acc, apiKey, 0, "page=1&offset=1")
	if err != nil {
		return "", fmt.Errorf("peek: %w", err)
	}
	if len(transfers) == 0 {
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
