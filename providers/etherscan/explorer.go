package etherscan

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Explorer endpoints.
//
// Etherscan's V2 API fronts most chains behind one key, but its free plan no
// longer covers Gnosis (chain 100) at all ("Free API access is not supported
// for this chain") and rations Celo ("Community Free API Limit reached").
// Blockscout instances expose the same Etherscan-compatible query and
// response shape without a key, so chains listed in blockscoutBases have a
// second source: Gnosis goes there directly, other chains fall back to it
// when Etherscan refuses on plan or quota grounds.
var (
	etherscanV2Base = "https://api.etherscan.io/v2/api"
	blockscoutBases = map[int]string{
		100:   "https://gnosis.blockscout.com/api",
		42220: "https://celo.blockscout.com/api",
	}
	// Chains Etherscan's free plan refuses outright: skip it entirely.
	etherscanUnsupported = map[int]bool{100: true}

	// OnFallback, when set, is called once per query that had to move from
	// one explorer to the next. The CLI uses it to print a note.
	OnFallback func(chainID int, from, to, reason string)
)

type explorerEndpoint struct {
	Name    string // "etherscan" | "blockscout"
	BaseURL string
	APIKey  string // empty for blockscout
}

// endpointsFor returns the explorers to try for a chain, in order.
func endpointsFor(chainID int, apiKey string) []explorerEndpoint {
	var out []explorerEndpoint
	if !etherscanUnsupported[chainID] {
		out = append(out, explorerEndpoint{
			Name:    "etherscan",
			BaseURL: fmt.Sprintf("%s?chainid=%d", etherscanV2Base, chainID),
			APIKey:  apiKey,
		})
	}
	if b := blockscoutBases[chainID]; b != "" {
		out = append(out, explorerEndpoint{Name: "blockscout", BaseURL: b})
	}
	if len(out) == 0 {
		out = append(out, explorerEndpoint{
			Name:    "etherscan",
			BaseURL: fmt.Sprintf("%s?chainid=%d", etherscanV2Base, chainID),
			APIKey:  apiKey,
		})
	}
	return out
}

// tokenTxURL builds the account/tokentx query. startBlock is inclusive, so
// callers passing the newest cached block get that block's transfers again;
// dedupe by Key() absorbs the overlap.
func (ep explorerEndpoint) tokenTxURL(acc Account, startBlock int64, extra string) string {
	q := "module=account&action=tokentx&contractaddress=" + acc.TokenAddress
	if acc.Address != "" && !strings.EqualFold(acc.Address, acc.TokenAddress) {
		q += "&address=" + acc.Address
	}
	q += fmt.Sprintf("&startblock=%d&endblock=99999999&sort=desc", startBlock)
	if extra != "" {
		q += "&" + extra
	}
	if ep.APIKey != "" {
		q += "&apikey=" + ep.APIKey
	}
	sep := "?"
	if strings.Contains(ep.BaseURL, "?") {
		sep = "&"
	}
	return ep.BaseURL + sep + q
}

// isEmptyResultMessage recognises the "nothing here" sentinel, which differs
// per explorer: Etherscan says "No transactions found", Blockscout "No token
// transfers found".
func isEmptyResultMessage(msg string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(msg)), "no ")
}

// isPlanRefusal reports whether an explorer declined because of its pricing
// plan or daily quota (as opposed to a transient per-second rate limit).
// These don't improve on retry; the caller should try the next explorer.
func isPlanRefusal(msg, detail string) bool {
	s := strings.ToLower(msg + " " + detail)
	for _, needle := range []string{
		"not supported for this chain",
		"api limit reached",
		"upgrade your api plan",
		"upgrade to api pro",
	} {
		if strings.Contains(s, needle) {
			return true
		}
	}
	return false
}

// Key is the dedup identity of a transfer across syncs: a token transfer is
// the same event when hash, parties, value, time and token all match. (A tx
// can carry several transfers, so the hash alone is not enough.)
func (t TokenTransfer) Key() string {
	return strings.ToLower(t.Hash) + "|" +
		strings.ToLower(t.From) + "|" +
		strings.ToLower(t.To) + "|" +
		t.Value + "|" +
		t.TimeStamp + "|" +
		t.TokenDecimal + "|" +
		strings.ToLower(t.TokenSymbol)
}

func parseInt64(s string) int64 {
	n, _ := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	return n
}

// MergeTokenTransfers unions a cached month with freshly fetched transfers.
// Fetched copies win (confirmations etc. refresh), order is newest-first by
// block then timestamp — the same order the explorers return.
func MergeTokenTransfers(existing, fetched []TokenTransfer) []TokenTransfer {
	byKey := make(map[string]TokenTransfer, len(existing)+len(fetched))
	for _, t := range existing {
		byKey[t.Key()] = t
	}
	for _, t := range fetched {
		byKey[t.Key()] = t
	}
	out := make([]TokenTransfer, 0, len(byKey))
	for _, t := range byKey {
		out = append(out, t)
	}
	sort.SliceStable(out, func(i, j int) bool {
		bi, bj := parseInt64(out[i].BlockNumber), parseInt64(out[j].BlockNumber)
		if bi != bj {
			return bi > bj
		}
		ti, tj := parseInt64(out[i].TimeStamp), parseInt64(out[j].TimeStamp)
		if ti != tj {
			return ti > tj
		}
		return out[i].Key() > out[j].Key()
	})
	return out
}

// NewestBlock returns the highest block number among the transfers (0 if none).
func NewestBlock(transfers []TokenTransfer) int64 {
	var max int64
	for _, t := range transfers {
		if b := parseInt64(t.BlockNumber); b > max {
			max = b
		}
	}
	return max
}

// Request pacing and backoff.
//
// Blockscout's public instances throttle bursts (HTTP 429) — 16 back-to-back
// tokentx queries (8 Gnosis accounts × 2 contracts) tripped it. Requests to
// the same explorer are spaced by minRequestGap, and a throttled request
// waits Retry-After (or an exponentially growing delay) before trying again.
const explorerMaxAttempts = 5

var (
	minRequestGap = map[string]time.Duration{
		"blockscout": 1200 * time.Millisecond,
		"etherscan":  250 * time.Millisecond, // free plan: 5 req/s
	}
	sleepFn = time.Sleep // injectable for tests
	nowFn   = time.Now

	paceMu      sync.Mutex
	lastRequest = map[string]time.Time{}
)

// pace blocks until at least minRequestGap has passed since the previous
// request to this explorer.
func (ep explorerEndpoint) pace() {
	gap := minRequestGap[ep.Name]
	if gap <= 0 {
		return
	}
	paceMu.Lock()
	wait := gap - nowFn().Sub(lastRequest[ep.Name])
	if wait > 0 {
		sleepFn(wait)
	}
	lastRequest[ep.Name] = nowFn()
	paceMu.Unlock()
}

// retryDelay is how long to wait before retrying a throttled request:
// Retry-After when given (seconds), else 2s doubling per attempt, capped.
func retryDelay(retryAfter string, attempt int) time.Duration {
	if secs, err := strconv.ParseFloat(strings.TrimSpace(retryAfter), 64); err == nil && secs > 0 {
		return time.Duration(secs * float64(time.Second))
	}
	d := 2 * time.Second << uint(attempt)
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	return d
}
