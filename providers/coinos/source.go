// Package coinos archives the payment history of a coinos.io wallet. The
// payments API requires the account's auth token (COINOS_TOKEN — the JWT
// shown under coinos.io → Settings → Account → API token / auth token).
// Raw payment objects are stored unchanged, grouped by the Brussels month of
// their creation time.
package coinos

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/CommonsHub/chb/providers"
)

const Source = "coinos"

const defaultAPIURL = "https://coinos.io/api"

type SourceProvider struct{}

func (SourceProvider) Name() string {
	return Source
}

func (SourceProvider) Files() []providers.File {
	return []providers.File{
		{Name: "<account>.json", Description: "Monthly coinos wallet payments (raw API JSON).", Private: true},
	}
}

// CacheFile is one month of raw payments for one coinos user.
type CacheFile struct {
	Username string            `json:"username"`
	CachedAt string            `json:"cachedAt"`
	Payments []json.RawMessage `json:"transactions"`
}

// Payment is the subset of a coinos payment that generate reads. Amounts are
// in sats; amount is negative for outgoing payments. Rate is the BTC price in
// Currency at the time of payment.
type Payment struct {
	ID        string  `json:"id"`
	Amount    int64   `json:"amount"`
	Fee       int64   `json:"fee"`
	OurFee    int64   `json:"ourfee"`
	Hash      string  `json:"hash"`
	Memo      string  `json:"memo"`
	Type      string  `json:"type"`
	Rate      float64 `json:"rate"`
	Currency  string  `json:"currency"`
	Ref       string  `json:"ref"`
	Tip       int64   `json:"tip"`
	Confirmed *bool   `json:"confirmed"`
	Created   int64   `json:"created"` // unix milliseconds
	With      *struct {
		Username string `json:"username"`
	} `json:"with"`
}

func RelPath(elems ...string) string {
	parts := append([]string{"providers", Source}, elems...)
	return filepath.Join(parts...)
}

func Path(dataDir, year, month string, elems ...string) string {
	return filepath.Join(append([]string{dataDir, year, month}, RelPath(elems...))...)
}

func FileName(slug string) string {
	return strings.ToLower(strings.TrimSpace(slug)) + ".json"
}

func APIURL() string {
	if v := strings.TrimRight(strings.TrimSpace(os.Getenv("COINOS_API_URL")), "/"); v != "" {
		return v
	}
	return defaultAPIURL
}

// ProfileURL is the public page of a coinos user.
func ProfileURL(username string) string { return "https://coinos.io/" + username }

func LoadCache(path string) (CacheFile, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return CacheFile{}, false
	}
	var cache CacheFile
	if json.Unmarshal(data, &cache) != nil {
		return CacheFile{}, false
	}
	return cache, true
}

func WriteCache(path string, cache CacheFile) error {
	data, err := json.MarshalIndent(cache, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		return err
	}
	_ = os.Chmod(path, 0600)
	return nil
}

// SamePayments reports whether two raw payment lists are identical (after
// compaction), so unchanged months are not rewritten.
func SamePayments(a, b []json.RawMessage) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		var ca, cb bytes.Buffer
		if json.Compact(&ca, a[i]) != nil || json.Compact(&cb, b[i]) != nil || !bytes.Equal(ca.Bytes(), cb.Bytes()) {
			return false
		}
	}
	return true
}

// FetchPayments returns the full payment history of the authenticated user
// as raw JSON objects, paging with limit/offset.
func FetchPayments(token string) ([]json.RawMessage, error) {
	client := &http.Client{Timeout: 30 * time.Second}
	const limit = 100
	var out []json.RawMessage
	seen := map[string]bool{}
	for offset := 0; offset < 1000000; offset += limit {
		q := url.Values{}
		q.Set("limit", fmt.Sprint(limit))
		q.Set("offset", fmt.Sprint(offset))
		req, err := http.NewRequest("GET", APIURL()+"/payments?"+q.Encode(), nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Accept", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("fetch payments: %w", err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("fetch payments: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		}
		batch, err := decodePayments(body)
		if err != nil {
			return nil, err
		}
		added := 0
		for _, raw := range batch {
			var p Payment
			if json.Unmarshal(raw, &p) != nil || p.ID == "" || seen[p.ID] {
				continue
			}
			seen[p.ID] = true
			out = append(out, raw)
			added++
		}
		// Stop on a short page, or when the server ignores offset and keeps
		// returning payments we already have.
		if len(batch) < limit || added == 0 {
			break
		}
	}
	return out, nil
}

// decodePayments accepts both `{"payments": [...]}` and a bare array.
func decodePayments(body []byte) ([]json.RawMessage, error) {
	var wrapped struct {
		Payments []json.RawMessage `json:"payments"`
	}
	if err := json.Unmarshal(body, &wrapped); err == nil && wrapped.Payments != nil {
		return wrapped.Payments, nil
	}
	var list []json.RawMessage
	if err := json.Unmarshal(body, &list); err != nil {
		return nil, fmt.Errorf("decode payments: %w", err)
	}
	return list, nil
}

// FetchBalance returns the authenticated user's balance in sats.
func FetchBalance(token string) (int64, error) {
	client := &http.Client{Timeout: 30 * time.Second}
	req, err := http.NewRequest("GET", APIURL()+"/me", nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	var me struct {
		Balance int64 `json:"balance"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&me); err != nil {
		return 0, err
	}
	return me.Balance, nil
}
