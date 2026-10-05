package cmd

// Contribution tokens issued, as a feed: YYYY/MM/<tier>/tokens-issued.json
// and latest/<tier>/tokens-issued.json (the last 60 days). Every mint of
// the contribution token (tokens.json "contribution": true): when, how
// many, to whom (Discord identity) and why (the issuing bot's Nostr
// annotation).
//
// Sources: the raw token transfers (providers/etherscan), the trusted
// Nostr metadata of the chain (providers/nostr/<chainId>/metadata.json,
// trusted authors only: the token bot counts once a seed follows it), and
// the wallet → Discord map in stewards/contributors.json.
//
// public/members carry no transaction id and no wallet address. Note that
// an amount and a time can still be matched with the public on-chain mint,
// and the token bot itself publishes "Issued N CHT to <user>" with the
// transaction on Nostr. stewards/ adds the transaction URI and the wallet.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	etherscansource "github.com/CommonsHub/chb/providers/etherscan"
	nostrsource "github.com/CommonsHub/chb/providers/nostr"
)

const tokensIssuedFile = "tokens-issued.json"

type TokensIssuedFile struct {
	GeneratedAt string        `json:"generatedAt"`
	Token       string        `json:"token"`
	Issued      []TokenIssued `json:"issued"` // newest first
}

type TokenIssued struct {
	ID         string                `json:"id,omitempty"`     // stewards only: transaction URI
	Wallet     string                `json:"wallet,omitempty"` // stewards only
	Timestamp  string                `json:"timestamp"`
	Amount     float64               `json:"amount"`
	Recipient  *ContributionIdentity `json:"recipient"` // null when the wallet is not linked to a Discord account
	Reason     string                `json:"reason,omitempty"`
	Kind       string                `json:"kind,omitempty"`
	DiscordURL *string               `json:"discordUrl"`
}

var issuedPrefix = regexp.MustCompile(`(?i)^\s*issued\s+[\d.,]+\s+\S+\s+to\s+\S+\s+for\s+(an?\s+)?`)

// issuedReason drops the bot's "Issued 3 CHT to leen8610 for a" prefix.
func issuedReason(desc string) string {
	r := strings.TrimSpace(issuedPrefix.ReplaceAllString(desc, ""))
	if containsEmail(r) {
		return ""
	}
	return r
}

func issuedKind(tags map[string]string) string {
	if t := strings.TrimSpace(tags["t"]); t != "" {
		return t
	}
	if tags["role"] != "" {
		return "role"
	}
	return ""
}

// walletIdentities maps wallet → Discord identity from every stewards
// contributors.json.
func walletIdentities(dataDir string) map[string]ContributionIdentity {
	out := map[string]ContributionIdentity{}
	paths, _ := filepath.Glob(filepath.Join(dataDir, "*", "*", stewardsDirName, "contributors.json"))
	sort.Strings(paths) // later months win
	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		var f MonthlyContributorsFile
		if json.Unmarshal(data, &f) != nil {
			continue
		}
		for _, c := range f.Contributors {
			if c.Address == nil || *c.Address == "" {
				continue
			}
			name := c.Profile.Name
			if containsEmail(name) {
				name = c.Profile.Username
				if containsEmail(name) {
					name = ""
				}
			}
			out[strings.ToLower(*c.Address)] = ContributionIdentity{ID: c.ID, DisplayName: name}
		}
	}
	return out
}

func monthTokensIssued(dataDir, year, month string, tok *TokenConfig, meta NostrMetadataCache, wallets map[string]ContributionIdentity, ex *txExclusions) []TokenIssued {
	path, found := etherscansource.FindFile(dataDir, year, month, tok.Chain, tok.Slug, tok.Symbol)
	if !found {
		return nil
	}
	cache, ok := etherscansource.LoadCache(path)
	if !ok {
		return nil
	}
	zero := "0x0000000000000000000000000000000000000000"
	var out []TokenIssued
	for _, tx := range cache.Transactions {
		if !strings.EqualFold(tx.From, zero) || strings.EqualFold(tx.To, zero) || ex.hash(tx.Hash) != "" {
			continue
		}
		dec := tok.Decimals
		if tx.TokenDecimal != "" {
			if d, err := strconv.Atoi(tx.TokenDecimal); err == nil {
				dec = d
			}
		}
		secs, _ := strconv.ParseInt(tx.TimeStamp, 10, 64)
		it := TokenIssued{
			ID:        BuildBlockchainURI(tok.ChainID, tx.Hash),
			Wallet:    strings.ToLower(tx.To),
			Timestamp: time.Unix(secs, 0).UTC().Format(time.RFC3339),
			Amount:    roundCents(etherscansource.ParseTokenValue(tx.Value, dec)),
		}
		if id, ok := wallets[strings.ToLower(tx.To)]; ok {
			id := id
			it.Recipient = &id
		}
		if m := meta.Transactions[strings.ToLower(tx.Hash)]; m != nil {
			it.Reason = issuedReason(m.Description)
			it.Kind = issuedKind(m.Tags)
			if r := strings.TrimSpace(m.Tags["r"]); strings.HasPrefix(r, "https://discord.com/") {
				it.DiscordURL = &r
			}
		}
		out = append(out, it)
	}
	return out
}

func sortTokensIssued(xs []TokenIssued) {
	sort.SliceStable(xs, func(i, j int) bool {
		if xs[i].Timestamp != xs[j].Timestamp {
			return xs[i].Timestamp > xs[j].Timestamp
		}
		return xs[i].ID > xs[j].ID
	})
}

func tokensIssuedForAudience(f TokensIssuedFile, a Audience) TokensIssuedFile {
	if a == AudienceStewards {
		return f
	}
	out := f
	out.Issued = make([]TokenIssued, len(f.Issued))
	for i, it := range f.Issued {
		it.ID, it.Wallet = "", ""
		out.Issued[i] = it
	}
	return out
}

// writeTokensIssued writes every tier unless the stewards copy already
// holds the same entries (no churn in completed months).
func writeTokensIssued(dataDir, year, month string, f TokensIssuedFile) {
	if data, err := os.ReadFile(audiencePath(dataDir, year, month, AudienceStewards, tokensIssuedFile)); err == nil {
		var prev TokensIssuedFile
		if json.Unmarshal(data, &prev) == nil && prev.Token == f.Token {
			a, _ := json.Marshal(prev.Issued)
			b, _ := json.Marshal(f.Issued)
			if string(a) == string(b) {
				return
			}
		}
	}
	writeTiersNoMirror(dataDir, year, month, tokensIssuedFile, func(a Audience) interface{} { return tokensIssuedForAudience(f, a) })
}

// generateTokensIssued writes every month that has the token's transfers
// and the latest window; returns the number of mints in the window.
func generateTokensIssued(dataDir string) int {
	tok := ContributionTokenConfig(LoadTokenConfigs())
	if tok == nil || tok.ChainID == 0 {
		return 0
	}
	meta := trustedNostrMetadata(LoadNostrMetadataCache(filepath.Join(dataDir, "latest", nostrsource.RelPath(strconv.Itoa(tok.ChainID), nostrsource.MetadataFile))))
	wallets := walletIdentities(dataDir)
	ex := loadTxExclusions(dataDir)
	now := time.Now().UTC()
	byMonth := map[string][]TokenIssued{}
	for _, ym := range dataMonthRange(dataDir) {
		xs := monthTokensIssued(dataDir, ym[:4], ym[5:], tok, meta, wallets, ex)
		if xs == nil {
			continue
		}
		sortTokensIssued(xs)
		byMonth[ym] = xs
		writeTokensIssued(dataDir, ym[:4], ym[5:], TokensIssuedFile{GeneratedAt: now.Format(time.RFC3339), Token: tok.Symbol, Issued: xs})
	}
	cutoff := now.AddDate(0, 0, -contributionsLatestDays).Format(time.RFC3339)
	recent := []TokenIssued{}
	for _, xs := range byMonth {
		for _, it := range xs {
			if it.Timestamp >= cutoff {
				recent = append(recent, it)
			}
		}
	}
	sortTokensIssued(recent)
	writeTokensIssued(dataDir, "latest", "", TokensIssuedFile{GeneratedAt: now.Format(time.RFC3339), Token: tok.Symbol, Issued: recent})
	return len(recent)
}
