package cmd

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	discordsource "github.com/CommonsHub/chb/providers/discord"
	etherscansource "github.com/CommonsHub/chb/providers/etherscan"
	kbcbrusselssource "github.com/CommonsHub/chb/providers/kbcbrussels"
	moneriumsource "github.com/CommonsHub/chb/providers/monerium"
	nostrsource "github.com/CommonsHub/chb/providers/nostr"
	odoosource "github.com/CommonsHub/chb/providers/odoo"
	stripesource "github.com/CommonsHub/chb/providers/stripe"
)

// ── Data types ──────────────────────────────────────────────────────────────

// ActivityGridData is the output format for activitygrid.json
type ActivityGridData struct {
	Years []ActivityGridYear `json:"years"`
}

type ActivityGridYear struct {
	Year   string              `json:"year"`
	Months []ActivityGridMonth `json:"months"`
}

type ActivityGridMonth struct {
	Month            string `json:"month"`
	ContributorCount int    `json:"contributorCount"`
	PhotoCount       int    `json:"photoCount"`
}

// ImageEntry represents an image in images.json
type ImageEntry struct {
	URL            string          `json:"url"`
	ID             string          `json:"id"`
	Author         ImageAuthor     `json:"author"`
	Reactions      json.RawMessage `json:"reactions,omitempty"`
	TotalReactions int             `json:"totalReactions"`
	Message        string          `json:"message"`
	Timestamp      string          `json:"timestamp"`
	ChannelID      string          `json:"channelId"`
	MessageID      string          `json:"messageId"`
	FilePath       string          `json:"filePath,omitempty"`
}

type ImageAuthor struct {
	ID          string `json:"id"`
	Username    string `json:"username"`
	DisplayName string `json:"displayName"`
	Avatar      string `json:"avatar,omitempty"`
}

type ImagesFile struct {
	Year   string       `json:"year,omitempty"`
	Month  string       `json:"month,omitempty"`
	Source string       `json:"source,omitempty"`
	Count  int          `json:"count"`
	Images []ImageEntry `json:"images"`
}

// ContributorProfile holds contributor info for monthly contributors.json
type ContributorProfile struct {
	Name        string   `json:"name"`
	Username    string   `json:"username"`
	Description *string  `json:"description"`
	AvatarURL   *string  `json:"avatar_url"`
	Roles       []string `json:"roles"`
}

type ContributorTokens struct {
	In  float64 `json:"in"`
	Out float64 `json:"out"`
}

type ContributorDiscord struct {
	Messages int `json:"messages"`
	Mentions int `json:"mentions"`
}

type ContributorEntry struct {
	ID      string             `json:"id"`
	Profile ContributorProfile `json:"profile"`
	Tokens  ContributorTokens  `json:"tokens"`
	Discord ContributorDiscord `json:"discord"`
	Address *string            `json:"address"`
}

type MonthlyContributorsFile struct {
	Year         string             `json:"year,omitempty"`
	Month        string             `json:"month,omitempty"`
	Period       string             `json:"period,omitempty"`
	Since        string             `json:"since,omitempty"`
	Until        string             `json:"until,omitempty"`
	Summary      ContributorSummary `json:"summary"`
	Contributors []ContributorEntry `json:"contributors"`
	GeneratedAt  string             `json:"generatedAt"`
}

type ContributorSummary struct {
	TotalContributors     int     `json:"totalContributors"`
	ContributorsWithAddr  int     `json:"contributorsWithAddress"`
	ContributorsWithToken int     `json:"contributorsWithTokens"`
	TotalTokensIn         float64 `json:"totalTokensIn"`
	TotalTokensOut        float64 `json:"totalTokensOut"`
	TotalMessages         int     `json:"totalMessages"`
	TotalImages           int     `json:"totalImages"`
	TotalDiscordMembers   int     `json:"totalDiscordMembers,omitempty"`
}

type contributorsRunCache struct {
	dataDir                   string
	discordMemberCount        int
	discordMemberCountFetched bool
	wallets                   *walletResolutionCache
	walletsDirty              bool
}

// TopContributor is the format in the global contributors.json
type TopContributor struct {
	ID                string  `json:"id"`
	Username          string  `json:"username"`
	DisplayName       string  `json:"displayName"`
	Avatar            *string `json:"avatar"`
	ContributionCount int     `json:"contributionCount"`
	JoinedAt          *string `json:"joinedAt"`
	WalletAddress     *string `json:"walletAddress"`
}

type TopContributorsFile struct {
	Contributors    []TopContributor `json:"contributors"`
	TotalMembers    int              `json:"totalMembers"`
	ActiveCommoners int              `json:"activeCommoners"`
	Timestamp       int64            `json:"timestamp"`
	IsMockData      bool             `json:"isMockData"`
}

// UserProfile is written to data/generated/profiles/{username}.json
type UserProfile struct {
	ID                string                  `json:"id"`
	Username          string                  `json:"username"`
	DisplayName       string                  `json:"displayName"`
	Avatar            *string                 `json:"avatar"`
	ContributionCount int                     `json:"contributionCount"`
	JoinedAt          *string                 `json:"joinedAt"`
	Introductions     []ProfileMessage        `json:"introductions"`
	Contributions     []ProfileMessage        `json:"contributions"`
	ImagesByMonth     map[string][]ImageEntry `json:"imagesByMonth"`
}

type ProfileMessage struct {
	Content     string          `json:"content"`
	Timestamp   string          `json:"timestamp"`
	Attachments json.RawMessage `json:"attachments,omitempty"`
	Reactions   json.RawMessage `json:"reactions,omitempty"`
	Mentions    json.RawMessage `json:"mentions,omitempty"`
	Author      json.RawMessage `json:"author,omitempty"`
	MessageID   string          `json:"messageId"`
	ChannelID   string          `json:"channelId"`
}

// YearlyUsersEntry is used in data/{year}/users.json
type YearlyUsersEntry struct {
	ID               string             `json:"id"`
	Profile          ContributorProfile `json:"profile"`
	Tokens           ContributorTokens  `json:"tokens"`
	Discord          ContributorDiscord `json:"discord"`
	Address          *string            `json:"address"`
	ContributionDays int                `json:"contributionDays"`
}

type YearlyUsersFile struct {
	Year         string             `json:"year"`
	Summary      YearlyUsersSummary `json:"summary"`
	Contributors []YearlyUsersEntry `json:"contributors"`
	GeneratedAt  string             `json:"generatedAt"`
}

type YearlyUsersSummary struct {
	TotalContributors     int     `json:"totalContributors"`
	ContributorsWithAddr  int     `json:"contributorsWithAddress"`
	ContributorsWithToken int     `json:"contributorsWithTokens"`
	TotalTokensIn         float64 `json:"totalTokensIn"`
	TotalTokensOut        float64 `json:"totalTokensOut"`
	TotalMessages         int     `json:"totalMessages"`
	TotalContributionDays int     `json:"totalContributionDays"`
}

// TransactionEntry for aggregated transactions.json
// centsToEuros converts an integer cent amount to euros with exact 2-decimal precision.
func centsToEuros(cents int64) float64 {
	return math.Round(float64(cents)) / 100
}

// roundCents rounds a float to exactly 2 decimal places (cent precision).
func roundCents(v float64) float64 {
	return math.Round(v*100) / 100
}

type TransactionEntry struct {
	ID               string  `json:"id"` // NIP-73 URI (matches the `i` tag used by Nostr annotations)
	Provider         string  `json:"provider"`
	ProviderID       string  `json:"providerId,omitempty"`
	AccountID        string  `json:"accountId,omitempty"`
	CounterpartyID   string  `json:"counterpartyId,omitempty"`
	Chain            *string `json:"chain,omitempty"`
	AccountSlug      string  `json:"accountSlug,omitempty"`
	AccountName      string  `json:"accountName,omitempty"`
	Currency         string  `json:"currency"`
	Value            string  `json:"value"`
	Amount           float64 `json:"amount"`
	NetAmount        float64 `json:"netAmount,omitempty"`
	GrossAmount      float64 `json:"grossAmount"`
	NormalizedAmount float64 `json:"normalizedAmount"`
	Fee              float64 `json:"fee"`
	Type             string  `json:"type"`
	Timestamp        int64   `json:"timestamp"`
	Application      string  `json:"application,omitempty"`
	StripeCustomerID string  `json:"stripeCustomerId,omitempty"`
	// Category and Collective live in Metadata in the public JSON
	// (metadata.category / metadata.collective). They're kept on the struct
	// for internal access by rules, reports and reconciliation; the custom
	// UnmarshalJSON below restores them from metadata when loading.
	Category   string `json:"-"`
	Collective string `json:"-"`
	// AccountCode and PartnerID are the Odoo-side mapping resolutions
	// computed at generate time by LookupOdooMapping. They live in
	// metadata.accountCode / metadata.partnerId in the serialized form
	// so consumers (KBC merge, Stripe push, categorize) can read them
	// without re-running the lookup chain. Producers MUST keep these
	// in sync with rules.json / odoo_mapping.json by re-running
	// `chb generate` after edits.
	AccountCode string                 `json:"-"`
	PartnerID   int                    `json:"-"`
	Event       string                 `json:"event,omitempty"`
	Tags        [][]string             `json:"tags,omitempty"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
	Spread      []SpreadEntry          `json:"spread,omitempty"`

	// Internal-only (omitempty + cleared when building publicTxs). Kept on
	// the struct so categorizer/rules/reconciliation logic and JSON fixtures
	// keep working — but the canonical public handles are
	// AccountID/CounterpartyID/ID/ProviderID.
	TxHash string `json:"txHash,omitempty"`
	// LogIndex disambiguates multiple transfers sharing the same TxHash
	// (e.g. a Safe multisend or DEX swap emits several ERC-20 Transfer
	// events in one tx). It's the 0-based ordinal of this transfer
	// among the same (account, hash) group as returned by etherscan.
	// Single-transfer txs — the common case — keep LogIndex=0 and
	// remain compatible with existing Odoo unique_import_id values.
	LogIndex       int    `json:"logIndex,omitempty"`
	Account        string `json:"account,omitempty"`
	Counterparty   string `json:"counterparty,omitempty"`
	StripeChargeID string `json:"stripeChargeId,omitempty"`
}

// MarshalJSON projects Category and Collective into metadata.<key> so the
// public JSON never carries duplicate root-level keys. AccountCode and
// PartnerID are deliberately NOT projected — those are Odoo-specific and
// live in providers/odoo/pending/ instead, keeping transactions.json
// target-agnostic.
func (tx TransactionEntry) MarshalJSON() ([]byte, error) {
	type alias TransactionEntry
	a := alias(tx)
	if tx.Category != "" || tx.Collective != "" || len(a.Metadata) > 0 {
		meta := make(map[string]interface{}, len(a.Metadata)+2)
		for k, v := range a.Metadata {
			meta[k] = v
		}
		if tx.Category != "" {
			meta["category"] = tx.Category
		} else {
			delete(meta, "category")
		}
		if tx.Collective != "" {
			meta["collective"] = tx.Collective
		} else {
			delete(meta, "collective")
		}
		// Strip any legacy accountCode/partnerId entries left over from
		// pre-pending generates so re-running `chb generate` cleanly
		// migrates older files.
		delete(meta, "accountCode")
		delete(meta, "partnerId")
		if len(meta) == 0 {
			a.Metadata = nil
		} else {
			a.Metadata = meta
		}
	}
	return json.Marshal(a)
}

// UnmarshalJSON populates internal-only convenience fields (Category /
// Collective) so consumers reading transactions.json keep working without
// changes. AccountCode / PartnerID are no longer emitted by MarshalJSON
// (they live in providers/odoo/pending/), but we still read them here for
// back-compat with older files generated before the pending split.
func (tx *TransactionEntry) UnmarshalJSON(data []byte) error {
	type alias TransactionEntry
	aux := struct {
		Category    string  `json:"category"`
		Collective  string  `json:"collective"`
		AccountCode string  `json:"accountCode"`
		PartnerID   float64 `json:"partnerId"`
		*alias
	}{alias: (*alias)(tx)}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	if aux.Category != "" {
		tx.Category = aux.Category
	} else if tx.Category == "" {
		tx.Category = stringMetadata(tx.Metadata, "category")
	}
	if aux.Collective != "" {
		tx.Collective = aux.Collective
	} else if tx.Collective == "" {
		tx.Collective = stringMetadata(tx.Metadata, "collective")
	}
	if aux.AccountCode != "" {
		tx.AccountCode = aux.AccountCode
	} else if tx.AccountCode == "" {
		tx.AccountCode = stringMetadata(tx.Metadata, "accountCode")
	}
	if aux.PartnerID != 0 {
		tx.PartnerID = int(aux.PartnerID)
	} else if tx.PartnerID == 0 {
		if v, ok := tx.Metadata["partnerId"]; ok {
			switch n := v.(type) {
			case float64:
				tx.PartnerID = int(n)
			case int:
				tx.PartnerID = n
			}
		}
	}
	// Public transactions.json strips TxHash (the canonical handle is the
	// NIP-73 ID URI). Restore it on load so verification, Odoo push and
	// orphan detection don't compare against an empty hash.
	if tx.TxHash == "" {
		tx.TxHash = TxHashFromURI(tx.ID)
	}
	return nil
}

// syncMetadataString writes value into tx.Metadata[key], or deletes the key
// when value is empty, so the public output never carries a stale string.
func syncMetadataString(tx *TransactionEntry, key, value string) {
	if value != "" {
		if tx.Metadata == nil {
			tx.Metadata = map[string]interface{}{}
		}
		tx.Metadata[key] = value
		return
	}
	if tx.Metadata != nil {
		delete(tx.Metadata, key)
	}
}

// IsIncoming returns true for credits (CREDIT, MINT).
func (tx TransactionEntry) IsIncoming() bool {
	return tx.Type == "CREDIT" || tx.Type == "MINT"
}

// IsOutgoing returns true for debits (DEBIT, BURN).
func (tx TransactionEntry) IsOutgoing() bool {
	return tx.Type == "DEBIT" || tx.Type == "BURN"
}

type TransactionsFile struct {
	Year         string             `json:"year"`
	Month        string             `json:"month"`
	GeneratedAt  string             `json:"generatedAt"`
	Transactions []TransactionEntry `json:"transactions"`
}

// TransactionPII holds private enrichment for a single transaction.
type TransactionPII struct {
	Name  string `json:"name,omitempty"`
	Email string `json:"email,omitempty"`
	IBAN  string `json:"iban,omitempty"`
}

// TransactionsPIIFile is saved to generated/private/enrichment.json.
type TransactionsPIIFile struct {
	GeneratedAt string                     `json:"generatedAt"`
	Enrichments map[string]*TransactionPII `json:"enrichments"` // keyed by transaction ID
}

// LoadTransactionsWithPII reads the public transactions file and merges PII from the private enrichment.
func LoadTransactionsWithPII(dataDir, year, month string) *TransactionsFile {
	txPath := filepath.Join(dataDir, year, month, "generated", "transactions.json")
	data, err := os.ReadFile(txPath)
	if err != nil {
		return nil
	}
	var txFile TransactionsFile
	if json.Unmarshal(data, &txFile) != nil {
		return nil
	}

	// Load PII enrichment
	piiPath := filepath.Join(dataDir, year, month, "generated", "private", "enrichment.json")
	piiData, err := os.ReadFile(piiPath)
	if err != nil {
		return &txFile // no PII file, return public data as-is
	}
	var piiFile TransactionsPIIFile
	if json.Unmarshal(piiData, &piiFile) != nil {
		return &txFile
	}

	// Merge PII back into transactions
	for i := range txFile.Transactions {
		tx := &txFile.Transactions[i]
		if pii, ok := piiFile.Enrichments[tx.ID]; ok {
			if pii.Name != "" {
				tx.Counterparty = pii.Name
			}
			if pii.Email != "" {
				if tx.Metadata == nil {
					tx.Metadata = map[string]interface{}{}
				}
				tx.Metadata["email"] = pii.Email
			}
			if iban := normalizeIBAN(pii.IBAN); iban != "" {
				if tx.Metadata == nil {
					tx.Metadata = map[string]interface{}{}
				}
				tx.Metadata["iban"] = iban
			}
		}
	}

	return &txFile
}

// CounterpartyEntry mirrors AddressMetadata so a counterparty has the same
// shape regardless of where it came from (celo, gnosis, stripe, iban, …).
// Entries are keyed by their NIP-73 URI in CounterpartiesFile.Counterparties,
// so the URI doesn't need to be repeated inside the value.
type CounterpartyEntry struct {
	Name         string            `json:"name,omitempty"`
	Slug         string            `json:"slug,omitempty"` // populated only for our own tracked accounts
	About        string            `json:"about,omitempty"`
	Picture      string            `json:"picture,omitempty"`
	Tags         map[string]string `json:"tags,omitempty"`
	NostrEventID string            `json:"nostrEventId,omitempty"`
	Author       string            `json:"author,omitempty"`
	CreatedAt    int64             `json:"createdAt,omitempty"`
}

type CounterpartiesFile struct {
	Month          string                       `json:"month"`
	GeneratedAt    string                       `json:"generatedAt"`
	Counterparties map[string]CounterpartyEntry `json:"counterparties"`
}

// ── Message reading helpers ─────────────────────────────────────────────────

type cachedMessageFile struct {
	Messages []json.RawMessage `json:"messages"`
}

type messageBasic struct {
	ID          string `json:"id"`
	AuthorID    string
	AuthorUser  string
	AuthorName  string
	AuthorAvat  string
	Content     string `json:"content"`
	Timestamp   string `json:"timestamp"`
	Attachments []struct {
		ID          string `json:"id"`
		URL         string `json:"url"`
		ContentType string `json:"content_type"`
	} `json:"attachments"`
	Mentions []struct {
		ID         string  `json:"id"`
		Username   string  `json:"username"`
		GlobalName *string `json:"global_name"`
		Avatar     *string `json:"avatar"`
	} `json:"mentions"`
	Reactions []struct {
		Emoji struct {
			Name string `json:"name"`
		} `json:"emoji"`
		Count int `json:"count"`
	} `json:"reactions"`
}

func parseMessage(raw json.RawMessage) messageBasic {
	var m struct {
		ID     string `json:"id"`
		Author struct {
			ID         string  `json:"id"`
			Username   string  `json:"username"`
			GlobalName *string `json:"global_name"`
			Avatar     *string `json:"avatar"`
		} `json:"author"`
		Content     string `json:"content"`
		Timestamp   string `json:"timestamp"`
		Attachments []struct {
			ID          string `json:"id"`
			URL         string `json:"url"`
			ContentType string `json:"content_type"`
		} `json:"attachments"`
		Mentions []struct {
			ID         string  `json:"id"`
			Username   string  `json:"username"`
			GlobalName *string `json:"global_name"`
			Avatar     *string `json:"avatar"`
		} `json:"mentions"`
		Reactions []struct {
			Emoji struct {
				Name string `json:"name"`
			} `json:"emoji"`
			Count int `json:"count"`
		} `json:"reactions"`
	}
	json.Unmarshal(raw, &m)

	mb := messageBasic{
		ID:          m.ID,
		Content:     m.Content,
		Timestamp:   m.Timestamp,
		Attachments: m.Attachments,
		Mentions:    m.Mentions,
		Reactions:   m.Reactions,
	}
	mb.AuthorID = m.Author.ID
	mb.AuthorUser = m.Author.Username
	if m.Author.GlobalName != nil {
		mb.AuthorName = *m.Author.GlobalName
	} else {
		mb.AuthorName = m.Author.Username
	}
	if m.Author.Avatar != nil {
		mb.AuthorAvat = *m.Author.Avatar
	}
	return mb
}

// readMessages reads all discord messages for a given year/month across all channels
func readMessages(dataDir, year, month string) []json.RawMessage {
	discordDir := discordsource.Path(dataDir, year, month)
	entries, err := os.ReadDir(discordDir)
	if err != nil {
		return nil
	}

	var all []json.RawMessage
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		msgPath := filepath.Join(discordDir, e.Name(), discordsource.MessagesFile)
		data, err := os.ReadFile(msgPath)
		if err != nil {
			continue
		}
		var f cachedMessageFile
		if json.Unmarshal(data, &f) == nil {
			all = append(all, f.Messages...)
		}
	}
	return all
}

// readChannelMessages reads messages from a specific channel
func readChannelMessages(dataDir, year, month, channelID string) []json.RawMessage {
	msgPath := discordsource.ChannelPath(dataDir, year, month, channelID)
	data, err := os.ReadFile(msgPath)
	if err != nil {
		return nil
	}
	var f cachedMessageFile
	if json.Unmarshal(data, &f) == nil {
		return f.Messages
	}
	return nil
}

// getAvailableYears returns year directories in data dir
func getAvailableYears(dataDir string) []string {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return nil
	}
	var years []string
	for _, e := range entries {
		if e.IsDir() && len(e.Name()) == 4 {
			years = append(years, e.Name())
		}
	}
	sort.Strings(years)
	return years
}

// getAvailableMonths returns month directories for a year
func getAvailableMonths(dataDir, year string) []string {
	yearDir := filepath.Join(dataDir, year)
	entries, err := os.ReadDir(yearDir)
	if err != nil {
		return nil
	}
	var months []string
	for _, e := range entries {
		if e.IsDir() && len(e.Name()) == 2 {
			months = append(months, e.Name())
		}
	}
	sort.Strings(months)
	return months
}

// getAllChannelIDs gets all Discord channel IDs from settings
func getAllChannelIDs() []string {
	settings, err := LoadSettings()
	if err != nil {
		return nil
	}
	channels := GetDiscordChannelIDs(settings)
	ids := make(map[string]bool)
	for _, id := range channels {
		ids[id] = true
	}
	result := make([]string, 0, len(ids))
	for id := range ids {
		result = append(result, id)
	}
	sort.Strings(result)
	return result
}

// ── Generate command ────────────────────────────────────────────────────────

// Generate runs all derived-data generators (images, contributors,
// transactions, counterparties, members, events, …). Prefer the targeted
// variants (GenerateTransactions, GenerateMessages, GenerateEvents,
// GenerateMembers) after a scoped sync — they skip unrelated work.
func Generate(args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		printGenerateHelp()
		return nil
	}

	verbose := HasFlag(args, "--verbose", "-v") || HasFlag(args, "--debug")
	// Per-phase diagnostic capture: each genStep buckets its own
	// Warnf calls so the row can show a ⚠ mark. The per-phase
	// "Issues" block has been removed — warnings stay in the log
	// file and the count is summarised once at process exit. Reset
	// at the start so this phase's buckets don't pick up stale
	// state from the pull phase.
	if !verbose {
		ResetCapturedStepDiagnostics()
	}
	dataDir := DataDir()
	now := time.Now().In(BrusselsTZ())
	posStartMonth, posEndMonth, posFound := ParseMonthRangeArg(args)
	startMonth, isHistory := ResolveSinceMonth(args, "")
	endMonth := ""
	if !isHistory && !posFound {
		startMonth = DefaultRecentStartMonth(now)
	}
	if posFound {
		startMonth = posStartMonth
		endMonth = posEndMonth
	}

	latestDir := filepath.Join(dataDir, "latest")
	hadLatestDir := dirExists(latestDir)

	years := getAvailableYears(dataDir)
	if len(years) == 0 {
		Warnf("%s⚠ No data found. Run sync first.%s", Fmt.Yellow, Fmt.Reset)
		return nil
	}

	allScopes := collectGenerateScopes(dataDir, years, startMonth, endMonth)
	// Also reprocess any PREVIOUSLY-generated month outside the recent window
	// whose source files moved since it was last generated — e.g. a `pull` that
	// backfilled charges/products into old months. Without this, those changes
	// would silently never reach the generated output (the default window only
	// covers recent months). Only months with an existing cursor are added, so a
	// fresh setup still needs --history for the full backlog.
	allScopes = append(allScopes, collectStaleGeneratedScopes(dataDir, years, startMonth)...)
	sort.Slice(allScopes, func(i, j int) bool {
		if allScopes[i].Year != allScopes[j].Year {
			return allScopes[i].Year < allScopes[j].Year
		}
		return allScopes[i].Month < allScopes[j].Month
	})
	scopeYears := uniqueGenerateScopeYears(allScopes)

	// Dirty-scope filter: drop months whose source files haven't moved
	// since the last successful generate. --force / --history opts back
	// into rebuilding everything in the window. The original window's
	// scopeYears is preserved so cross-month rollups still iterate the
	// full year set even when individual months are clean.
	force := HasFlag(args, "--force")
	scopes, cleanScopes := filterDirtyGenerateScopes(dataDir, allScopes, force || isHistory)

	if verbose {
		fmt.Printf("\n%s🔧 Generating derived data files...%s\n", Fmt.Bold, Fmt.Reset)
		fmt.Printf("%sDATA_DIR: %s%s\n\n", Fmt.Dim, dataDir, Fmt.Reset)
		fmt.Printf("📋 Found %s: %s\n\n", Pluralize(len(years), "year", ""), strings.Join(years, ", "))
		if len(scopes) > 0 {
			first := scopes[0].Year + "-" + scopes[0].Month
			last := scopes[len(scopes)-1].Year + "-" + scopes[len(scopes)-1].Month
			fmt.Printf("%sGeneration window: %s → %s%s\n\n", Fmt.Dim, first, last, Fmt.Reset)
		}
	} else {
		header := "Generating derived data"
		if len(allScopes) > 0 {
			first := allScopes[0].Year + "-" + allScopes[0].Month
			last := allScopes[len(allScopes)-1].Year + "-" + allScopes[len(allScopes)-1].Month
			if first == last {
				header += " for " + first
			} else {
				header += " for " + first + " → " + last
			}
		}
		if len(scopes) == 0 && len(cleanScopes) > 0 {
			header += " — all sources unchanged, rebuilding rollups only"
		}
		// Match the pull/push banner shape: 2-space gutter + bold, no
		// trailing blank line. Keeps the three sync phases visually
		// consistent and avoids the 3-blank-line gap that the old
		// double-newline produced.
		fmt.Printf("\n  %s%s%s\n", Fmt.Bold, header, Fmt.Reset)
	}

	// Write latest/generated/ README
	writeGeneratedReadme(dataDir)

	settings, _ := LoadSettings()

	// genStep runs fn with stdout swallowed (compact mode) or untouched
	// (verbose), times it, and prints a live status line via StatusLine
	// (compact) or a per-step banner (verbose). fn returns a short
	// summary string ("247 images") shown after the spinner finishes.
	genStep := func(label string, fn func() string) {
		if verbose {
			summary := fn()
			if summary != "" {
				fmt.Printf("  %s%s%s\n", Fmt.Dim, summary, Fmt.Reset)
			}
			return
		}
		diag := BeginStepDiagnostics(label)
		sl := NewStatusLine(label)
		SetActiveStatusLine(sl)
		restore := silenceStdout()
		summary := fn()
		restore()
		SetActiveStatusLine(nil)
		EndStepDiagnostics()
		// Generate sub-steps never return errors directly; the ⚠ mark
		// surfaces any Warnf emitted inside fn (e.g. PII guard) and the
		// detail lands in the footer.
		sl.Final(StepMark(nil, diag), summary)
	}
	_ = genStep // referenced below in step blocks

	genStep("Images", func() string {
		total := 0
		for _, scope := range scopes {
			if n := generateMonthImagesGo(dataDir, scope.Year, scope.Month); n > 0 {
				if verbose {
					fmt.Printf("  ✓ %s-%s: %s\n", scope.Year, scope.Month, Pluralize(n, "image", ""))
				}
				total += n
			}
		}
		if hadLatestDir {
			if n := generateMonthImagesGo(dataDir, "latest", ""); n > 0 {
				if verbose {
					fmt.Printf("  ✓ latest: %s\n", Pluralize(n, "image", ""))
				}
				total += n
			}
		}
		return Pluralize(total, "image", "")
	})

	genStep("Activity grids", func() string {
		grid := generateActivityGridGo(dataDir, years)
		for _, year := range scopeYears {
			generateYearActivityGridGo(dataDir, year, grid)
		}
		return Pluralize(len(scopeYears), "year", "")
	})

	genStep("Monthly contributors", func() string {
		cache := newContributorsRunCache(dataDir)
		total := 0
		for _, scope := range scopes {
			if n := generateMonthContributorsGo(dataDir, scope.Year, scope.Month, settings, cache, time.Time{}); n > 0 {
				if verbose {
					fmt.Printf("  ✓ %s-%s: %s\n", scope.Year, scope.Month, Pluralize(n, "contributor", ""))
				}
				total += n
			}
		}
		if hadLatestDir {
			cutoff := time.Now().UTC().AddDate(0, 0, -LatestContributorsWindowDays)
			if n := generateMonthContributorsGo(dataDir, "latest", "", settings, cache, cutoff); n > 0 {
				if verbose {
					fmt.Printf("  ✓ latest (%dd): %s\n", LatestContributorsWindowDays, Pluralize(n, "contributor", ""))
				}
				total += n
			}
		}
		cache.save()
		return Pluralize(total, "contributor", "")
	})

	genStep("Top contributors", func() string {
		generateTopContributorsGo(dataDir, settings)
		return "contributors.json"
	})

	genStep("User profiles", func() string {
		generateUserProfilesGo(dataDir, settings)
		return ""
	})

	genStep("Yearly users", func() string {
		for _, year := range scopeYears {
			generateYearlyUsersGo(dataDir, year, settings)
		}
		return Pluralize(len(scopeYears), "year", "")
	})

	genStep("Transactions", func() string {
		total := 0
		for _, scope := range scopes {
			if n := generateTransactionsGo(dataDir, scope.Year, scope.Month, settings); n > 0 {
				if verbose {
					fmt.Printf("  ✓ %s-%s: %s\n", scope.Year, scope.Month, Pluralize(n, "transaction", ""))
				}
				total += n
			}
		}
		if hadLatestDir {
			if n := generateTransactionsGo(dataDir, "latest", "", settings); n > 0 {
				if verbose {
					fmt.Printf("  ✓ latest: %s\n", Pluralize(n, "transaction", ""))
				}
				total += n
			}
		}
		return Pluralize(total, "transaction", "")
	})

	genStep("Members", func() string {
		generateMembersGo(dataDir, scopes)
		return ""
	})

	genStep("Event ticket sales", func() string {
		enrichEventsWithTicketSales(dataDir)
		return ""
	})

	genStep("Latest events", func() string {
		generateLatestEventsGo(dataDir)
		generateMarkdownFiles(dataDir)
		return ""
	})

	genStep("Counterparties", func() string {
		for _, scope := range scopes {
			generateCounterpartiesGo(dataDir, scope.Year, scope.Month)
		}
		if hadLatestDir {
			generateCounterpartiesGo(dataDir, "latest", "")
		}
		return ""
	})

	genStep("Inbound-spread indexes", func() string {
		if err := rebuildInboundSpreads(dataDir); err != nil {
			return "error: " + err.Error()
		}
		return ""
	})

	genStep("Host commissions", func() string {
		if err := rebuildCommissions(dataDir); err != nil {
			return "error: " + err.Error()
		}
		return ""
	})

	genStep("Monthly summaries", func() string {
		total := 0
		for _, scope := range scopes {
			if generateMonthlyReportGo(dataDir, scope.Year, scope.Month, settings) {
				if verbose {
					fmt.Printf("  ✓ %s-%s: generated/summary.json\n", scope.Year, scope.Month)
				}
				total++
			}
		}
		return Pluralize(total, "summary", "summaries")
	})

	genStep("Cross-month balances", func() string {
		n, err := rebuildSummaryRollup(dataDir)
		if err != nil {
			return "error: " + err.Error()
		}
		return fmt.Sprintf("%d collective row%s", n, plural(n))
	})

	// Stamp cursors for the months we just regenerated so subsequent
	// runs can skip them when no source file moved.
	for _, s := range scopes {
		stampGenerateMonthCursor(dataDir, s.Year, s.Month)
	}

	if verbose {
		fmt.Printf("\n%s✅ All data generation complete!%s\n\n", Fmt.Green, Fmt.Reset)
	} else {
		if len(cleanScopes) > 0 {
			fmt.Printf("  %s%d month%s skipped (sources unchanged; --force to rebuild)%s\n",
				Fmt.Dim, len(cleanScopes), plural(len(cleanScopes)), Fmt.Reset)
		}
		fmt.Println()
	}
	return nil
}

// GenerateTransactions runs only the transaction-related generators. This
// is what AccountFetch / `chb accounts <slug> sync` invoke, since they
// don't touch messages, images, events, members, etc.
//
// Steps: aggregated transactions (`transactions.json`) + counterparties.
func GenerateTransactions(args []string) error {
	startedAt := time.Now()
	dataDir := DataDir()
	now := time.Now().In(BrusselsTZ())
	posStartMonth, posEndMonth, posFound := ParseMonthRangeArg(args)
	startMonth, isHistory := ResolveSinceMonth(args, "")
	endMonth := ""
	if !isHistory && !posFound {
		startMonth = DefaultRecentStartMonth(now)
	}
	if posFound {
		startMonth = posStartMonth
		endMonth = posEndMonth
	}

	years := getAvailableYears(dataDir)
	if len(years) == 0 {
		return nil
	}
	scopes := collectGenerateScopes(dataDir, years, startMonth, endMonth)
	return generateTransactionScopes(dataDir, scopes, startedAt)
}

func GenerateTransactionsForMonths(months []string) error {
	startedAt := time.Now()
	dataDir := DataDir()
	seen := map[string]bool{}
	var scopes []generateScope
	for _, month := range months {
		year, m, ok := parseGenerateMonth(month)
		if !ok {
			continue
		}
		key := year + "-" + m
		if seen[key] {
			continue
		}
		seen[key] = true
		scopes = append(scopes, generateScope{Year: year, Month: m})
	}
	sort.Slice(scopes, func(i, j int) bool {
		return scopes[i].Year+scopes[i].Month < scopes[j].Year+scopes[j].Month
	})
	return generateTransactionScopes(dataDir, scopes, startedAt)
}

func parseGenerateMonth(month string) (string, string, bool) {
	if y, m, ok := ParseSinceMonth(month); ok {
		return y, m, true
	}
	if len(month) == 7 && month[4] == '/' {
		return ParseSinceMonth(strings.ReplaceAll(month, "/", "-"))
	}
	return "", "", false
}

func generateTransactionScopes(dataDir string, scopes []generateScope, startedAt time.Time) error {
	settings, _ := LoadSettings()
	latestDir := filepath.Join(dataDir, "latest")

	fmt.Printf("\n%sGenerating standardized transaction data...%s\n", Fmt.Bold, Fmt.Reset)
	fmt.Printf("  Date range: %s -> %s\n", firstGenerateScopeLabel(scopes), lastGenerateScopeLabel(scopes))
	fmt.Printf("  Data dir: %s\n\n", dataDir)
	totalTx := 0
	processorNames := registeredDataProcessorNames()
	for _, scope := range scopes {
		fmt.Printf("%s/%s\n", scope.Year, scope.Month)
		status := newStatusLine()
		status.Update("Generating %s...", displayMonthRelPath(scope.Year, scope.Month, filepath.Join("generated", "transactions.json")))
		n := generateTransactionsGo(dataDir, scope.Year, scope.Month, settings)
		status.Clear()
		if n > 0 {
			fmt.Printf("  %s✓%s generated/transactions.json (%d transactions)\n", Fmt.Green, Fmt.Reset, n)
			if len(processorNames) > 0 {
				fmt.Printf("  %s✓%s processors: %s\n", Fmt.Green, Fmt.Reset, strings.Join(processorNames, ", "))
			}
			totalTx += n
		}
		status.Update("Generating %s...", displayMonthRelPath(scope.Year, scope.Month, filepath.Join("generated", "counterparties.json")))
		cpCount := generateCounterpartiesGo(dataDir, scope.Year, scope.Month)
		status.Clear()
		if cpCount > 0 {
			fmt.Printf("  %s✓%s generated/counterparties.json (%d counterparties)\n", Fmt.Green, Fmt.Reset, cpCount)
		}
		status.Update("Generating %s...", displayMonthRelPath(scope.Year, scope.Month, filepath.Join("generated", "summary.json")))
		reportWritten := generateMonthlyReportGo(dataDir, scope.Year, scope.Month, settings)
		status.Clear()
		if reportWritten {
			fmt.Printf("  %s✓%s generated/summary.json\n", Fmt.Green, Fmt.Reset)
		}
	}
	if _, err := os.Stat(latestDir); err == nil {
		fmt.Printf("latest\n")
		status := newStatusLine()
		status.Update("Generating %s...", displayMonthRelPath("latest", "", filepath.Join("generated", "transactions.json")))
		n := generateTransactionsGo(dataDir, "latest", "", settings)
		status.Clear()
		if n > 0 {
			fmt.Printf("  %s✓%s generated/transactions.json (%d transactions)\n", Fmt.Green, Fmt.Reset, n)
			if len(processorNames) > 0 {
				fmt.Printf("  %s✓%s processors: %s\n", Fmt.Green, Fmt.Reset, strings.Join(processorNames, ", "))
			}
			totalTx += n
		}
		status.Update("Generating %s...", displayMonthRelPath("latest", "", filepath.Join("generated", "counterparties.json")))
		cpCount := generateCounterpartiesGo(dataDir, "latest", "")
		status.Clear()
		if cpCount > 0 {
			fmt.Printf("  %s✓%s generated/counterparties.json (%d counterparties)\n", Fmt.Green, Fmt.Reset, cpCount)
		}
	}

	elapsed := time.Since(startedAt).Round(time.Millisecond)
	fmt.Printf("\n%s✓ Transaction generation complete%s: %d tx across %s, %s\n\n",
		Fmt.Green, Fmt.Reset, totalTx, Pluralize(len(scopes), "month", ""), elapsed)
	return nil
}

// GenerateEvents runs only event-related generators: it attaches ticket
// sales to every events.json (requires transactions to have been generated
// already) and then rebuilds the latest-upcoming digest.
func GenerateEvents(args []string) error {
	dataDir := DataDir()
	years := getAvailableYears(dataDir)
	if len(years) == 0 {
		return nil
	}
	fmt.Printf("\n%s🎟  Attaching ticket sales to events...%s\n", Fmt.Bold, Fmt.Reset)
	enrichEventsWithTicketSales(dataDir)
	fmt.Printf("\n%s📅 Generating latest events...%s\n", Fmt.Bold, Fmt.Reset)
	generateLatestEventsGo(dataDir)
	generateMarkdownFiles(dataDir)
	fmt.Printf("%s✓ Events generators complete%s\n\n", Fmt.Green, Fmt.Reset)
	return nil
}

// GenerateMessages runs only message-derived generators: per-month images.
func GenerateMessages(args []string) error {
	dataDir := DataDir()
	now := time.Now().In(BrusselsTZ())
	posStartMonth, posEndMonth, posFound := ParseMonthRangeArg(args)
	startMonth, isHistory := ResolveSinceMonth(args, "")
	endMonth := ""
	if !isHistory && !posFound {
		startMonth = DefaultRecentStartMonth(now)
	}
	if posFound {
		startMonth = posStartMonth
		endMonth = posEndMonth
	}
	years := getAvailableYears(dataDir)
	if len(years) == 0 {
		return nil
	}
	scopes := collectGenerateScopes(dataDir, years, startMonth, endMonth)

	fmt.Printf("\n%s📸 Generating images...%s\n", Fmt.Bold, Fmt.Reset)
	total := 0
	for _, scope := range scopes {
		n := generateMonthImagesGo(dataDir, scope.Year, scope.Month)
		if n > 0 {
			fmt.Printf("  ✓ %s-%s: %s\n", scope.Year, scope.Month, Pluralize(n, "image", ""))
			total += n
		}
	}
	if latestDir := filepath.Join(dataDir, "latest"); dirExists(latestDir) {
		n := generateMonthImagesGo(dataDir, "latest", "")
		if n > 0 {
			fmt.Printf("  ✓ latest: %s\n", Pluralize(n, "image", ""))
			total += n
		}
	}
	fmt.Printf("%s✓ Message generators complete%s (%d images)\n\n", Fmt.Green, Fmt.Reset, total)
	return nil
}

// GenerateMembers runs only member-related generators.
func GenerateMembers(args []string) error {
	dataDir := DataDir()
	now := time.Now().In(BrusselsTZ())
	posStartMonth, posEndMonth, posFound := ParseMonthRangeArg(args)
	startMonth, isHistory := ResolveSinceMonth(args, "")
	endMonth := ""
	if !isHistory && !posFound {
		startMonth = DefaultRecentStartMonth(now)
	}
	if posFound {
		startMonth = posStartMonth
		endMonth = posEndMonth
	}
	years := getAvailableYears(dataDir)
	if len(years) == 0 {
		return nil
	}
	scopes := collectGenerateScopes(dataDir, years, startMonth, endMonth)

	fmt.Printf("\n%s👥 Generating members...%s\n", Fmt.Bold, Fmt.Reset)
	generateMembersGo(dataDir, scopes)
	fmt.Printf("%s✓ Member generators complete%s\n\n", Fmt.Green, Fmt.Reset)
	return nil
}

func dirExists(p string) bool {
	if st, err := os.Stat(p); err == nil && st.IsDir() {
		return true
	}
	return false
}

type generateScope struct {
	Year  string
	Month string
}

// collectStaleGeneratedScopes returns months BEFORE the recent window that were
// generated before but whose source files have moved since (cursor is stale).
// This makes `chb generate` pick up out-of-window source changes — e.g. a pull
// that backfilled products into old-month charges — without needing --history.
func collectStaleGeneratedScopes(dataDir string, years []string, beforeMonth string) []generateScope {
	var out []generateScope
	for _, year := range years {
		for _, month := range getAvailableMonths(dataDir, year) {
			ym := year + "-" + month
			if beforeMonth == "" || ym >= beforeMonth {
				continue // in-window months are handled by collectGenerateScopes
			}
			cur := LoadSyncCursor(SyncCursorKeyForGenerateMonth(year, month))
			if cur.UpdatedAt.IsZero() {
				continue // never generated — leave the backlog to --history
			}
			if monthSourceMaxMTime(dataDir, year, month).After(cur.MaxSourceMTime) {
				out = append(out, generateScope{Year: year, Month: month})
			}
		}
	}
	return out
}

func collectGenerateScopes(dataDir string, years []string, startMonth, endMonth string) []generateScope {
	var scopes []generateScope
	for _, year := range years {
		for _, month := range getAvailableMonths(dataDir, year) {
			ym := year + "-" + month
			if startMonth != "" && ym < startMonth {
				continue
			}
			if endMonth != "" && ym > endMonth {
				continue
			}
			scopes = append(scopes, generateScope{Year: year, Month: month})
		}
	}
	return scopes
}

// monthSourceMaxMTime returns the most recent mtime of any file under
// the month's providers/ + sources/ trees. Used to short-circuit the
// generate pipeline for months that haven't received fresh provider
// data since the last run.
func monthSourceMaxMTime(dataDir, year, month string) time.Time {
	var latest time.Time
	for _, sub := range []string{"providers", "sources"} {
		root := filepath.Join(dataDir, year, month, sub)
		_ = filepath.Walk(root, func(_ string, info os.FileInfo, err error) error {
			if err != nil || info == nil || info.IsDir() {
				return nil
			}
			if t := info.ModTime(); t.After(latest) {
				latest = t
			}
			return nil
		})
	}
	return latest
}

// filterDirtyGenerateScopes returns only scopes whose source files have
// changed since the last generate run for that month, plus any scope
// for which we have no cursor yet. force=true returns all scopes
// unchanged.
func filterDirtyGenerateScopes(dataDir string, scopes []generateScope, force bool) (dirty, clean []generateScope) {
	if force {
		return scopes, nil
	}
	for _, s := range scopes {
		cur := LoadSyncCursor(SyncCursorKeyForGenerateMonth(s.Year, s.Month))
		mtime := monthSourceMaxMTime(dataDir, s.Year, s.Month)
		if cur.UpdatedAt.IsZero() || mtime.After(cur.MaxSourceMTime) || cur.MaxSourceMTime.IsZero() {
			dirty = append(dirty, s)
			continue
		}
		clean = append(clean, s)
	}
	return dirty, clean
}

// stampGenerateMonthCursor records that we've processed a month and
// the max source mtime at the time of processing. Subsequent runs use
// this to skip months whose sources haven't moved.
func stampGenerateMonthCursor(dataDir, year, month string) {
	mtime := monthSourceMaxMTime(dataDir, year, month)
	_ = SaveSyncCursor(SyncCursor{
		Key:            SyncCursorKeyForGenerateMonth(year, month),
		MaxSourceMTime: mtime,
	})
}

func uniqueGenerateScopeYears(scopes []generateScope) []string {
	seen := map[string]bool{}
	var years []string
	for _, scope := range scopes {
		if seen[scope.Year] {
			continue
		}
		seen[scope.Year] = true
		years = append(years, scope.Year)
	}
	return years
}

func firstGenerateScopeLabel(scopes []generateScope) string {
	if len(scopes) == 0 {
		return "-"
	}
	return scopes[0].Year + "-" + scopes[0].Month
}

func lastGenerateScopeLabel(scopes []generateScope) string {
	if len(scopes) == 0 {
		return "-"
	}
	last := scopes[len(scopes)-1]
	return last.Year + "-" + last.Month
}

// ── Image generation ────────────────────────────────────────────────────────

func generateMonthImagesGo(dataDir, year, month string) int {
	var images []ImageEntry
	discordDir := discordsource.Path(dataDir, year, month)
	entries, _ := os.ReadDir(discordDir)
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		channelID := e.Name()
		msgPath := filepath.Join(discordDir, channelID, discordsource.MessagesFile)
		data, err := os.ReadFile(msgPath)
		if err != nil {
			continue
		}
		var f cachedMessageFile
		if json.Unmarshal(data, &f) != nil {
			continue
		}

		for _, raw := range f.Messages {
			m := parseMessage(raw)
			for _, att := range m.Attachments {
				if !isDiscordImageAttachment(att.ContentType, att.URL) {
					continue
				}

				totalReactions := 0
				for _, r := range m.Reactions {
					totalReactions += r.Count
				}

				reactionsJSON, _ := json.Marshal(convertReactions(m.Reactions))
				ext := extFromURL(att.URL, ".jpg")
				filePath := relativeDiscordImagePathFromTimestamp(m.Timestamp, att.ID, ext)

				images = append(images, ImageEntry{
					URL:            att.URL,
					ID:             att.ID,
					Author:         ImageAuthor{ID: m.AuthorID, Username: m.AuthorUser, DisplayName: m.AuthorName, Avatar: m.AuthorAvat},
					Reactions:      reactionsJSON,
					TotalReactions: totalReactions,
					Message:        m.Content,
					Timestamp:      m.Timestamp,
					ChannelID:      channelID,
					MessageID:      m.ID,
					FilePath:       filePath,
				})
			}
		}
	}

	if len(images) == 0 {
		return 0
	}

	// Sort by totalReactions desc
	sort.Slice(images, func(i, j int) bool {
		return images[i].TotalReactions > images[j].TotalReactions
	})

	// De-duplicate by ID
	seen := map[string]bool{}
	var unique []ImageEntry
	for _, img := range images {
		if !seen[img.ID] {
			seen[img.ID] = true
			unique = append(unique, img)
		}
	}
	images = unique

	out := ImagesFile{Year: year, Month: month, Count: len(images), Images: images}
	imgData, _ := marshalIndentedNoHTMLEscape(out)
	writeMonthFile(dataDir, year, month, filepath.Join("generated", "images.json"), imgData)

	return len(images)
}

func isDiscordImageAttachment(contentType, rawURL string) bool {
	if strings.HasPrefix(contentType, "image/") {
		return true
	}
	urlClean := strings.Split(rawURL, "?")[0]
	ext := strings.ToLower(filepath.Ext(urlClean))
	return ext == ".jpg" || ext == ".jpeg" || ext == ".png" || ext == ".gif" || ext == ".webp"
}

// ── Activity grid ───────────────────────────────────────────────────────────

func generateActivityGridGo(dataDir string, years []string) ActivityGridData {
	var grid ActivityGridData

	for _, year := range years {
		months := getAvailableMonths(dataDir, year)
		var yearMonths []ActivityGridMonth

		for _, month := range months {
			rawMsgs := readMessages(dataDir, year, month)
			contributorIDs := map[string]bool{}
			photoCount := 0

			for _, raw := range rawMsgs {
				m := parseMessage(raw)
				if m.AuthorID != "" {
					contributorIDs[m.AuthorID] = true
				}
				for _, mention := range m.Mentions {
					if mention.ID != "" {
						contributorIDs[mention.ID] = true
					}
				}
				for _, att := range m.Attachments {
					if strings.HasPrefix(att.ContentType, "image/") {
						photoCount++
					}
				}
			}

			yearMonths = append(yearMonths, ActivityGridMonth{
				Month:            month,
				ContributorCount: len(contributorIDs),
				PhotoCount:       photoCount,
			})
		}

		grid.Years = append(grid.Years, ActivityGridYear{Year: year, Months: yearMonths})
	}

	outputPath := filepath.Join(dataDir, "latest", "generated", "activitygrid.json")
	writeJSONFile(outputPath, grid)
	fmt.Printf("  ✓ Generated global activity grid\n")

	return grid
}

func generateYearActivityGridGo(dataDir, year string, grid ActivityGridData) {
	for _, y := range grid.Years {
		if y.Year == year {
			out := struct {
				Year   string              `json:"year"`
				Months []ActivityGridMonth `json:"months"`
			}{Year: year, Months: y.Months}
			outputPath := filepath.Join(dataDir, year, "generated", "activitygrid.json")
			os.MkdirAll(filepath.Dir(outputPath), 0755)
			writeJSONFile(outputPath, out)
			fmt.Printf("  ✓ %s activity grid\n", year)
			return
		}
	}
}

// ── Monthly contributors ────────────────────────────────────────────────────

func newContributorsRunCache(dataDir string) *contributorsRunCache {
	return &contributorsRunCache{
		dataDir: dataDir,
		wallets: loadWalletResolutionCache(dataDir),
	}
}

func (c *contributorsRunCache) getDiscordMemberCount(settings *Settings) int {
	if c == nil {
		return fetchDiscordMemberCount(settings)
	}
	if !c.discordMemberCountFetched {
		c.discordMemberCount = fetchDiscordMemberCount(settings)
		c.discordMemberCountFetched = true
	}
	return c.discordMemberCount
}

func (c *contributorsRunCache) save() {
	if c == nil || !c.walletsDirty {
		return
	}
	if err := saveWalletResolutionCache(c.dataDir, c.wallets); err == nil {
		c.walletsDirty = false
	}
}

func contributorsOutputPath(dataDir, year, month string) string {
	return filepath.Join(dataDir, year, month, "generated", "contributors.json")
}

func contributorInputPaths(dataDir, year, month string) []string {
	var inputs []string

	settingsPath := settingsFilePath("settings.json")
	if _, err := os.Stat(settingsPath); err == nil {
		inputs = append(inputs, settingsPath)
	}

	imagesPath := filepath.Join(dataDir, year, month, "generated", "images.json")
	if _, err := os.Stat(imagesPath); err == nil {
		inputs = append(inputs, imagesPath)
	}

	discordDir := discordsource.Path(dataDir, year, month)
	if entries, err := os.ReadDir(discordDir); err == nil {
		for _, e := range entries {
			if e.IsDir() {
				inputs = append(inputs, filepath.Join(discordDir, e.Name(), discordsource.MessagesFile))
			}
		}
	}

	etherscanCeloDir := etherscansource.Path(dataDir, year, month, "celo")
	if entries, err := os.ReadDir(etherscanCeloDir); err == nil {
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			if e.Name() == "CHT.json" || strings.HasSuffix(e.Name(), ".CHT.json") {
				inputs = append(inputs, filepath.Join(etherscanCeloDir, e.Name()))
			}
		}
	}

	return inputs
}

func isGeneratedFileUpToDate(outputPath string, inputPaths []string) bool {
	if len(inputPaths) == 0 {
		return false
	}

	outInfo, err := os.Stat(outputPath)
	if err != nil {
		return false
	}

	outMod := outInfo.ModTime()
	for _, inputPath := range inputPaths {
		inInfo, err := os.Stat(inputPath)
		if err != nil {
			continue
		}
		if inInfo.ModTime().After(outMod) {
			return false
		}
	}

	return true
}

func readMonthlyContributorCount(path string) int {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	var f MonthlyContributorsFile
	if json.Unmarshal(data, &f) != nil {
		return 0
	}
	if len(f.Contributors) > 0 {
		return len(f.Contributors)
	}
	return f.Summary.TotalContributors
}

// LatestContributorsWindowDays controls the rolling window used when computing
// latest/generated/contributors.json.
const LatestContributorsWindowDays = 90

// parseMessageTimestamp parses Discord/image timestamps, which are RFC3339 with
// microsecond precision (e.g. "2026-04-14T13:34:10.952000+00:00"). Returns zero
// time on failure.
func parseMessageTimestamp(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t
	}
	return time.Time{}
}

// generateMonthContributorsGo builds contributors.json for a year/month. If
// cutoff is non-zero, messages, token transfers, and images older than cutoff
// are excluded (used for the rolling latest/ window).
func generateMonthContributorsGo(dataDir, year, month string, settings *Settings, runCache *contributorsRunCache, cutoff time.Time) int {
	discordDir := discordsource.Path(dataDir, year, month)
	if _, err := os.Stat(discordDir); os.IsNotExist(err) {
		return 0
	}

	outputPath := contributorsOutputPath(dataDir, year, month)
	// The cutoff depends on wall-clock time, so skip the mtime-based freshness
	// check whenever it's active; otherwise the same mtimes keep serving a stale
	// window across runs.
	if cutoff.IsZero() && isGeneratedFileUpToDate(outputPath, contributorInputPaths(dataDir, year, month)) {
		return readMonthlyContributorCount(outputPath)
	}

	type userInfo struct {
		id, username, displayName, avatar string
		messages, mentions                int
		description                       string
	}

	users := map[string]*userInfo{}

	// Read all channel messages
	entries, _ := os.ReadDir(discordDir)
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		msgs := readChannelMessages(dataDir, year, month, e.Name())
		for _, raw := range msgs {
			m := parseMessage(raw)
			if m.AuthorID == "" {
				continue
			}
			if !cutoff.IsZero() {
				if ts := parseMessageTimestamp(m.Timestamp); ts.IsZero() || ts.Before(cutoff) {
					continue
				}
			}
			u, ok := users[m.AuthorID]
			if !ok {
				u = &userInfo{id: m.AuthorID, username: m.AuthorUser, displayName: m.AuthorName, avatar: m.AuthorAvat}
				users[m.AuthorID] = u
			}
			u.messages++

			for _, mention := range m.Mentions {
				if mention.ID == "" {
					continue
				}
				mu, ok := users[mention.ID]
				if !ok {
					name := mention.Username
					if mention.GlobalName != nil {
						name = *mention.GlobalName
					}
					av := ""
					if mention.Avatar != nil {
						av = *mention.Avatar
					}
					mu = &userInfo{id: mention.ID, username: mention.Username, displayName: name, avatar: av}
					users[mention.ID] = mu
				}
				mu.mentions++
			}
		}
	}

	if len(users) == 0 {
		return 0
	}

	type chtTx struct {
		From      string `json:"from"`
		To        string `json:"to"`
		Value     string `json:"value"`
		TimeStamp string `json:"timeStamp"`
	}
	var chtTxs []chtTx

	etherscanCeloDir := etherscansource.Path(dataDir, year, month, "celo")
	if entries, err := os.ReadDir(etherscanCeloDir); err == nil {
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), ".CHT.json") {
				continue
			}
			if data, err := os.ReadFile(filepath.Join(etherscanCeloDir, e.Name())); err == nil {
				var txFile struct {
					Transactions []chtTx `json:"transactions"`
				}
				if json.Unmarshal(data, &txFile) == nil {
					chtTxs = append(chtTxs, txFile.Transactions...)
				}
			}
		}
	}

	decimals := 6 // CHT default
	if settings != nil && settings.ContributionToken != nil && settings.ContributionToken.Decimals > 0 {
		decimals = settings.ContributionToken.Decimals
	}

	zeroAddr := "0x0000000000000000000000000000000000000000"

	// Resolve Discord user IDs → wallet addresses via CardManager contract
	var discordIDs []string
	for _, u := range users {
		discordIDs = append(discordIDs, u.id)
	}
	var walletCache *walletResolutionCache
	if runCache != nil {
		walletCache = runCache.wallets
	}
	discordToAddr, walletsDirty := resolveDiscordToWalletMap(discordIDs, settings, walletCache)
	if walletsDirty && runCache != nil {
		runCache.walletsDirty = true
	}

	// Build per-address token totals
	type addrTokens struct {
		in, out float64
	}
	addrTotals := map[string]*addrTokens{}
	divisor := math.Pow10(decimals)
	for _, tx := range chtTxs {
		if !cutoff.IsZero() {
			secs, err := strconv.ParseInt(tx.TimeStamp, 10, 64)
			if err != nil || time.Unix(secs, 0).UTC().Before(cutoff) {
				continue
			}
		}
		val := 0.0
		if v, err := strconv.ParseFloat(tx.Value, 64); err == nil {
			val = v / divisor
		}
		if tx.From != zeroAddr {
			if _, ok := addrTotals[strings.ToLower(tx.From)]; !ok {
				addrTotals[strings.ToLower(tx.From)] = &addrTokens{}
			}
			addrTotals[strings.ToLower(tx.From)].out += val
		}
		if tx.To != zeroAddr {
			if _, ok := addrTotals[strings.ToLower(tx.To)]; !ok {
				addrTotals[strings.ToLower(tx.To)] = &addrTokens{}
			}
			addrTotals[strings.ToLower(tx.To)].in += val
		}
	}

	// Build contributors
	var contributors []ContributorEntry
	for _, u := range users {
		var tokensIn, tokensOut float64
		var address *string

		if walletAddr, ok := discordToAddr[u.id]; ok {
			address = &walletAddr
			if totals, ok := addrTotals[strings.ToLower(walletAddr)]; ok {
				tokensIn = math.Round(totals.in*100) / 100
				tokensOut = math.Round(totals.out*100) / 100
			}
		}

		var avatarURL *string
		if u.avatar != "" {
			s := fmt.Sprintf("https://cdn.discordapp.com/avatars/%s/%s.png", u.id, u.avatar)
			avatarURL = &s
		}

		contributors = append(contributors, ContributorEntry{
			ID: u.id,
			Profile: ContributorProfile{
				Name:        u.displayName,
				Username:    u.username,
				Description: nilIfEmpty(u.description),
				AvatarURL:   avatarURL,
				Roles:       []string{},
			},
			Tokens:  ContributorTokens{In: tokensIn, Out: tokensOut},
			Discord: ContributorDiscord{Messages: u.messages, Mentions: u.mentions},
			Address: address,
		})
	}

	// Sort by messages desc
	sort.Slice(contributors, func(i, j int) bool {
		return contributors[i].Discord.Messages > contributors[j].Discord.Messages
	})

	// Count images for this month
	totalImages := 0
	imagesPath := filepath.Join(dataDir, year, month, "generated", "images.json")
	if data, err := os.ReadFile(imagesPath); err == nil {
		var imgFile ImagesFile
		if json.Unmarshal(data, &imgFile) == nil {
			if cutoff.IsZero() {
				totalImages = imgFile.Count
			} else {
				for _, img := range imgFile.Images {
					if ts := parseMessageTimestamp(img.Timestamp); !ts.IsZero() && !ts.Before(cutoff) {
						totalImages++
					}
				}
			}
		}
	}

	// Get Discord member count
	discordMembers := fetchDiscordMemberCount(settings)
	if runCache != nil {
		discordMembers = runCache.getDiscordMemberCount(settings)
	}

	summary := ContributorSummary{
		TotalContributors:   len(contributors),
		TotalImages:         totalImages,
		TotalDiscordMembers: discordMembers,
	}
	for _, c := range contributors {
		summary.TotalMessages += c.Discord.Messages
		summary.TotalTokensIn += math.Round(c.Tokens.In*100) / 100
		summary.TotalTokensOut += math.Round(c.Tokens.Out*100) / 100
		if c.Address != nil {
			summary.ContributorsWithAddr++
		}
		if c.Tokens.In > 0 || c.Tokens.Out > 0 {
			summary.ContributorsWithToken++
		}
	}
	summary.TotalTokensIn = math.Round(summary.TotalTokensIn*100) / 100
	summary.TotalTokensOut = math.Round(summary.TotalTokensOut*100) / 100

	out := MonthlyContributorsFile{
		Summary:      summary,
		Contributors: contributors,
		GeneratedAt:  time.Now().UTC().Format(time.RFC3339),
	}
	if !cutoff.IsZero() {
		out.Period = fmt.Sprintf("%ddays", LatestContributorsWindowDays)
		out.Since = cutoff.Format("2006-01-02")
		out.Until = time.Now().UTC().Format("2006-01-02")
	} else {
		out.Year = year
		out.Month = month
	}

	contribData, _ := json.MarshalIndent(out, "", "  ")
	writeMonthFile(dataDir, year, month, filepath.Join("generated", "contributors.json"), contribData)

	return len(contributors)
}

// ── Top contributors (global) ───────────────────────────────────────────────

func generateTopContributorsGo(dataDir string, settings *Settings) {
	// Get contributions channel ID
	contributionsChannel := "1297965144579637248" // default
	if settings != nil {
		channels := GetDiscordChannelIDs(settings)
		if id, ok := channels["contributions"]; ok {
			contributionsChannel = id
		}
	}

	introductionsChannel := "1380592679364329522" // default
	if settings != nil {
		channels := GetDiscordChannelIDs(settings)
		if id, ok := channels["introductions"]; ok {
			introductionsChannel = id
		}
	}

	years := getAvailableYears(dataDir)
	now := time.Now()
	threeMonthsAgo := now.AddDate(0, -3, 0)

	type contribInfo struct {
		id, username, displayName string
		avatar                    *string
		contributionCount         int
		joinedAt                  *string
	}

	contributorMap := map[string]*contribInfo{}
	contributionCounts := map[string]int{}

	isBot := func(username string) bool {
		return strings.Contains(strings.ToLower(username), "bot")
	}
	isDeleted := func(username string) bool {
		return username == "Deleted User" || strings.HasPrefix(username, "deleted_user_")
	}

	// Collect from recent months
	for _, year := range years {
		months := getAvailableMonths(dataDir, year)
		for _, month := range months {
			// Parse month date
			y := 0
			m := 0
			fmt.Sscanf(year, "%d", &y)
			fmt.Sscanf(month, "%d", &m)
			monthDate := time.Date(y, time.Month(m), 1, 0, 0, 0, 0, time.UTC)
			if monthDate.Before(threeMonthsAgo) {
				continue
			}

			msgs := readChannelMessages(dataDir, year, month, contributionsChannel)
			for _, raw := range msgs {
				pm := parseMessage(raw)
				if isDeleted(pm.AuthorUser) || isBot(pm.AuthorUser) {
					continue
				}

				msgTime, _ := time.Parse(time.RFC3339, pm.Timestamp)
				if msgTime.IsZero() {
					msgTime, _ = time.Parse("2006-01-02T15:04:05.000Z", pm.Timestamp)
				}
				if msgTime.Before(threeMonthsAgo) {
					continue
				}

				contributionCounts[pm.AuthorID]++
				if _, ok := contributorMap[pm.AuthorID]; !ok {
					var av *string
					if pm.AuthorAvat != "" {
						s := fmt.Sprintf("https://cdn.discordapp.com/avatars/%s/%s.png", pm.AuthorID, pm.AuthorAvat)
						av = &s
					}
					ts := pm.Timestamp
					contributorMap[pm.AuthorID] = &contribInfo{
						id: pm.AuthorID, username: pm.AuthorUser, displayName: pm.AuthorName,
						avatar: av, joinedAt: &ts,
					}
				} else {
					existing := contributorMap[pm.AuthorID]
					if existing.joinedAt != nil && pm.Timestamp < *existing.joinedAt {
						existing.joinedAt = &pm.Timestamp
					}
				}

				// Process mentions
				for _, mention := range pm.Mentions {
					if isDeleted(mention.Username) || isBot(mention.Username) {
						continue
					}
					if _, ok := contributorMap[mention.ID]; !ok {
						var av *string
						if mention.Avatar != nil && *mention.Avatar != "" {
							s := fmt.Sprintf("https://cdn.discordapp.com/avatars/%s/%s.png", mention.ID, *mention.Avatar)
							av = &s
						}
						name := mention.Username
						if mention.GlobalName != nil {
							name = *mention.GlobalName
						}
						contributorMap[mention.ID] = &contribInfo{
							id: mention.ID, username: mention.Username, displayName: name,
							avatar: av,
						}
					}
				}
			}
		}
	}

	// Update contribution counts
	for id, count := range contributionCounts {
		if c, ok := contributorMap[id]; ok {
			c.contributionCount = count
		}
	}

	// Check introductions for joinedAt
	for _, year := range years {
		months := getAvailableMonths(dataDir, year)
		for _, month := range months {
			msgs := readChannelMessages(dataDir, year, month, introductionsChannel)
			for _, raw := range msgs {
				pm := parseMessage(raw)
				if isDeleted(pm.AuthorUser) || isBot(pm.AuthorUser) {
					continue
				}
				if c, ok := contributorMap[pm.AuthorID]; ok {
					if c.joinedAt == nil || pm.Timestamp < *c.joinedAt {
						c.joinedAt = &pm.Timestamp
					}
				}
			}
		}
	}

	// Build top 24
	var list []TopContributor
	for _, c := range contributorMap {
		list = append(list, TopContributor{
			ID:                c.id,
			Username:          c.username,
			DisplayName:       c.displayName,
			Avatar:            c.avatar,
			ContributionCount: c.contributionCount,
			JoinedAt:          c.joinedAt,
		})
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].ContributionCount > list[j].ContributionCount
	})
	if len(list) > 24 {
		list = list[:24]
	}

	out := TopContributorsFile{
		Contributors:    list,
		TotalMembers:    0,
		ActiveCommoners: len(contributorMap),
		Timestamp:       time.Now().Unix(),
		IsMockData:      false,
	}

	outputPath := filepath.Join(dataDir, "latest", "generated", "contributors.json")
	writeJSONFile(outputPath, out)
	fmt.Printf("  ✓ Generated contributors.json (%d contributors, %d active)\n", len(list), len(contributorMap))
}

// ── User profiles ───────────────────────────────────────────────────────────

func generateUserProfilesGo(dataDir string, settings *Settings) {
	// Collect all contributors
	type contribData struct {
		id, username, displayName string
		avatar                    *string
		contributionCount         int
		joinedAt                  *string
	}
	contributors := map[string]*contribData{}

	// From global contributors.json
	globalPath := filepath.Join(dataDir, "latest", "generated", "contributors.json")
	if data, err := os.ReadFile(globalPath); err == nil {
		var f TopContributorsFile
		if json.Unmarshal(data, &f) == nil {
			for _, c := range f.Contributors {
				contributors[c.ID] = &contribData{
					id: c.ID, username: c.Username, displayName: c.DisplayName,
					avatar: c.Avatar, contributionCount: c.ContributionCount, joinedAt: c.JoinedAt,
				}
			}
		}
	}

	if len(contributors) == 0 {
		Warnf("  %s⚠ No contributors found%s", Fmt.Yellow, Fmt.Reset)
		return
	}

	contributionsChannel := "1297965144579637248"
	introductionsChannel := "1380592679364329522"
	if settings != nil {
		channels := GetDiscordChannelIDs(settings)
		if id, ok := channels["contributions"]; ok {
			contributionsChannel = id
		}
		if id, ok := channels["introductions"]; ok {
			introductionsChannel = id
		}
	}

	profilesDir := filepath.Join(dataDir, "latest", "generated", "profiles")
	os.MkdirAll(profilesDir, 0755)

	profileCount := 0
	years := getAvailableYears(dataDir)

	for _, cd := range contributors {
		profile := UserProfile{
			ID:                cd.id,
			Username:          cd.username,
			DisplayName:       cd.displayName,
			Avatar:            cd.avatar,
			ContributionCount: cd.contributionCount,
			JoinedAt:          cd.joinedAt,
			Introductions:     []ProfileMessage{},
			Contributions:     []ProfileMessage{},
			ImagesByMonth:     map[string][]ImageEntry{},
		}

		for _, year := range years {
			months := getAvailableMonths(dataDir, year)
			for _, month := range months {
				key := fmt.Sprintf("%s-%s", year, month)

				// Introductions
				introMsgs := readChannelMessages(dataDir, year, month, introductionsChannel)
				for _, raw := range introMsgs {
					pm := parseMessage(raw)
					if pm.AuthorID == cd.id && len(pm.Content) > 10 {
						profile.Introductions = append(profile.Introductions, ProfileMessage{
							Content:   pm.Content,
							Timestamp: pm.Timestamp,
							MessageID: pm.ID,
							ChannelID: introductionsChannel,
						})
					}
				}

				// Contributions
				contribMsgs := readChannelMessages(dataDir, year, month, contributionsChannel)
				for _, raw := range contribMsgs {
					pm := parseMessage(raw)
					isAuthor := pm.AuthorID == cd.id
					isMentioned := false
					for _, mention := range pm.Mentions {
						if mention.ID == cd.id {
							isMentioned = true
							break
						}
					}
					if isAuthor || isMentioned {
						profile.Contributions = append(profile.Contributions, ProfileMessage{
							Content:   pm.Content,
							Timestamp: pm.Timestamp,
							MessageID: pm.ID,
							ChannelID: contributionsChannel,
						})
					}
				}

				// Images
				imagesPath := filepath.Join(dataDir, year, month, "generated", "images.json")
				if data, err := os.ReadFile(imagesPath); err == nil {
					var imf ImagesFile
					if json.Unmarshal(data, &imf) == nil {
						var userImages []ImageEntry
						for _, img := range imf.Images {
							if img.Author.ID == cd.id {
								userImages = append(userImages, img)
							}
						}
						if len(userImages) > 0 {
							profile.ImagesByMonth[key] = userImages
						}
					}
				}
			}
		}

		// Sort
		sort.Slice(profile.Introductions, func(i, j int) bool {
			return profile.Introductions[i].Timestamp < profile.Introductions[j].Timestamp
		})
		sort.Slice(profile.Contributions, func(i, j int) bool {
			return profile.Contributions[i].Timestamp > profile.Contributions[j].Timestamp
		})

		profilePath := filepath.Join(profilesDir, cd.username+".json")
		writeJSONFile(profilePath, profile)
		profileCount++
	}

	fmt.Printf("  ✓ Generated %s\n", Pluralize(profileCount, "user profile", ""))
}

// ── Yearly users ────────────────────────────────────────────────────────────

func generateYearlyUsersGo(dataDir, year string, settings *Settings) {
	months := getAvailableMonths(dataDir, year)
	contributionsChannel := "1297965144579637248"
	if settings != nil {
		channels := GetDiscordChannelIDs(settings)
		if id, ok := channels["contributions"]; ok {
			contributionsChannel = id
		}
	}

	// Aggregate monthly contributors
	userMap := map[string]*struct {
		entry ContributorEntry
		days  map[string]bool
	}{}

	for _, month := range months {
		contribPath := filepath.Join(dataDir, year, month, "generated", "contributors.json")
		data, err := os.ReadFile(contribPath)
		if err != nil {
			continue
		}
		var f MonthlyContributorsFile
		if json.Unmarshal(data, &f) != nil {
			continue
		}
		for _, c := range f.Contributors {
			if u, ok := userMap[c.ID]; ok {
				u.entry.Tokens.In += c.Tokens.In
				u.entry.Tokens.Out += c.Tokens.Out
				u.entry.Discord.Messages += c.Discord.Messages
				u.entry.Discord.Mentions += c.Discord.Mentions
				u.entry.Profile = c.Profile
				if c.Address != nil && u.entry.Address == nil {
					u.entry.Address = c.Address
				}
			} else {
				userMap[c.ID] = &struct {
					entry ContributorEntry
					days  map[string]bool
				}{entry: c, days: map[string]bool{}}
			}
		}
	}

	// Count contribution days
	for _, month := range months {
		msgs := readChannelMessages(dataDir, year, month, contributionsChannel)
		for _, raw := range msgs {
			pm := parseMessage(raw)
			date := ""
			if len(pm.Timestamp) >= 10 {
				date = pm.Timestamp[:10]
			}
			if u, ok := userMap[pm.AuthorID]; ok {
				u.days[date] = true
			}
			for _, mention := range pm.Mentions {
				if u, ok := userMap[mention.ID]; ok {
					u.days[date] = true
				}
			}
		}
	}

	var contributors []YearlyUsersEntry
	for _, u := range userMap {
		dayCount := len(u.days)
		if dayCount == 0 && u.entry.Tokens.In == 0 {
			continue
		}
		contributors = append(contributors, YearlyUsersEntry{
			ID:               u.entry.ID,
			Profile:          u.entry.Profile,
			Tokens:           u.entry.Tokens,
			Discord:          u.entry.Discord,
			Address:          u.entry.Address,
			ContributionDays: dayCount,
		})
	}

	sort.Slice(contributors, func(i, j int) bool {
		return contributors[i].Tokens.In > contributors[j].Tokens.In
	})

	summary := YearlyUsersSummary{TotalContributors: len(contributors)}
	for _, c := range contributors {
		summary.TotalTokensIn += c.Tokens.In
		summary.TotalTokensOut += c.Tokens.Out
		summary.TotalMessages += c.Discord.Messages
		summary.TotalContributionDays += c.ContributionDays
		if c.Address != nil {
			summary.ContributorsWithAddr++
		}
		if c.Tokens.In > 0 || c.Tokens.Out > 0 {
			summary.ContributorsWithToken++
		}
	}

	out := YearlyUsersFile{
		Year:         year,
		Summary:      summary,
		Contributors: contributors,
		GeneratedAt:  time.Now().UTC().Format(time.RFC3339),
	}

	outputPath := filepath.Join(dataDir, year, "generated", "contributors.json")
	os.MkdirAll(filepath.Dir(outputPath), 0755)
	writeJSONFile(outputPath, out)
	fmt.Printf("  ✓ %s: %d contributors\n", year, len(contributors))
}

// ── Transactions ────────────────────────────────────────────────────────────

func generateTransactionsGo(dataDir, year, month string, settings *Settings) int {
	stripePaths := stripesource.TransactionCachePaths(dataDir, year, month)
	etherscanDir := filepath.Join(dataDir, year, month, "providers", "etherscan")
	etherscanExists := true
	if _, err := os.Stat(etherscanDir); os.IsNotExist(err) {
		etherscanExists = false
	}
	moneriumDir := filepath.Join(dataDir, year, month, moneriumsource.RelPath())
	moneriumExists := true
	if _, err := os.Stat(moneriumDir); os.IsNotExist(err) {
		moneriumExists = false
	}
	// kbcbrussels is a manual CSV provider: a single rolling export sits
	// under latest/providers/kbcbrussels/ and the generator filters rows
	// by booking month. There's no per-month archive to stat — we let the
	// loader return nothing for accounts/months with no matching rows.
	kbcAccounts := kbcAccountsByIBAN()
	kbcEnabled := len(kbcAccounts) > 0
	if len(stripePaths) == 0 && !etherscanExists && !moneriumExists && !kbcEnabled {
		return 0
	}

	// Build set of all tracked wallet addresses to detect internal transfers,
	// plus a slug→address index used to reject stale/legacy Etherscan cache
	// files. When an account slug is repointed to a new wallet (e.g. savings
	// split from a hacked replacement address), old {slug}.EURe.json files may
	// remain on disk. Generating them under the new slug double-counts the old
	// wallet and makes Odoo journal sync drift.
	trackedAddresses := map[string]string{} // lowercase address -> account slug
	accountAddressBySlug := map[string]string{}
	if settings != nil {
		for _, acc := range settings.Finance.Accounts {
			if acc.Address != "" {
				addr := strings.ToLower(acc.Address)
				trackedAddresses[addr] = acc.Slug
				accountAddressBySlug[strings.ToLower(acc.Slug)] = addr
			}
		}
	}
	for _, acc := range LoadAccountConfigs() {
		if acc.Address != "" {
			addr := strings.ToLower(acc.Address)
			trackedAddresses[addr] = acc.Slug
			accountAddressBySlug[strings.ToLower(acc.Slug)] = addr
		}
	}

	var transactions []TransactionEntry

	// Process Stripe transactions
	if len(stripePaths) > 0 {
		for _, path := range stripePaths {
			data, err := os.ReadFile(path)
			if err != nil {
				continue
			}

			// Try Stripe source transaction archive format.
			var stripeCacheFile struct {
				Transactions []struct {
					ID                string                 `json:"id"`
					Amount            int64                  `json:"amount"`
					Net               int64                  `json:"net"`
					Fee               int64                  `json:"fee"`
					Currency          string                 `json:"currency"`
					Description       string                 `json:"description"`
					Created           int64                  `json:"created"`
					ReportingCategory string                 `json:"reporting_category"`
					Type              string                 `json:"type"`
					Source            json.RawMessage        `json:"source"`
					CustomerName      string                 `json:"customerName"`
					CustomerEmail     string                 `json:"customerEmail"`
					Metadata          map[string]interface{} `json:"metadata"`
				} `json:"transactions"`
				AccountID string `json:"accountId"`
				Currency  string `json:"currency"`
			}
			if json.Unmarshal(data, &stripeCacheFile) != nil || len(stripeCacheFile.Transactions) == 0 {
				continue
			}

			accountName := "💳 Stripe"
			accountSlug := "stripe"
			if stripeCacheFile.AccountID != "" {
				accountSlug = stripeCacheFile.AccountID
			}

			// Load private customer data (PII: names, emails)
			stripeCustomers := stripesource.LoadCustomerData(dataDir, year, month)

			// Load private charge data (app info, payment methods)
			stripeCharges, refundToCharge := stripesource.LoadChargeData(dataDir, year, month)

			for _, tx := range stripeCacheFile.Transactions {
				amount := centsToEuros(tx.Amount)
				fee := centsToEuros(tx.Fee)
				net := centsToEuros(tx.Net)
				txType := "CREDIT"
				if tx.Amount < 0 {
					txType = "DEBIT"
					amount = -amount
				}
				// Stripe billing fees (usage fees, taxes) are real debits from the
				// balance — but only when the money actually flows out. Stripe also
				// files credit grants (e.g. promo "free credit" adjustments) under
				// reporting_category "fee" with a positive amount; those increase
				// the balance and must stay CREDIT, or the type would contradict
				// the signed net amount and flip signs downstream (Odoo push/fix).
				if tx.ReportingCategory == "fee" && tx.Amount < 0 {
					txType = "DEBIT"
				}

				currency := strings.ToUpper(tx.Currency)
				if currency == "" {
					currency = "EUR"
				}

				// Determine counterparty: prefer private customer data, then inline, then charge data
				counterparty := tx.CustomerName
				// metadata is a summary of the most-common tags. The
				// `category` key is filled in after the enrichment chain
				// (rules / Nostr / Odoo) with the final assigned category —
				// not the Stripe `reporting_category` ("charge", "fee", …),
				// which isn't a category in our sense.
				metadata := map[string]interface{}{
					"description": tx.Description,
				}
				if tx.CustomerEmail != "" {
					metadata["email"] = tx.CustomerEmail
				}
				// Load from private customers file (PII stored separately)
				if stripeCustomers != nil {
					if cust, ok := stripeCustomers[tx.ID]; ok {
						if cust.Name != "" && counterparty == "" {
							counterparty = cust.Name
						}
						if cust.Email != "" && metadata["email"] == nil {
							metadata["email"] = cust.Email
						}
					}
				}
				// Merge inline metadata from expanded source. See
				// foldStripeMetadataValue: semantic keys (`name`,
				// `display_name`, `collective`) are promoted; the rest get a
				// `stripe_` prefix to avoid colliding with our own keys.
				for k, v := range tx.Metadata {
					if s, ok := v.(string); ok && s != "" {
						foldStripeMetadataValue(metadata, k, s)
					}
				}

				// Merge charge/session enrichment for app, payment-link and event
				// metadata even when the balance transaction already had customer
				// data. Refunds are mapped back to their original charge.
				chID := stripesource.ExtractChargeID(tx.Source)
				if chID == "" && refundToCharge != nil {
					srcID := stripesource.ExtractSourceID(tx.Source)
					if strings.HasPrefix(srcID, "re_") {
						chID = refundToCharge[srcID]
					}
				}
				customerID := ""
				if stripeCharges != nil && chID != "" {
					if ch, ok := stripeCharges[chID]; ok {
						if ch.CustomerID != "" {
							customerID = ch.CustomerID
						}
						if counterparty == "" {
							if name := ch.BestName(); name != "" {
								counterparty = name
							}
						}
						if metadata["email"] == nil {
							if email := ch.BestEmail(); email != "" {
								metadata["email"] = email
							}
						}
						if ch.ApplicationName != "" {
							metadata["application"] = ch.ApplicationName
						} else if ch.Application != "" {
							metadata["application"] = ch.Application
						}
						if ch.PaymentLink != "" {
							metadata["paymentLink"] = ch.PaymentLink
						}
						if ch.ProductName != "" {
							metadata["product"] = ch.ProductName
						}
						for k, v := range ch.Metadata {
							foldStripeMetadataValue(metadata, k, v)
						}
						for k, v := range ch.CustomFields {
							metadata["custom_"+k] = v
						}
					}
				}

				// Final fallback: for fee txs (Stripe Automatic Tax,
				// Usage Fee, processing fees) there's no customer or
				// vendor — the counterparty *is* Stripe itself. The
				// descriptive text lives in metadata.description so
				// rules.json can match it there. For other Stripe txs
				// without a customer (anonymous direct API charges),
				// keep using tx.Description as a useful label.
				if counterparty == "" {
					if strings.EqualFold(tx.ReportingCategory, "fee") {
						counterparty = "Stripe"
					} else {
						counterparty = tx.Description
					}
				}

				// Stash Stripe's reporting_category in metadata.kind so
				// rules.json can match on it ({"kind":"payout",...} →
				// category "payout"). Keeping classification in one
				// place — rules.json — avoids "magic" categorisation
				// that lives in code and surprises future readers.
				if tx.ReportingCategory != "" {
					metadata["kind"] = strings.ToLower(tx.ReportingCategory)
				}

				// Schema convention across providers:
				//   Amount           = signed gross (the canonical
				//                      customer-facing number, with
				//                      direction baked into the sign)
				//   GrossAmount      = |Amount| (positive magnitude)
				//   NetAmount        = signed net (after Stripe fees)
				//   NormalizedAmount = signed net (balance impact;
				//                      this is what account balance
				//                      math should consume)
				//   Fee              = positive fee magnitude
				// For providers without separate fees (etherscan, kbc)
				// gross == net so Amount == NormalizedAmount == NetAmount.
				signedGross := amount
				if txType == "DEBIT" || txType == "BURN" {
					signedGross = -amount
				}

				transactions = append(transactions, TransactionEntry{
					ID:               BuildStripeURI(tx.ID),
					ProviderID:       tx.ID,
					AccountID:        BuildStripeAccountURI(stripeCacheFile.AccountID),
					CounterpartyID:   BuildStripeCustomerURI(customerID),
					TxHash:           tx.ID,
					Provider:         "stripe",
					Account:          "stripe",
					AccountSlug:      accountSlug,
					AccountName:      accountName,
					Currency:         currency,
					Value:            fmt.Sprintf("%.2f", signedGross),
					Amount:           roundCents(signedGross),
					NetAmount:        roundCents(net),
					GrossAmount:      roundCents(math.Abs(amount)),
					NormalizedAmount: roundCents(net),
					Fee:              roundCents(fee),
					Type:             txType,
					Counterparty:     counterparty,
					Timestamp:        tx.Created,
					Application:      stringMetadata(metadata, "application"),
					StripeChargeID:   tx.ID,
					StripeCustomerID: customerID,
					Metadata:         metadata,
				})
			}
		}
	}

	// (chain, SYMBOL) → token contract address, used to build the
	// `ethereum:<chainId>:token:<contract>` URI when one side of a transfer
	// is the zero address (mint/burn).
	tokenContracts := buildTokenContractIndex(settings)

	// Process blockchain transactions (e.g. celo/CHT)
	processChainDir := func(chain string) {
		chainDir := etherscansource.Path(dataDir, year, month, chain)
		entries, err := os.ReadDir(chainDir)
		if err != nil {
			return
		}
		internalHashes := internalTransferHashesFromChainDir(chainDir)
		chainID := chainIDForSourceChain(settings, chain)

		// Load Nostr metadata: per-month snapshot first, then merge with the
		// timeless `latest/` registry so addresses labeled after this month
		// was synced still get a name.
		var nostrMeta NostrMetadataCache
		if chainID != 0 {
			monthCache := LoadNostrMetadataCache(nostrsource.ChainMetadataPath(dataDir, year, month, chainID))
			latestCache := LoadNostrMetadataCache(filepath.Join(dataDir, "latest", nostrsource.RelPath(strconv.Itoa(chainID), nostrsource.MetadataFile)))
			nostrMeta = MergeNostrMetadata(latestCache, monthCache)
		}

		// Dedup transfers that appear under more than one contract version for
		// the SAME account (the EURe V1->V2 upgrade lists some transfers on
		// both the legacy and the new contract). Keyed by account slug so an
		// internal transfer between two of our own wallets — same
		// hash/from/to/value in two different accounts' files — is preserved.
		seenAccountTransfer := map[string]bool{}

		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			data, err := os.ReadFile(filepath.Join(chainDir, e.Name()))
			if err != nil {
				continue
			}

			var txFile etherscansource.CacheFile
			if json.Unmarshal(data, &txFile) != nil || len(txFile.Transactions) == 0 {
				continue
			}

			accountAddr := txFile.Account
			tokenSymbol := txFile.Token

			// Derive account slug from filename: {slug}.{token}.json
			accountSlug := chain
			fname := e.Name()
			if idx := strings.Index(fname, "."); idx > 0 {
				accountSlug = fname[:idx]
			}
			if expectedAddr := accountAddressBySlug[strings.ToLower(accountSlug)]; expectedAddr != "" && accountAddr != "" && !strings.EqualFold(accountAddr, expectedAddr) {
				// Stale legacy cache from before the slug was repointed; ignore
				// instead of attributing the old wallet's transfers to this slug.
				continue
			}

			if tokenSymbol == "" {
				tokenSymbol = chain
			}

			tokenContract := tokenContracts[strings.ToLower(chain)+":"+strings.ToUpper(tokenSymbol)]

			// Per-(account, txHash) ordinal — etherscan returns transfers in
			// log order, so the first occurrence is logIndex=0, second is 1,
			// etc. The cache file is already scoped to one account+token, so
			// resetting per file is correct.
			txHashCounter := map[string]int{}

			for _, tx := range txFile.Transactions {
				// Drop transfers on the exclusion list (e.g. EURe V1->V2
				// migration mints, which re-issue the legacy-contract balance
				// already recorded via legacy transfers — counting both
				// double-counts). See settings/excluded-transactions.json.
				if isExcludedOnchainTx(chain, tx.Hash, tx.To) {
					continue
				}
				// Drop the second sighting of a transfer that the V1->V2 upgrade
				// lists under both contracts for this account.
				dupKey := accountSlug + "|" + strings.ToLower(tx.Hash) + "|" + strings.ToLower(tx.From) + "|" + strings.ToLower(tx.To) + "|" + tx.Value
				if seenAccountTransfer[dupKey] {
					continue
				}
				seenAccountTransfer[dupKey] = true
				dec := 18
				if tx.TokenDecimal != "" {
					fmt.Sscanf(tx.TokenDecimal, "%d", &dec)
				}

				amount := etherscansource.ParseTokenValue(tx.Value, dec)

				zeroAddr := "0x0000000000000000000000000000000000000000"
				fromZero := strings.EqualFold(tx.From, zeroAddr)
				toZero := strings.EqualFold(tx.To, zeroAddr)
				txType := "CREDIT"
				accountSide := tx.To // address of "our" side
				counterparty := tx.From
				if accountAddr != "" {
					// Wallet-specific tracking: outgoing = DEBIT, mints/burns
					// touching this wallet are still classified as MINT/BURN.
					accountSide = accountAddr
					if fromZero {
						txType = "MINT"
						counterparty = tx.From
					} else if toZero {
						txType = "BURN"
						counterparty = tx.To
					} else if strings.EqualFold(tx.From, accountAddr) {
						txType = "DEBIT"
						counterparty = tx.To
					}
				} else {
					// Token-wide tracking (e.g. CHT): classify by mint/burn/transfer
					if fromZero {
						txType = "MINT"
						accountSide = tx.To
						counterparty = tx.From
					} else if toZero {
						txType = "BURN"
						accountSide = tx.From
						counterparty = tx.To
					} else {
						// Token-wide transfer between two non-tracked
						// addresses. From the sender's perspective this
						// is just a DEBIT — no separate type needed.
						txType = "DEBIT"
						accountSide = tx.From
						counterparty = tx.To
					}
				}

				// Detect internal transfers: both from and to are tracked accounts
				_, fromTracked := trackedAddresses[strings.ToLower(tx.From)]
				_, toTracked := trackedAddresses[strings.ToLower(tx.To)]
				isInternal := fromTracked && toTracked
				if internalHashes[strings.ToLower(tx.Hash)] {
					isInternal = true
				}

				// EURb accounts (fridge, coffee) are like Stripe: withdrawals are internal
				if txType == "DEBIT" && strings.EqualFold(tokenSymbol, "EURb") {
					isInternal = true
				}

				internalDirection := ""
				if isInternal {
					internalDirection = txType
					// Keep one row per tracked account side. The tx hash is the
					// same, but the account-relative direction differs and is
					// needed for per-account balances.
					txType = "INTERNAL"
				}

				ts := int64(0)
				fmt.Sscanf(tx.TimeStamp, "%d", &ts)

				chainStr := chain
				// counterpartyURI: when the other side is the zero address, the
				// canonical counterparty is the token contract (otherwise EURb
				// vs EURe vs CHT mints all collide on 0x0).
				var counterpartyURI string
				if strings.EqualFold(counterparty, zeroAddr) && tokenContract != "" {
					counterpartyURI = BuildBlockchainTokenURI(chainID, chain, tokenContract)
				} else {
					counterpartyURI = BuildBlockchainAddressURI(chainID, chain, counterparty)
				}
				hashKey := strings.ToLower(tx.Hash)
				logIndex := txHashCounter[hashKey]
				txHashCounter[hashKey]++

				// Schema convention: Amount is signed gross from the
				// perspective of accountId (positive = money INTO the
				// account, negative = money OUT). GrossAmount stays as
				// the positive magnitude. For etherscan there's no fee,
				// so NetAmount = NormalizedAmount = Amount.
				signedAmount := amount
				switch txType {
				case "DEBIT", "BURN":
					signedAmount = -amount
				case "INTERNAL":
					if internalDirection == "DEBIT" {
						signedAmount = -amount
					}
				}

				entry := TransactionEntry{
					ID:               BuildBlockchainURI(chainID, tx.Hash),
					ProviderID:       tx.Hash,
					AccountID:        BuildBlockchainAddressURI(chainID, chain, accountSide),
					CounterpartyID:   counterpartyURI,
					TxHash:           tx.Hash,
					LogIndex:         logIndex,
					Provider:         "etherscan",
					Chain:            &chainStr,
					Account:          accountAddr,
					AccountSlug:      accountSlug,
					AccountName:      fmt.Sprintf("⛓️ %s %s", strings.Title(chain), tokenSymbol),
					Currency:         tokenSymbol,
					Value:            fmt.Sprintf("%.6f", signedAmount),
					Amount:           signedAmount,
					NetAmount:        signedAmount,
					GrossAmount:      amount,
					NormalizedAmount: signedAmount,
					Fee:              0,
					Type:             txType,
					Counterparty:     counterparty,
					Timestamp:        ts,
				}
				if internalDirection != "" {
					entry.Metadata = map[string]interface{}{
						"direction": internalDirection,
					}
				}

				// Enrich with Nostr metadata
				if nostrMeta.Transactions != nil {
					if txMeta, ok := nostrMeta.Transactions[strings.ToLower(tx.Hash)]; ok {
						if entry.Metadata == nil {
							entry.Metadata = map[string]interface{}{}
						}
						if txMeta.Description != "" {
							entry.Metadata["description"] = txMeta.Description
						}
						for k, v := range txMeta.Tags {
							entry.Metadata[k] = v
						}
						if len(txMeta.TagList) > 0 {
							entry.Tags = append(entry.Tags, txMeta.TagList...)
						}
					}
				}
				if nostrMeta.Addresses != nil {
					if addrMeta, ok := nostrMeta.Addresses[strings.ToLower(counterparty)]; ok {
						if addrMeta.Name != "" {
							entry.Counterparty = addrMeta.Name
						}
					}
				}

				// Token-wide tracking (account=="") encodes both sides via
				// the schema's standard fields:
				//   accountId / Account        = sender (the perspective)
				//   counterpartyId / Counterparty = receiver
				// Names: when we know who an address belongs to (via Nostr
				// address metadata), overwrite AccountName with the sender
				// label and Counterparty with the receiver label. The raw
				// addresses are still recoverable from accountId / counterpartyId.
				if accountAddr == "" && nostrMeta.Addresses != nil {
					fromAddr := strings.ToLower(tx.From)
					toAddr := strings.ToLower(tx.To)
					if fromAddr != "" && fromAddr != zeroAddr {
						if m, ok := nostrMeta.Addresses[fromAddr]; ok && m.Name != "" {
							entry.AccountName = m.Name
						}
					}
					if toAddr != "" && toAddr != zeroAddr {
						if m, ok := nostrMeta.Addresses[toAddr]; ok && m.Name != "" {
							entry.Counterparty = m.Name
						}
					}
				}

				transactions = append(transactions, entry)
			}
		}
	}

	// Check for known chain directories
	for _, chain := range []string{"celo", "gnosis", "ethereum", "polygon"} {
		processChainDir(chain)
	}

	// Process KBC Brussels CSV rows. The provider has no API — the
	// operator drops one CSV in latest/providers/kbcbrussels/ and the
	// loader filters by booking month. `year == "latest"` is the
	// rebuild-everything mode used by `chb generate latest`.
	if kbcEnabled {
		for iban, acc := range kbcAccounts {
			// Odoo source of truth: pull the month's txs from the journal-lines
			// cache, not the bootstrap CSV (which is ignored entirely).
			if acc.IsOdooSourceOfTruth() {
				a := acc
				for _, entry := range kbcTransactionsFromOdoo(&a) {
					if year != "latest" && !kbcEntryInYearMonth(entry, year, month) {
						continue
					}
					transactions = append(transactions, entry)
				}
				continue
			}
			var rows []kbcbrusselssource.Transaction
			var err error
			if year == "latest" && month == "" {
				rows, err = kbcbrusselssource.LoadTransactionsForIBAN(dataDir, iban)
			} else {
				rows, err = kbcbrusselssource.LoadTransactionsForMonth(dataDir, iban, year, month)
			}
			if err != nil {
				fmt.Printf("    %s⚠ kbcbrussels %s: %v%s\n", Fmt.Yellow, iban, err, Fmt.Reset)
				continue
			}
			for _, row := range rows {
				entry := kbcRowToTransactionEntry(acc, row)
				transactions = append(transactions, entry)
			}
		}
	}

	if len(transactions) == 0 {
		return 0
	}

	// Load Nostr annotations (highest priority for categorization)
	nostrAnnotations := map[string]*TxAnnotation{}
	// Stripe annotations
	stripeAnnotPath := nostrsource.Path(dataDir, year, month, nostrsource.StripeAnnotationsFile)
	if data, err := os.ReadFile(stripeAnnotPath); err == nil {
		var cache NostrAnnotationCache
		if json.Unmarshal(data, &cache) == nil {
			for k, v := range cache.Annotations {
				nostrAnnotations[k] = v
			}
		}
	}
	annotationsPath := nostrsource.Path(dataDir, year, month, nostrsource.AnnotationsFile)
	if data, err := os.ReadFile(annotationsPath); err == nil {
		var cache NostrAnnotationCache
		if json.Unmarshal(data, &cache) == nil {
			for k, v := range cache.Annotations {
				nostrAnnotations[k] = v
			}
		}
	}
	// Blockchain annotations from Nostr source metadata are applied while building chain transactions.
	// These are already applied during transaction building above via TxMetadata.Tags

	// Load Odoo enrichment (second priority)
	odooCategories := map[string]string{} // stripeRef -> category
	odooPath := odoosource.Path(dataDir, year, month, odoosource.AnalyticEnrichmentFile)
	if data, err := os.ReadFile(odooPath); err == nil {
		var odooEnrich struct {
			Mappings []struct {
				StripeReference string `json:"stripeReference"`
				Category        string `json:"category"`
			} `json:"mappings"`
		}
		if json.Unmarshal(data, &odooEnrich) == nil {
			for _, m := range odooEnrich.Mappings {
				odooCategories[m.StripeReference] = m.Category
			}
		}
	}

	// Load CSV enrichment (imported spreadsheet)
	csvEnrichment := LoadStripeCSVEnrichment()

	// Apply enrichment priority chain: Nostr > CSV > Odoo > Local rules
	if settings != nil {
		for i := range transactions {
			tx := &transactions[i]

			// Build URI for Nostr lookup
			var uri string
			if tx.Provider == "stripe" && tx.StripeChargeID != "" {
				uri = BuildStripeURI(tx.StripeChargeID)
			} else if tx.Provider == "etherscan" && tx.TxHash != "" {
				// Check if blockchain tx already has category from Nostr tags (set during enrichment above)
				if cat, ok := tx.Metadata["category"]; ok {
					if catStr, ok := cat.(string); ok && catStr != "" && tx.Category == "" {
						tx.Category = catStr
					}
				}
				if col, ok := tx.Metadata["collective"]; ok {
					if colStr, ok := col.(string); ok && colStr != "" && tx.Collective == "" {
						tx.Collective = colStr
					}
				}
				// Also build the URI so blockchain txs get their accounting
				// annotations (category/collective/event/spread) applied below.
				chainID := 0
				if tx.Chain != nil {
					chainID = chainIDForSourceChain(settings, *tx.Chain)
				}
				if chainID > 0 {
					uri = BuildBlockchainURI(chainID, tx.TxHash)
				}
			}

			// Fall back to the transaction's own NIP-73 id for providers
			// whose URI isn't rebuilt above (e.g. iban: bank txs). tx.ID is
			// already the canonical URI used as the Nostr annotation key, so
			// this lets annotations apply to every provider, not just
			// Stripe/Etherscan.
			if uri == "" {
				uri = tx.ID
			}

			// 1. Nostr annotations (highest priority)
			if uri != "" {
				if ann, ok := nostrAnnotations[uri]; ok {
					if ann.Category != "" {
						tx.Category = ann.Category
					}
					if ann.Collective != "" {
						tx.Collective = ann.Collective
					}
					if ann.Event != "" {
						tx.Event = ann.Event
					}
					if len(ann.Tags) > 0 {
						tx.Tags = append(tx.Tags, ann.Tags...)
					}
					if len(ann.Spread) > 0 {
						tx.Spread = ann.Spread
					}
				}
			}

			// 1b. Auto-assign collective from Stripe metadata
			if tx.Collective == "" {
				// From payment link metadata: collective = "openletter"
				if col, ok := tx.Metadata["collective"]; ok {
					if colStr, ok := col.(string); ok && colStr != "" {
						tx.Collective = colStr
					}
				}
			}
			if tx.Collective == "" {
				// From Open Collective: metadata.to = "https://opencollective.com/openletter"
				if to, ok := tx.Metadata["to"]; ok {
					if toStr, ok := to.(string); ok && strings.Contains(toStr, "opencollective.com/") {
						parts := strings.Split(toStr, "opencollective.com/")
						if len(parts) == 2 {
							slug := strings.TrimRight(parts[1], "/")
							if slug != "" {
								tx.Collective = slug
							}
						}
					}
				}
			}

			// 1c. Auto-assign event from Stripe/Nostr metadata
			if tx.Event == "" {
				// Luma: event_api_id is the same UID as in events.json
				if evtID, ok := tx.Metadata["event_api_id"]; ok {
					if evtStr, ok := evtID.(string); ok && evtStr != "" {
						tx.Event = evtStr
					}
				}
				if tx.Event == "" {
					if evtID, ok := tx.Metadata["eventId"]; ok {
						if evtStr, ok := evtID.(string); ok && evtStr != "" {
							tx.Event = evtStr
						}
					}
				}
			}

			// 2. CSV enrichment (from imported spreadsheet)
			if csvEnrichment != nil && tx.Provider == "stripe" {
				if entry, ok := csvEnrichment.Entries[tx.TxHash]; ok {
					if entry.Category != "" && tx.Category == "" {
						tx.Category = entry.Category
					}
					if entry.Collective != "" && tx.Collective == "" {
						tx.Collective = entry.Collective
					}
					if entry.Product != "" {
						if tx.Metadata == nil {
							tx.Metadata = map[string]interface{}{}
						}
						tx.Metadata["product"] = entry.Product
					}
				}
			}

			// 3. Odoo enrichment (third priority, only if not set by above)
			if tx.Category == "" && tx.StripeChargeID != "" {
				if cat, ok := odooCategories[tx.StripeChargeID]; ok {
					tx.Category = cat
				}
			}

			// Local rules run after processors below so that plugin-enriched
			// metadata (Monerium memo, Luma event info, …) is available to
			// the rule matcher.
		}
	}

	runTransactionProcessors(dataDir, year, month, transactions)

	// 4. Local rules (lowest priority for category/collective/event,
	// always-applied for type/description). Runs after processors so memo
	// and IBAN added by plugins (Monerium) are visible to the matcher.
	if settings != nil {
		categorizer := NewCategorizer(settings)
		for i := range transactions {
			categorizer.Apply(&transactions[i])
		}
	}

	// 4b. Canonicalise collective slugs. Upstream sources sometimes use
	// alternate spellings (Open Collective uses "commonshub-brussels" for
	// what our chart of accounts calls "commonshub"); collectives.json
	// declares the aliases. Running this here means downstream — Odoo
	// mapping lookups, the breakdown view, the analytic-plan accounts — all
	// see one canonical slug.
	for i := range transactions {
		if c := CanonicalCollectiveSlug(transactions[i].Collective); c != transactions[i].Collective {
			transactions[i].Collective = c
		}
	}

	// 5. Odoo mapping — resolves the lookup-driven account_code and
	// partner_id per tx using the (now-categorised) category/collective.
	// Persisting these onto transactions.json means push/merge can
	// trust the local file and never has to call LookupOdooMapping itself
	// (so changing rules.json or odoo_mapping.json requires re-running
	// `chb generate`, which is the right contract per CLAUDE.md).
	if odooMappings, err := LoadOdooMappings(); err == nil && len(odooMappings) > 0 {
		for i := range transactions {
			applyOdooMapping(odooMappings, &transactions[i])
		}
	}

	// Category and collective live only in metadata in the public JSON,
	// so mirror the struct fields into the metadata map (or drop the keys
	// when no value was assigned). AccountCode / PartnerID are NOT
	// projected into transactions.json — they get written separately to
	// providers/odoo/pending/<YYYY-MM>.json by writeOdooPendingFromTransactions
	// below, keeping transactions.json target-agnostic.
	for i := range transactions {
		tx := &transactions[i]
		syncMetadataString(tx, "category", tx.Category)
		syncMetadataString(tx, "collective", tx.Collective)
		// Strip any legacy accountCode/partnerId metadata so re-running
		// generate cleanly migrates older files into the new shape.
		if tx.Metadata != nil {
			delete(tx.Metadata, "accountCode")
			delete(tx.Metadata, "partnerId")
		}
	}

	for i := range transactions {
		syncTransactionTags(&transactions[i])
	}

	// Strip the customer name from Stripe public descriptions, replacing
	// it with a synthesized "ticket <event>" / "<category> <collective>"
	// summary when those tags are known. The customer name is preserved
	// in the PII file (loaded with --with-pii) so detail views can still
	// show it on demand.
	eventsByID := loadEventsByID(dataDir)
	for i := range transactions {
		tx := &transactions[i]
		if tx.Provider != "stripe" {
			continue
		}
		desc := buildPublicStripeDescription(tx)
		// Fallback only: if the Stripe description didn't give us an event name
		// (desc is the bare "ticket" label), resolve it from the events registry
		// by the tx's event UID.
		if strings.EqualFold(desc, "ticket") && tx.Event != "" {
			if ev, ok := eventsByID[bareEventID(tx.Event)]; ok && strings.TrimSpace(ev.Name) != "" {
				desc = ev.Name
			}
		}
		if tx.Metadata == nil {
			tx.Metadata = map[string]interface{}{}
		}
		if desc == "" {
			delete(tx.Metadata, "description")
		} else {
			tx.Metadata["description"] = desc
		}
		// Drop event-id duplicates (the UID lives once in tx.Event) and
		// low-value provider internals so ticket metadata stays clean.
		for _, k := range stripeRedundantEventMetaKeys {
			delete(tx.Metadata, k)
		}
	}

	// Sort by timestamp
	sort.Slice(transactions, func(i, j int) bool {
		return transactions[i].Timestamp < transactions[j].Timestamp
	})

	// Drop transactions explicitly flagged ["t", "ignore"] via a Nostr
	// annotation — the operator marked them as not-real-business-activity
	// (test charges, mistaken transfers, refunds whose pair we kept,
	// etc.) and they should not appear in the public transactions feed
	// or in any downstream aggregate (counterparties.json, summaries,
	// reports, etc.).
	ignoredCount := 0
	kept := transactions[:0]
	for _, tx := range transactions {
		if transactionHasTag(tx, []string{"t", "ignore"}) {
			ignoredCount++
			continue
		}
		kept = append(kept, tx)
	}
	transactions = kept
	if ignoredCount > 0 {
		fmt.Printf("    %s↷ skipped %s tagged #ignore%s\n", Fmt.Dim, Pluralize(ignoredCount, "tx", ""), Fmt.Reset)
	}

	// Split PII from public transactions
	piiFile := TransactionsPIIFile{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Enrichments: map[string]*TransactionPII{},
	}

	publicTxs := make([]TransactionEntry, len(transactions))
	for i, tx := range transactions {
		publicTxs[i] = tx
		// Drop internal-only fields from public output. Canonical handles
		// (id/providerId/accountId/counterpartyId) replace them.
		publicTxs[i].TxHash = ""
		publicTxs[i].Account = ""
		publicTxs[i].Counterparty = ""
		publicTxs[i].StripeChargeID = ""

		// Extract PII: customer name (for Stripe) and email.
		var piiName, piiEmail, piiIBAN string
		if email, ok := tx.Metadata["email"].(string); ok && email != "" {
			piiEmail = email
		}
		if iban, ok := tx.Metadata["iban"].(string); ok {
			if normalized := normalizeIBAN(iban); normalized != "" {
				piiIBAN = normalized
				tx.Metadata["iban"] = normalized
			}
		}

		// For Stripe/Monerium, counterparty may be a person's name (PII).
		// Blockchain 0x addresses are public, not PII. Monerium runs on
		// etherscan but its plugin adds a `source:monerium` tag we can match.
		isMonerium := transactionHasTag(tx, []string{"source", "monerium"})
		if tx.Provider == "stripe" || isMonerium {
			if tx.Counterparty != "" && !strings.HasPrefix(tx.Counterparty, "0x") {
				piiName = tx.Counterparty
			}
			// Drop the display counterparty entirely — counterpartyId is the
			// canonical handle and counterparties.json owns any safe metadata.
			publicTxs[i].Counterparty = ""
		}

		// Strip ANY email-looking value from public metadata. Merchant-side
		// metadata often carries emails under keys like `stripe_email`,
		// `stripe_receipt_email`, `customer_email` etc., so we can't just
		// drop one well-known key — we detect by pattern and preserve
		// the first one we see in the PII file.
		if len(tx.Metadata) > 0 {
			publicMeta := make(map[string]interface{}, len(tx.Metadata))
			for k, v := range tx.Metadata {
				if s, ok := v.(string); ok && containsEmail(s) {
					if piiEmail == "" {
						piiEmail = s
					}
					continue
				}
				publicMeta[k] = v
			}
			delete(publicMeta, "email") // legacy key, always redundant
			delete(publicMeta, "iban")  // PII — kept only in private/enrichment.json
			publicTxs[i].Metadata = publicMeta
		}

		if piiName != "" || piiEmail != "" || piiIBAN != "" {
			piiFile.Enrichments[tx.ID] = &TransactionPII{
				Name:  piiName,
				Email: piiEmail,
				IBAN:  piiIBAN,
			}
		}
	}

	out := TransactionsFile{
		Year:         year,
		Month:        month,
		GeneratedAt:  time.Now().UTC().Format(time.RFC3339),
		Transactions: publicTxs,
	}

	txData, _ := json.MarshalIndent(out, "", "  ")
	writeMonthFile(dataDir, year, month, filepath.Join("generated", "transactions.json"), txData)

	// Write Odoo-specific resolution out to providers/odoo/pending/. The
	// public transactions.json is now target-agnostic; push paths look up
	// AccountCode/PartnerID from pending instead.
	pendingEntries := map[string]OdooPendingEntry{}
	for _, tx := range transactions {
		if tx.ID == "" {
			continue
		}
		if tx.AccountCode == "" && tx.PartnerID == 0 {
			continue
		}
		pendingEntries[tx.ID] = OdooPendingEntry{
			TxURI:       tx.ID,
			AccountCode: tx.AccountCode,
			PartnerID:   tx.PartnerID,
			Category:    tx.Category,
			Collective:  tx.Collective,
		}
	}
	if err := WriteOdooPending(dataDir, year, month, pendingEntries); err != nil {
		Warnf("  %s⚠ Could not write Odoo pending file for %s-%s: %v%s", Fmt.Yellow, year, month, err, Fmt.Reset)
	}

	// Save PII enrichment to private directory
	if len(piiFile.Enrichments) > 0 {
		piiData, _ := json.MarshalIndent(piiFile, "", "  ")
		piiRelPath := filepath.Join("generated", "private", "enrichment.json")
		_ = writeDataFile(filepath.Join(dataDir, year, month, piiRelPath), piiData)
		// Also write to latest
		_ = writeDataFile(filepath.Join(dataDir, "latest", piiRelPath), piiData)
	}

	return len(transactions)
}

func internalTransferHashesFromChainDir(chainDir string) map[string]bool {
	type seenAccount struct {
		account string
		count   int
	}
	seen := map[string]map[string]bool{}
	entries, err := os.ReadDir(chainDir)
	if err != nil {
		return map[string]bool{}
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(chainDir, e.Name()))
		if err != nil {
			continue
		}
		var txFile etherscansource.CacheFile
		if json.Unmarshal(data, &txFile) != nil || txFile.Account == "" {
			continue
		}
		account := strings.ToLower(txFile.Account)
		for _, tx := range txFile.Transactions {
			hash := strings.ToLower(tx.Hash)
			if hash == "" {
				continue
			}
			if seen[hash] == nil {
				seen[hash] = map[string]bool{}
			}
			seen[hash][account] = true
		}
	}
	internal := map[string]bool{}
	for hash, accounts := range seen {
		if len(accounts) > 1 {
			internal[hash] = true
		}
	}
	return internal
}

// foldStripeMetadataValue writes a single Stripe metadata pair into the
// canonical tx metadata map. `name`/`display_name`/`displayName` are filtered
// through safeFirstName because anything labelled "name" risks carrying full
// names or emails. Other keys pass through unprefixed; existing keys (our own
// derived values) take precedence so a merchant can't overwrite them.
// buildPublicStripeDescription returns a clean, PII-free description for
// a Stripe transaction. Preferred forms, in order:
//   - "ticket <eventName>" when an event is associated (or just "ticket"
//     if the name is not yet resolved)
//   - "<category> <collective>" when both are known (e.g. "donation
//     openletter", "membership commonshub")
//   - "<category>" or "<collective>" when only one is set
//
// As a last resort, the original Stripe description is kept but with the
// customer name (tx.Counterparty, still present at this stage) scrubbed
// so it never reaches the public file.
func buildPublicStripeDescription(tx *TransactionEntry) string {
	eventName := firstTransactionTagValue(*tx, "eventName")
	eventID := firstTransactionTagValue(*tx, "event")
	if eventID == "" && tx.Event != "" {
		eventID = tx.Event
	}
	if eventID != "" || eventName != "" {
		// The Stripe charge description IS the event name (Luma sets it) — use it
		// directly. It's more resilient than depending on the events registry,
		// and the category already conveys "ticket". Fall back to a known event
		// name, then the registry (in the generate pass), then "ticket".
		if desc := strings.TrimSpace(stringMetadata(tx.Metadata, "description")); desc != "" && !strings.EqualFold(desc, "ticket") {
			return scrubCustomerNameFromDescription(desc, tx.Counterparty)
		}
		if eventName != "" {
			return eventName
		}
		return "ticket"
	}

	// Stripe fees (processing fees, Automatic Tax, Usage Fee, and fee credits
	// like "€10,000 free Credit") carry their meaning in the Stripe description,
	// not a "<category> <collective>" summary — keep the original text.
	if strings.EqualFold(stringMetadata(tx.Metadata, "kind"), "fee") || strings.EqualFold(tx.Category, "stripe_fee") {
		if desc := stringMetadata(tx.Metadata, "description"); desc != "" {
			return scrubCustomerNameFromDescription(desc, tx.Counterparty)
		}
	}

	cat := tx.Category
	coll := tx.Collective
	switch {
	case cat != "" && coll != "":
		return cat + " " + coll
	case cat != "":
		return cat
	case coll != "":
		return coll
	}

	desc := stringMetadata(tx.Metadata, "description")
	if desc == "" {
		return ""
	}
	return scrubCustomerNameFromDescription(desc, tx.Counterparty)
}

// scrubCustomerNameFromDescription removes a known customer-name token
// (and the common Stripe connectors that precede it) from a free-text
// description. Returns the trimmed result; never panics on empty input.
func scrubCustomerNameFromDescription(desc, name string) string {
	name = strings.TrimSpace(name)
	if name == "" || desc == "" {
		return desc
	}
	// Stripe-style "Donation by NAME", "Payment from NAME", "for NAME",
	// "- NAME". The name itself is also stripped if it appears bare.
	for _, prefix := range []string{" by ", " from ", " for ", " - ", ", "} {
		if i := strings.Index(strings.ToLower(desc), strings.ToLower(prefix+name)); i >= 0 {
			desc = desc[:i] + desc[i+len(prefix)+len(name):]
		}
	}
	if i := strings.Index(strings.ToLower(desc), strings.ToLower(name)); i >= 0 {
		desc = desc[:i] + desc[i+len(name):]
	}
	return strings.TrimSpace(strings.Trim(strings.TrimSpace(desc), "-,;:"))
}

// stripeRedundantEventMetaKeys are the per-transaction metadata keys that
// duplicate the canonical tx.Event UID, or are low-value provider internals
// (e.g. Luma payment-session ids). They're stripped once tx.Event is resolved,
// so a ticket transaction carries the event UID once (in tx.Event) and the
// event's name/URL stay in the events registry — not on every ticket.
var stripeRedundantEventMetaKeys = []string{
	"eventId", "event_api_id", "event_id", "eventApiId",
	"luma_payment_started_api_id", "payment_type",
}

func foldStripeMetadataValue(metadata map[string]interface{}, k, v string) {
	if v == "" {
		return
	}
	switch k {
	case "name", "display_name", "displayName":
		safe := safeFirstName(v)
		if safe == "" {
			return
		}
		if _, exists := metadata["name"]; !exists {
			metadata["name"] = safe
		}
	default:
		if _, exists := metadata[k]; !exists {
			metadata[k] = v
		}
	}
}

// safeFirstName returns a privacy-safe first token from a free-text input.
// Returns "" when the value looks like an email or exceeds a sane length.
func safeFirstName(s string) string {
	s = strings.TrimSpace(s)
	if s == "" || strings.Contains(s, "@") {
		return ""
	}
	parts := strings.Fields(s)
	if len(parts) == 0 {
		return ""
	}
	first := parts[0]
	if strings.Contains(first, "@") || len(first) > 30 {
		return ""
	}
	return first
}

// buildAccountURIIndex returns a NIP-73 URI → FinanceAccount map for our own
// tracked accounts. Used so counterparties.json can show the account name /
// slug whenever a tx references one of them (either as accountId, or as the
// counterpartyId of an inverse-direction tx).
func buildAccountURIIndex(settings *Settings) map[string]FinanceAccount {
	out := map[string]FinanceAccount{}
	if settings == nil {
		return out
	}
	for _, acc := range settings.Finance.Accounts {
		var uri string
		switch strings.ToLower(acc.Provider) {
		case "stripe":
			uri = BuildStripeAccountURI(acc.AccountID)
		case "etherscan":
			if acc.Address != "" {
				uri = BuildBlockchainAddressURI(acc.ChainID, acc.Chain, acc.Address)
			}
		}
		if uri != "" {
			out[uri] = acc
		}
	}
	return out
}

// buildTokenContractIndex returns a (lowercase chain) + (uppercase symbol) →
// contract-address map drawn from FinanceAccount.Token and tokens.json. Used
// to resolve mint/burn counterparties (the zero address) to the token's
// contract.
func buildTokenContractIndex(settings *Settings) map[string]string {
	out := map[string]string{}
	if settings != nil {
		for _, acc := range settings.Finance.Accounts {
			if acc.Token == nil || acc.Token.Address == "" || acc.Chain == "" || acc.Token.Symbol == "" {
				continue
			}
			key := strings.ToLower(acc.Chain) + ":" + strings.ToUpper(acc.Token.Symbol)
			if _, exists := out[key]; !exists {
				out[key] = strings.ToLower(acc.Token.Address)
			}
		}
		if t := settings.ContributionToken; t != nil && t.Address != "" && t.Chain != "" && t.Symbol != "" {
			key := strings.ToLower(t.Chain) + ":" + strings.ToUpper(t.Symbol)
			if _, exists := out[key]; !exists {
				out[key] = strings.ToLower(t.Address)
			}
		}
	}
	for _, t := range LoadTokenConfigs() {
		if t.Address == "" || t.Chain == "" || t.Symbol == "" {
			continue
		}
		key := strings.ToLower(t.Chain) + ":" + strings.ToUpper(t.Symbol)
		if _, exists := out[key]; !exists {
			out[key] = strings.ToLower(t.Address)
		}
	}
	return out
}

func chainIDForSourceChain(settings *Settings, chain string) int {
	if settings == nil {
		return 0
	}
	for _, acc := range settings.Finance.Accounts {
		if strings.EqualFold(acc.Chain, chain) && acc.ChainID != 0 {
			return acc.ChainID
		}
	}
	if settings.ContributionToken != nil && strings.EqualFold(settings.ContributionToken.Chain, chain) {
		return settings.ContributionToken.ChainID
	}
	return 0
}

// ── Counterparties ──────────────────────────────────────────────────────────

func generateCounterpartiesGo(dataDir, year, month string) int {
	txPath := filepath.Join(dataDir, year, month, "generated", "transactions.json")
	data, err := os.ReadFile(txPath)
	if err != nil {
		return 0
	}

	var txFile TransactionsFile
	if json.Unmarshal(data, &txFile) != nil || len(txFile.Transactions) == 0 {
		return 0
	}

	settings, _ := LoadSettings()
	chainCaches := map[int]NostrMetadataCache{}
	accountsByURI := buildAccountURIIndex(settings)

	counterparties := map[string]CounterpartyEntry{}
	for _, tx := range txFile.Transactions {
		id, addr, fallbackName, chainID := counterpartyIdentity(tx, settings)
		if id == "" {
			continue
		}
		if _, ok := counterparties[id]; ok {
			continue
		}

		entry := CounterpartyEntry{Name: fallbackName}
		if chainID > 0 && addr != "" {
			cache, ok := chainCaches[chainID]
			if !ok {
				monthCache := LoadNostrMetadataCache(nostrsource.ChainMetadataPath(dataDir, year, month, chainID))
				latestCache := LoadNostrMetadataCache(filepath.Join(dataDir, "latest", nostrsource.RelPath(strconv.Itoa(chainID), nostrsource.MetadataFile)))
				cache = MergeNostrMetadata(latestCache, monthCache)
				chainCaches[chainID] = cache
			}
			if md, ok := cache.Addresses[addr]; ok && md != nil {
				if md.Name != "" {
					entry.Name = md.Name
				}
				entry.About = md.About
				entry.Picture = md.Picture
				if len(md.Tags) > 0 {
					entry.Tags = map[string]string{}
					for k, v := range md.Tags {
						entry.Tags[k] = v
					}
				}
				entry.NostrEventID = md.NostrEventID
				entry.Author = md.Author
				entry.CreatedAt = md.CreatedAt
			}
		}

		counterparties[id] = entry
	}

	// Layer in our own tracked accounts. They show up as both accountId and
	// counterpartyId across transactions (when funds move between us and
	// someone else), and the settings entry has the canonical name + slug.
	for _, tx := range txFile.Transactions {
		for _, uri := range []string{tx.AccountID, tx.CounterpartyID} {
			if uri == "" {
				continue
			}
			acc, ok := accountsByURI[uri]
			if !ok {
				continue
			}
			entry := counterparties[uri]
			entry.Name = acc.Name
			entry.Slug = acc.Slug
			counterparties[uri] = entry
		}
	}

	if len(counterparties) == 0 {
		return 0
	}

	out := CounterpartiesFile{
		Month:          fmt.Sprintf("%s-%s", year, month),
		GeneratedAt:    time.Now().UTC().Format(time.RFC3339),
		Counterparties: counterparties,
	}

	cpData, _ := json.MarshalIndent(out, "", "  ")
	writeMonthFile(dataDir, year, month, filepath.Join("generated", "counterparties.json"), cpData)
	return len(counterparties)
}

// counterpartyIdentity returns the NIP-73 URI for a tx counterparty, plus the
// raw address (lowercased, empty for non-blockchain), a fallback display name,
// and the chain ID when known. Returns an empty id when the tx has no usable
// counterparty.
func counterpartyIdentity(tx TransactionEntry, settings *Settings) (id, address, fallbackName string, chainID int) {
	display := strings.TrimSpace(tx.Counterparty)

	switch tx.Provider {
	case "etherscan":
		addr := stringMetadata(tx.Metadata, "counterpartyAddress")
		if addr == "" {
			if a := stringMetadata(tx.Metadata, "from"); a != "" {
				addr = a
			} else if a := stringMetadata(tx.Metadata, "to"); a != "" {
				addr = a
			} else if strings.HasPrefix(display, "0x") {
				addr = display
			}
		}
		if addr == "" {
			return "", "", "", 0
		}
		addr = strings.ToLower(addr)
		chain := ""
		if tx.Chain != nil {
			chain = *tx.Chain
		}
		cid := chainIDForSourceChain(settings, chain)
		name := display
		if strings.EqualFold(name, addr) {
			name = ""
		}
		return BuildBlockchainAddressURI(cid, chain, addr), addr, name, cid

	case "stripe":
		if tx.StripeCustomerID == "" {
			// Guest checkouts / one-off payments have no Stripe customer
			// object — there's no canonical id to dedup against, so skip.
			return "", "", "", 0
		}
		// We only carry a name when the Stripe metadata explicitly contained
		// `name` or `display_name` (already filtered through safeFirstName
		// at metadata-fold time, sitting in metadata["name"]). Anything else
		// — billing names, description fallbacks — is treated as PII.
		name := stringMetadata(tx.Metadata, "name")
		return BuildStripeCustomerURI(tx.StripeCustomerID), "", name, 0

	default:
		if display == "" {
			return "", "", "", 0
		}
		provider := strings.ToLower(strings.TrimSpace(tx.Provider))
		if provider == "" {
			return "text:" + display, "", display, 0
		}
		return provider + ":counterparty:" + display, "", display, 0
	}
}

// ── Latest events generation ────────────────────────────────────────────────

// LatestEvent is a simplified event for the website
type LatestEvent struct {
	ID              string          `json:"id"`
	Name            string          `json:"name"`
	Description     string          `json:"description,omitempty"`
	StartAt         string          `json:"startAt"`
	EndAt           string          `json:"endAt,omitempty"`
	URL             string          `json:"url,omitempty"`
	CoverImage      string          `json:"coverImage,omitempty"`
	CoverImageLocal string          `json:"coverImageLocal,omitempty"`
	Tags            json.RawMessage `json:"tags,omitempty"`
	Location        string          `json:"location,omitempty"`
}

type LatestEventsFile struct {
	GeneratedAt string        `json:"generatedAt"`
	Count       int           `json:"count"`
	Events      []LatestEvent `json:"events"`
}

func generateLatestEventsGo(dataDir string) {
	now := time.Now()
	var upcoming []LatestEvent
	for _, ev := range loadUpcomingFullEvents(dataDir) {
		upcoming = append(upcoming, LatestEvent{
			ID:              ev.ID,
			Name:            ev.Name,
			Description:     ev.Description,
			StartAt:         ev.StartAt,
			EndAt:           ev.EndAt,
			URL:             ev.URL,
			CoverImage:      ev.CoverImage,
			CoverImageLocal: ev.CoverImageLocal,
			Tags:            ev.Tags,
			Location:        ev.Location,
		})
	}

	outputPath := filepath.Join(dataDir, "latest", "generated", "events.json")
	out := LatestEventsFile{
		GeneratedAt: now.UTC().Format(time.RFC3339),
		Count:       len(upcoming),
		Events:      upcoming,
	}
	writeJSONFile(outputPath, out)
	fmt.Printf("  ✓ latest/events.json: %s\n", Pluralize(len(upcoming), "upcoming event", ""))
}

// ── Generated README ────────────────────────────────────────────────────────

func writeGeneratedReadme(dataDir string) {
	readme := `# latest/generated/

Files in this folder are produced by ` + "`chb generate`" + `.
They are derived from raw synced data and can be regenerated at any time.

**Do not edit manually** — they will be overwritten on the next run.

## Files

| File | Description |
|------|-------------|
| events.json | Merged events from ICS + Luma API |
| transactions.json | Aggregated transactions (gnosis, stripe, monerium) |
| contributors.json | Monthly contributor stats (tokens, messages) |
| counterparties.json | Unique counterparties from transactions |
| members.json | Membership snapshot (Stripe + Odoo) |
| images.json | Images extracted from Discord messages |
`
	readmePath := filepath.Join(dataDir, "latest", "generated", "README.md")
	_ = writeDataFile(readmePath, []byte(readme))
}

// ── Helpers ─────────────────────────────────────────────────────────────────

func writeJSONFile(path string, v interface{}) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return writeDataFile(path, data)
}

func marshalIndentedNoHTMLEscape(v interface{}) ([]byte, error) {
	var buf strings.Builder
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return []byte(strings.TrimSuffix(buf.String(), "\n")), nil
}

func relativeDiscordImagePathFromTimestamp(timestamp, attachmentID, ext string) string {
	t, err := time.Parse(time.RFC3339Nano, timestamp)
	if err != nil {
		t, err = time.Parse("2006-01-02T15:04:05+00:00", timestamp)
	}
	if err != nil {
		return filepath.ToSlash(discordsource.ImageRelPath(attachmentID + ext))
	}
	t = t.In(BrusselsTZ())
	return filepath.ToSlash(filepath.Join(
		fmt.Sprintf("%d", t.Year()),
		fmt.Sprintf("%02d", t.Month()),
		discordsource.ImageRelPath(attachmentID+ext),
	))
}

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

type reactionSimple struct {
	Emoji string `json:"emoji"`
	Count int    `json:"count"`
}

func convertReactions(reactions []struct {
	Emoji struct {
		Name string `json:"name"`
	} `json:"emoji"`
	Count int `json:"count"`
}) []reactionSimple {
	var out []reactionSimple
	for _, r := range reactions {
		out = append(out, reactionSimple{Emoji: r.Emoji.Name, Count: r.Count})
	}
	return out
}

func printGenerateHelp() {
	f := Fmt
	fmt.Printf(`
%schb generate%s — Generate derived data files from cached data

%sUSAGE%s
  %schb generate%s [year[/month]] [options]

Processes cached Discord messages, financial transactions, and events
to produce derived data files needed by the website:
  • contributors.json — top contributors
  • activitygrid.json — Discord activity heatmap
  • images.json — Discord images with reactions
  • transactions.json — aggregated financial data
  • counterparties.json — transaction counterparties
  • User profiles in generated/profiles/
  • Yearly aggregates

%sOPTIONS%s
  %s<year>%s               Generate for a specific year
  %s<year/month>%s         Generate for a specific month
  %s--since%s <date>       Generate from a specific date onward
  %s--history%s            Regenerate all months
  %s--help, -h%s           Show this help

%sNOTE%s
  By default, only the current month and previous month are regenerated.
  Run after 'chb sync' to refresh derived data for the recent window.
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
		f.Bold, f.Reset,
	)
}

// ── Members generation ─────────────────────────────────────────────────────

// generateMembersGo builds members.json for each month and latest/ from cached
// provider snapshots (written by `chb members sync`).
func generateMembersGo(dataDir string, scopes []generateScope) {
	totalMonths := 0
	var latestMembers []Member
	var latestSummary MembersSummary
	var latestYM string

	for _, scope := range scopes {
		year, month := scope.Year, scope.Month
		snapshots := loadCachedProviderSnapshots(dataDir, year, month)
		if len(snapshots) == 0 {
			continue
		}

		members := mergeProviderSnapshots(snapshots)
		summary := calculateMembersSummary(members)

		out := MembersOutputFile{
			Year:        year,
			Month:       month,
			ProductID:   "mixed",
			GeneratedAt: time.Now().UTC().Format(time.RFC3339),
			Summary:     summary,
			Members:     members,
		}

		data, _ := json.MarshalIndent(out, "", "  ")
		writeMonthFile(dataDir, year, month, filepath.Join("generated", "members.json"), data)
		totalMonths++

		ym := year + "-" + month
		if ym > latestYM {
			latestYM = ym
			latestMembers = members
			latestSummary = summary
		}

		fmt.Printf("  ✓ %s-%s: %d members (active: %d, MRR: €%.2f)\n",
			year, month, len(members), summary.ActiveMembers, summary.MRR.Value)
	}

	// Write latest
	if latestMembers != nil {
		parts := strings.SplitN(latestYM, "-", 2)
		out := MembersOutputFile{
			Year:        parts[0],
			Month:       parts[1],
			ProductID:   "mixed",
			GeneratedAt: time.Now().UTC().Format(time.RFC3339),
			Summary:     latestSummary,
			Members:     latestMembers,
		}
		data, _ := json.MarshalIndent(out, "", "  ")
		latestPath := filepath.Join(dataDir, "latest", "generated", "members.json")
		_ = writeDataFile(latestPath, data)
	}

	if totalMonths == 0 {
		fmt.Printf("  %sNo provider snapshots found. Run `chb members sync` first.%s\n", Fmt.Dim, Fmt.Reset)
	}
}

// loadCachedProviderSnapshots loads Stripe and Odoo provider snapshots from disk
// for a given year/month.
func loadCachedProviderSnapshots(dataDir, year, month string) []providerSnapshot {
	var snapshots []providerSnapshot

	paths := []string{
		stripesource.Path(dataDir, year, month, stripesource.SubscriptionsFile),
		odoosource.Path(dataDir, year, month, odoosource.SubscriptionsFile),
	}
	for _, snapPath := range paths {
		data, err := os.ReadFile(snapPath)
		if err != nil {
			continue
		}
		var snap providerSnapshot
		if json.Unmarshal(data, &snap) == nil && len(snap.Subscriptions) > 0 {
			snapshots = append(snapshots, snap)
		}
	}
	return snapshots
}

// ── Discord member count ───────────────────────────────────────────────────

// fetchDiscordMemberCount returns the approximate member count for the guild.
func fetchDiscordMemberCount(settings *Settings) int {
	if settings == nil || settings.Discord.GuildID == "" {
		return 0
	}
	token := os.Getenv("DISCORD_BOT_TOKEN")
	if token == "" {
		return 0
	}

	url := fmt.Sprintf("https://discord.com/api/v10/guilds/%s?with_counts=true", settings.Discord.GuildID)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return 0
	}
	req.Header.Set("Authorization", "Bot "+token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return 0
	}

	var guild struct {
		ApproximateMemberCount int `json:"approximate_member_count"`
	}
	json.NewDecoder(resp.Body).Decode(&guild)
	return guild.ApproximateMemberCount
}
