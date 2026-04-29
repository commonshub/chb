package cmd

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
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
	ID               string                 `json:"id"`
	TxHash           string                 `json:"txHash"`
	Provider         string                 `json:"provider"`
	Chain            *string                `json:"chain"`
	Account          string                 `json:"account"`
	AccountSlug      string                 `json:"accountSlug"`
	AccountName      string                 `json:"accountName"`
	Currency         string                 `json:"currency"`
	Value            string                 `json:"value"`
	Amount           float64                `json:"amount"`
	NetAmount        float64                `json:"netAmount,omitempty"`
	GrossAmount      float64                `json:"grossAmount"`
	NormalizedAmount float64                `json:"normalizedAmount"`
	Fee              float64                `json:"fee"`
	Type             string                 `json:"type"`
	Counterparty     string                 `json:"counterparty"`
	Timestamp        int64                  `json:"timestamp"`
	Application      string                 `json:"application,omitempty"`
	StripeChargeID   string                 `json:"stripeChargeId,omitempty"`
	Category         string                 `json:"category,omitempty"`
	Collective       string                 `json:"collective,omitempty"`
	Event            string                 `json:"event,omitempty"`
	Tags             [][]string             `json:"tags,omitempty"`
	Metadata         map[string]interface{} `json:"metadata,omitempty"`
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
		}
	}

	return &txFile
}

// CounterpartyEntry for counterparties.json
type CounterpartyEntry struct {
	ID       string               `json:"id"`
	Metadata CounterpartyMetadata `json:"metadata"`
}

type CounterpartyMetadata struct {
	Description string  `json:"description"`
	Type        *string `json:"type"`
}

type CounterpartiesFile struct {
	Month          string              `json:"month"`
	GeneratedAt    string              `json:"generatedAt"`
	Counterparties []CounterpartyEntry `json:"counterparties"`
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
	discordDir := filepath.Join(dataDir, year, month, "messages", "discord")
	entries, err := os.ReadDir(discordDir)
	if err != nil {
		return nil
	}

	var all []json.RawMessage
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		msgPath := filepath.Join(discordDir, e.Name(), "messages.json")
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
	msgPath := filepath.Join(dataDir, year, month, "messages", "discord", channelID, "messages.json")
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

	dataDir := DataDir()
	now := time.Now().In(BrusselsTZ())
	posYear, posMonth, posFound := ParseYearMonthArg(args)
	startMonth, isHistory := ResolveSinceMonth(args, "")
	if !isHistory && !posFound {
		startMonth = DefaultRecentStartMonth(now)
	}

	fmt.Printf("\n%s🔧 Generating derived data files...%s\n", Fmt.Bold, Fmt.Reset)
	fmt.Printf("%sDATA_DIR: %s%s\n\n", Fmt.Dim, dataDir, Fmt.Reset)

	years := getAvailableYears(dataDir)
	if len(years) == 0 {
		fmt.Println("⚠️  No data found. Run sync first.")
		return nil
	}

	fmt.Printf("📋 Found %d year(s): %s\n\n", len(years), strings.Join(years, ", "))

	scopes := collectGenerateScopes(dataDir, years, posYear, posMonth, posFound, startMonth)
	scopeYears := uniqueGenerateScopeYears(scopes)
	if len(scopes) > 0 {
		first := scopes[0].Year + "-" + scopes[0].Month
		last := scopes[len(scopes)-1].Year + "-" + scopes[len(scopes)-1].Month
		fmt.Printf("%sGeneration window: %s → %s%s\n\n", Fmt.Dim, first, last, Fmt.Reset)
	}

	// Write generated/ README
	writeGeneratedReadme(dataDir)

	settings, _ := LoadSettings()

	// 1. Generate images.json per month
	fmt.Printf("📸 Generating images...\n")
	totalImages := 0
	for _, scope := range scopes {
		n := generateMonthImagesGo(dataDir, scope.Year, scope.Month)
		if n > 0 {
			fmt.Printf("  ✓ %s-%s: %d image(s)\n", scope.Year, scope.Month, n)
			totalImages += n
		}
	}

	// Generate latest images (from latest/messages/discord/)
	latestDir := filepath.Join(dataDir, "latest")
	if _, err := os.Stat(latestDir); err == nil {
		n := generateMonthImagesGo(dataDir, "latest", "")
		if n > 0 {
			fmt.Printf("  ✓ latest: %d image(s)\n", n)
			totalImages += n
		}
	}
	fmt.Printf("  %s%d total images%s\n\n", Fmt.Dim, totalImages, Fmt.Reset)

	// 2. Generate activity grid
	fmt.Printf("📊 Generating activity grids...\n")
	gridData := generateActivityGridGo(dataDir, years)
	for _, year := range scopeYears {
		generateYearActivityGridGo(dataDir, year, gridData)
	}
	fmt.Println()

	// 3. Generate monthly contributors
	fmt.Printf("👥 Generating monthly contributors...\n")
	contributorsCache := newContributorsRunCache(dataDir)
	for _, scope := range scopes {
		n := generateMonthContributorsGo(dataDir, scope.Year, scope.Month, settings, contributorsCache, time.Time{})
		if n > 0 {
			fmt.Printf("  ✓ %s-%s: %d contributor(s)\n", scope.Year, scope.Month, n)
		}
	}
	// Also generate for latest/ — rolling 90-day window
	if _, err := os.Stat(latestDir); err == nil {
		cutoff := time.Now().UTC().AddDate(0, 0, -LatestContributorsWindowDays)
		n := generateMonthContributorsGo(dataDir, "latest", "", settings, contributorsCache, cutoff)
		if n > 0 {
			fmt.Printf("  ✓ latest (%dd): %d contributor(s)\n", LatestContributorsWindowDays, n)
		}
	}
	contributorsCache.save()
	fmt.Println()

	// 4. Generate top contributors (global contributors.json)
	fmt.Printf("👥 Generating top contributors...\n")
	generateTopContributorsGo(dataDir, settings)
	fmt.Println()

	// 5. Generate user profiles
	fmt.Printf("👤 Generating user profiles...\n")
	generateUserProfilesGo(dataDir, settings)
	fmt.Println()

	// 6. Generate yearly users
	fmt.Printf("📅 Generating yearly users...\n")
	for _, year := range scopeYears {
		generateYearlyUsersGo(dataDir, year, settings)
	}
	fmt.Println()

	// 7. Generate aggregated transactions
	fmt.Printf("💰 Generating transactions...\n")
	for _, scope := range scopes {
		n := generateTransactionsGo(dataDir, scope.Year, scope.Month, settings)
		if n > 0 {
			fmt.Printf("  ✓ %s-%s: %d transaction(s)\n", scope.Year, scope.Month, n)
		}
	}
	// Also generate for latest/
	if _, err := os.Stat(latestDir); err == nil {
		n := generateTransactionsGo(dataDir, "latest", "", settings)
		if n > 0 {
			fmt.Printf("  ✓ latest: %d transaction(s)\n", n)
		}
	}
	fmt.Println()

	// 8. Generate members from cached provider snapshots
	fmt.Printf("👥 Generating members...\n")
	generateMembersGo(dataDir, scopes)
	fmt.Println()

	// 8.5 Attach ticket-sale transactions to each event. Must run after
	// transactions are regenerated so the Event → tx index is current.
	fmt.Printf("🎟  Attaching ticket sales to events...\n")
	enrichEventsWithTicketSales(dataDir)
	fmt.Println()

	// 9. Generate latest events
	fmt.Printf("📅 Generating latest events...\n")
	generateLatestEventsGo(dataDir, years)
	fmt.Println()

	// 10. Generate counterparties
	fmt.Printf("🏢 Generating counterparties...\n")
	for _, scope := range scopes {
		generateCounterpartiesGo(dataDir, scope.Year, scope.Month)
	}
	// Also generate for latest/
	if _, err := os.Stat(latestDir); err == nil {
		generateCounterpartiesGo(dataDir, "latest", "")
	}
	fmt.Println()

	fmt.Printf("\n%s✅ All data generation complete!%s\n\n", Fmt.Green, Fmt.Reset)
	return nil
}

// GenerateTransactions runs only the transaction-related generators. This
// is what AccountFetch / `chb accounts <slug> sync` invoke, since they
// don't touch messages, images, events, members, etc.
//
// Steps: aggregated transactions (`transactions.json`) + counterparties.
func GenerateTransactions(args []string) error {
	dataDir := DataDir()
	now := time.Now().In(BrusselsTZ())
	posYear, posMonth, posFound := ParseYearMonthArg(args)
	startMonth, isHistory := ResolveSinceMonth(args, "")
	if !isHistory && !posFound {
		startMonth = DefaultRecentStartMonth(now)
	}

	years := getAvailableYears(dataDir)
	if len(years) == 0 {
		return nil
	}
	scopes := collectGenerateScopes(dataDir, years, posYear, posMonth, posFound, startMonth)

	settings, _ := LoadSettings()
	latestDir := filepath.Join(dataDir, "latest")

	fmt.Printf("\n%s💰 Generating transactions...%s\n", Fmt.Bold, Fmt.Reset)
	totalTx := 0
	for _, scope := range scopes {
		n := generateTransactionsGo(dataDir, scope.Year, scope.Month, settings)
		if n > 0 {
			fmt.Printf("  ✓ %s-%s: %d transaction(s)\n", scope.Year, scope.Month, n)
			totalTx += n
		}
	}
	if _, err := os.Stat(latestDir); err == nil {
		n := generateTransactionsGo(dataDir, "latest", "", settings)
		if n > 0 {
			fmt.Printf("  ✓ latest: %d transaction(s)\n", n)
			totalTx += n
		}
	}

	fmt.Printf("\n%s🏢 Generating counterparties...%s\n", Fmt.Bold, Fmt.Reset)
	for _, scope := range scopes {
		generateCounterpartiesGo(dataDir, scope.Year, scope.Month)
	}
	if _, err := os.Stat(latestDir); err == nil {
		generateCounterpartiesGo(dataDir, "latest", "")
	}

	fmt.Printf("\n%s✓ Transaction generators complete%s (%d tx across %d month(s))\n\n",
		Fmt.Green, Fmt.Reset, totalTx, len(scopes))
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
	generateLatestEventsGo(dataDir, years)
	fmt.Printf("%s✓ Events generators complete%s\n\n", Fmt.Green, Fmt.Reset)
	return nil
}

// GenerateMessages runs only message-derived generators: per-month images.
func GenerateMessages(args []string) error {
	dataDir := DataDir()
	now := time.Now().In(BrusselsTZ())
	posYear, posMonth, posFound := ParseYearMonthArg(args)
	startMonth, isHistory := ResolveSinceMonth(args, "")
	if !isHistory && !posFound {
		startMonth = DefaultRecentStartMonth(now)
	}
	years := getAvailableYears(dataDir)
	if len(years) == 0 {
		return nil
	}
	scopes := collectGenerateScopes(dataDir, years, posYear, posMonth, posFound, startMonth)

	fmt.Printf("\n%s📸 Generating images...%s\n", Fmt.Bold, Fmt.Reset)
	total := 0
	for _, scope := range scopes {
		n := generateMonthImagesGo(dataDir, scope.Year, scope.Month)
		if n > 0 {
			fmt.Printf("  ✓ %s-%s: %d image(s)\n", scope.Year, scope.Month, n)
			total += n
		}
	}
	if latestDir := filepath.Join(dataDir, "latest"); dirExists(latestDir) {
		n := generateMonthImagesGo(dataDir, "latest", "")
		if n > 0 {
			fmt.Printf("  ✓ latest: %d image(s)\n", n)
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
	posYear, posMonth, posFound := ParseYearMonthArg(args)
	startMonth, isHistory := ResolveSinceMonth(args, "")
	if !isHistory && !posFound {
		startMonth = DefaultRecentStartMonth(now)
	}
	years := getAvailableYears(dataDir)
	if len(years) == 0 {
		return nil
	}
	scopes := collectGenerateScopes(dataDir, years, posYear, posMonth, posFound, startMonth)

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

func collectGenerateScopes(dataDir string, years []string, posYear, posMonth string, posFound bool, startMonth string) []generateScope {
	var scopes []generateScope
	for _, year := range years {
		for _, month := range getAvailableMonths(dataDir, year) {
			ym := year + "-" + month
			if posFound {
				if posMonth != "" && ym != posYear+"-"+posMonth {
					continue
				}
				if posMonth == "" && !strings.HasPrefix(ym, posYear+"-") {
					continue
				}
			}
			if startMonth != "" && ym < startMonth {
				continue
			}
			scopes = append(scopes, generateScope{Year: year, Month: month})
		}
	}
	return scopes
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

// ── Image generation ────────────────────────────────────────────────────────

func generateMonthImagesGo(dataDir, year, month string) int {
	rawMessages := readMessages(dataDir, year, month)
	if len(rawMessages) == 0 {
		return 0
	}

	var images []ImageEntry
	for _, raw := range rawMessages {
		m := parseMessage(raw)
		for _, att := range m.Attachments {
			isImage := strings.HasPrefix(att.ContentType, "image/")
			if !isImage {
				// Check URL extension
				urlClean := strings.Split(att.URL, "?")[0]
				ext := strings.ToLower(filepath.Ext(urlClean))
				isImage = ext == ".jpg" || ext == ".jpeg" || ext == ".png" || ext == ".gif" || ext == ".webp"
			}
			if !isImage {
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
				ChannelID:      "", // filled below from directory scan
				MessageID:      m.ID,
				FilePath:       filePath,
			})
		}
	}

	// Also scan per-channel to get channelID
	discordDir := filepath.Join(dataDir, year, month, "messages", "discord")
	entries, _ := os.ReadDir(discordDir)
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		channelID := e.Name()
		msgPath := filepath.Join(discordDir, channelID, "messages.json")
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
				isImage := strings.HasPrefix(att.ContentType, "image/")
				if !isImage {
					urlClean := strings.Split(att.URL, "?")[0]
					ext := strings.ToLower(filepath.Ext(urlClean))
					isImage = ext == ".jpg" || ext == ".jpeg" || ext == ".png" || ext == ".gif" || ext == ".webp"
				}
				if !isImage {
					continue
				}

				// Update channelID for any image we already have
				for i := range images {
					if images[i].ID == att.ID {
						images[i].ChannelID = channelID
						images[i].URL = att.URL
					}
				}
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

	outputPath := filepath.Join(dataDir, "generated", "activitygrid.json")
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

	settingsPath := filepath.Join(settingsDir(), "settings.json")
	if _, err := os.Stat(settingsPath); err == nil {
		inputs = append(inputs, settingsPath)
	}

	imagesPath := filepath.Join(dataDir, year, month, "generated", "images.json")
	if _, err := os.Stat(imagesPath); err == nil {
		inputs = append(inputs, imagesPath)
	}

	discordDir := filepath.Join(dataDir, year, month, "messages", "discord")
	if entries, err := os.ReadDir(discordDir); err == nil {
		for _, e := range entries {
			if e.IsDir() {
				inputs = append(inputs, filepath.Join(discordDir, e.Name(), "messages.json"))
			}
		}
	}

	financeDir := filepath.Join(dataDir, year, month, "finance", "celo")
	if entries, err := os.ReadDir(financeDir); err == nil {
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			if e.Name() == "CHT.json" || strings.HasSuffix(e.Name(), ".CHT.json") {
				inputs = append(inputs, filepath.Join(financeDir, e.Name()))
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
	discordDir := filepath.Join(dataDir, year, month, "messages", "discord")
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

	// Read CHT transactions
	chtPath := filepath.Join(dataDir, year, month, "finance", "celo", "CHT.json")
	type chtTx struct {
		From      string `json:"from"`
		To        string `json:"to"`
		Value     string `json:"value"`
		TimeStamp string `json:"timeStamp"`
	}
	var chtTxs []chtTx
	if data, err := os.ReadFile(chtPath); err == nil {
		var chtFile struct {
			Transactions []chtTx `json:"transactions"`
		}
		json.Unmarshal(data, &chtFile)
		chtTxs = chtFile.Transactions
	}

	// Also try the new file format (slug.token.json)
	financeDir := filepath.Join(dataDir, year, month, "finance", "celo")
	if entries, err := os.ReadDir(financeDir); err == nil {
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), ".CHT.json") {
				continue
			}
			if data, err := os.ReadFile(filepath.Join(financeDir, e.Name())); err == nil {
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

	outputPath := filepath.Join(dataDir, "generated", "contributors.json")
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
	globalPath := filepath.Join(dataDir, "generated", "contributors.json")
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
		fmt.Printf("  ⚠ No contributors found\n")
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

	profilesDir := filepath.Join(dataDir, "generated", "profiles")
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

	fmt.Printf("  ✓ Generated %d user profile(s)\n", profileCount)
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
	financeDir := filepath.Join(dataDir, year, month, "finance")
	financeExists := true
	if _, err := os.Stat(financeDir); os.IsNotExist(err) {
		financeExists = false
	}
	stripePaths := stripeTransactionCachePaths(dataDir, year, month)
	if !financeExists && len(stripePaths) == 0 {
		return 0
	}

	// Build set of all tracked wallet addresses to detect internal transfers
	trackedAddresses := map[string]string{} // lowercase address -> account slug
	if settings != nil {
		for _, acc := range settings.Finance.Accounts {
			if acc.Address != "" {
				trackedAddresses[strings.ToLower(acc.Address)] = acc.Slug
			}
		}
	}

	var transactions []TransactionEntry
	seenTxHash := map[string]bool{} // track blockchain tx hashes to dedup internal transfers

	// Process Stripe transactions
	if len(stripePaths) > 0 {
		for _, path := range stripePaths {
			data, err := os.ReadFile(path)
			if err != nil {
				continue
			}

			// Try StripeCacheFile format
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
			stripeCustomers := loadStripeCustomerData(dataDir, year, month)

			// Load private charge enrichment (app info, payment methods)
			stripeCharges, refundToCharge := LoadStripeChargeEnrichment(dataDir, year, month)

			for _, tx := range stripeCacheFile.Transactions {
				amount := centsToEuros(tx.Amount)
				fee := centsToEuros(tx.Fee)
				net := centsToEuros(tx.Net)
				txType := "CREDIT"
				if tx.Amount < 0 {
					txType = "DEBIT"
					amount = -amount
				}
				// Payouts are handled by the payout-based Odoo sync, skip in generate
				if tx.ReportingCategory == "payout" {
					continue
				}
				// Stripe billing fees (usage fees, taxes) are real debits from the balance
				if tx.ReportingCategory == "fee" {
					txType = "DEBIT"
				}

				currency := strings.ToUpper(tx.Currency)
				if currency == "" {
					currency = "EUR"
				}

				// Determine counterparty: prefer private customer data, then inline, then charge enrichment
				counterparty := tx.CustomerName
				metadata := map[string]interface{}{
					"category":    tx.ReportingCategory,
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
				// Merge inline metadata from expanded source
				for k, v := range tx.Metadata {
					if s, ok := v.(string); ok && s != "" {
						metadata["stripe_"+k] = s
					}
				}

				// Merge charge/session enrichment for app, payment-link and event
				// metadata even when the balance transaction already had customer
				// data. Refunds are mapped back to their original charge.
				chID := extractChargeID(tx.Source)
				if chID == "" && refundToCharge != nil {
					srcID := extractSourceID(tx.Source)
					if strings.HasPrefix(srcID, "re_") {
						chID = refundToCharge[srcID]
					}
				}
				if stripeCharges != nil && chID != "" {
					if ch, ok := stripeCharges[chID]; ok {
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
						if ch.PaymentMethod != "" {
							metadata["paymentMethod"] = ch.PaymentMethod
						}
						if ch.PaymentLink != "" {
							metadata["paymentLink"] = ch.PaymentLink
						}
						for k, v := range ch.Metadata {
							if _, exists := metadata["stripe_"+k]; !exists {
								metadata["stripe_"+k] = v
							}
						}
						for k, v := range ch.CustomFields {
							metadata["custom_"+k] = v
						}
					}
				}

				// Final fallback: use balance tx description
				if counterparty == "" {
					counterparty = tx.Description
				}

				transactions = append(transactions, TransactionEntry{
					ID:               fmt.Sprintf("stripe:%s", tx.ID),
					TxHash:           tx.ID,
					Provider:         "stripe",
					Account:          "stripe",
					AccountSlug:      accountSlug,
					AccountName:      accountName,
					Currency:         currency,
					Value:            fmt.Sprintf("%.2f", net),
					Amount:           roundCents(net),
					NetAmount:        roundCents(net),
					GrossAmount:      roundCents(math.Abs(amount)),
					NormalizedAmount: roundCents(net),
					Fee:              roundCents(fee),
					Type:             txType,
					Counterparty:     counterparty,
					Timestamp:        tx.Created,
					Application:      stringMetadata(metadata, "application"),
					StripeChargeID:   tx.ID,
					Metadata:         metadata,
				})
			}
		}
	}

	// Process blockchain transactions (e.g. celo/CHT)
	processChainDir := func(chain string) {
		chainDir := filepath.Join(financeDir, chain)
		entries, err := os.ReadDir(chainDir)
		if err != nil {
			return
		}

		// Load Nostr metadata if available
		var nostrMeta NostrMetadataCache
		nostrPath := filepath.Join(chainDir, "nostr-metadata.json")
		if data, err := os.ReadFile(nostrPath); err == nil {
			json.Unmarshal(data, &nostrMeta)
		}

		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			data, err := os.ReadFile(filepath.Join(chainDir, e.Name()))
			if err != nil {
				continue
			}

			var txFile struct {
				Transactions []struct {
					Hash         string `json:"hash"`
					From         string `json:"from"`
					To           string `json:"to"`
					Value        string `json:"value"`
					TimeStamp    string `json:"timeStamp"`
					TokenDecimal string `json:"tokenDecimal"`
					TokenSymbol  string `json:"tokenSymbol"`
				} `json:"transactions"`
				Account string `json:"account"`
				Chain   string `json:"chain"`
				Token   string `json:"token"`
			}
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

			if tokenSymbol == "" {
				tokenSymbol = chain
			}

			for _, tx := range txFile.Transactions {
				dec := 18
				if tx.TokenDecimal != "" {
					fmt.Sscanf(tx.TokenDecimal, "%d", &dec)
				}

				val := new(big.Float)
				val.SetString(tx.Value)
				divisor := new(big.Float).SetFloat64(math.Pow10(dec))
				result := new(big.Float).Quo(val, divisor)
				amount, _ := result.Float64()

				zeroAddr := "0x0000000000000000000000000000000000000000"
				txType := "CREDIT"
				counterparty := tx.From
				if accountAddr != "" {
					// Wallet-specific tracking: outgoing = DEBIT
					if strings.EqualFold(tx.From, accountAddr) {
						txType = "DEBIT"
						counterparty = tx.To
					}
				} else {
					// Token-wide tracking (e.g. CHT): classify by mint/burn/transfer
					if strings.EqualFold(tx.From, zeroAddr) {
						txType = "CREDIT" // mint
					} else if strings.EqualFold(tx.To, zeroAddr) {
						txType = "DEBIT" // burn
					} else {
						txType = "TRANSFER" // regular transfer between addresses
					}
					counterparty = tx.From
				}

				// Detect internal transfers: both from and to are tracked accounts
				_, fromTracked := trackedAddresses[strings.ToLower(tx.From)]
				_, toTracked := trackedAddresses[strings.ToLower(tx.To)]
				isInternal := fromTracked && toTracked

				// EURb accounts (fridge, coffee) are like Stripe: withdrawals are internal
				if txType == "DEBIT" && strings.EqualFold(tokenSymbol, "EURb") {
					isInternal = true
				}

				internalDirection := ""
				if isInternal {
					internalDirection = txType
					// Only keep one side of internal transfers (skip if we already saw this tx)
					if seenTxHash[strings.ToLower(tx.Hash)] {
						continue
					}
					seenTxHash[strings.ToLower(tx.Hash)] = true
					// Mark as internal — neither in nor out for reporting
					txType = "INTERNAL"
				}

				ts := int64(0)
				fmt.Sscanf(tx.TimeStamp, "%d", &ts)

				chainStr := chain
				entry := TransactionEntry{
					ID:               fmt.Sprintf("%s:%s", chain, tx.Hash[:Min(len(tx.Hash), 16)]),
					TxHash:           tx.Hash,
					Provider:         "etherscan",
					Chain:            &chainStr,
					Account:          accountAddr,
					AccountSlug:      accountSlug,
					AccountName:      fmt.Sprintf("⛓️ %s %s", strings.Title(chain), tokenSymbol),
					Currency:         tokenSymbol,
					Value:            fmt.Sprintf("%.6f", amount),
					Amount:           amount,
					NetAmount:        amount,
					GrossAmount:      amount,
					NormalizedAmount: amount,
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

				transactions = append(transactions, entry)
			}
		}
	}

	// Check for known chain directories
	for _, chain := range []string{"celo", "gnosis", "ethereum"} {
		processChainDir(chain)
	}

	// Enrich blockchain transactions with Monerium counterparty data
	// Monerium orders have txHashes that match on-chain EURe transactions
	moneriumEnrichDir := filepath.Join(financeDir, "monerium", "private")
	if entries, err := os.ReadDir(moneriumEnrichDir); err == nil {
		// Build map: txHash → Monerium order counterparty + memo
		type moneriumInfo struct {
			Counterparty string
			Memo         string
		}
		moneriumByHash := map[string]*moneriumInfo{}

		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
				continue
			}
			data, err := os.ReadFile(filepath.Join(moneriumEnrichDir, e.Name()))
			if err != nil {
				continue
			}
			var cacheFile struct {
				Orders []struct {
					Kind        string `json:"kind"`
					Memo        string `json:"memo"`
					State       string `json:"state"`
					Counterpart struct {
						Details struct {
							Name        string `json:"name"`
							CompanyName string `json:"companyName"`
							FirstName   string `json:"firstName"`
							LastName    string `json:"lastName"`
						} `json:"details"`
					} `json:"counterpart"`
					Meta struct {
						TxHashes []string `json:"txHashes"`
					} `json:"meta"`
				} `json:"orders"`
			}
			if json.Unmarshal(data, &cacheFile) != nil {
				continue
			}
			for _, order := range cacheFile.Orders {
				name := order.Counterpart.Details.CompanyName
				if name == "" {
					name = order.Counterpart.Details.Name
				}
				if name == "" && order.Counterpart.Details.FirstName != "" {
					name = order.Counterpart.Details.FirstName + " " + order.Counterpart.Details.LastName
				}
				if name == "" {
					continue
				}
				for _, hash := range order.Meta.TxHashes {
					moneriumByHash[strings.ToLower(hash)] = &moneriumInfo{
						Counterparty: name,
						Memo:         order.Memo,
					}
				}
			}
		}

		// Enrich blockchain transactions
		if len(moneriumByHash) > 0 {
			for i := range transactions {
				tx := &transactions[i]
				if tx.Provider != "etherscan" || tx.TxHash == "" {
					continue
				}
				if info, ok := moneriumByHash[strings.ToLower(tx.TxHash)]; ok {
					// Only enrich if counterparty is a raw address (not already named)
					if strings.HasPrefix(tx.Counterparty, "0x") {
						tx.Counterparty = info.Counterparty
					}
					if info.Memo != "" {
						if tx.Metadata == nil {
							tx.Metadata = map[string]interface{}{}
						}
						tx.Metadata["memo"] = info.Memo
						if tx.Metadata["description"] == nil || tx.Metadata["description"] == "" {
							tx.Metadata["description"] = info.Memo
						}
					}
				}
			}
		}
	}

	if len(transactions) == 0 {
		return 0
	}

	// Load Nostr annotations (highest priority for categorization)
	nostrAnnotations := map[string]*TxAnnotation{}
	// Stripe annotations
	stripeAnnotPath := filepath.Join(dataDir, year, month, "finance", "stripe", "nostr-annotations.json")
	if data, err := os.ReadFile(stripeAnnotPath); err == nil {
		var cache NostrAnnotationCache
		if json.Unmarshal(data, &cache) == nil {
			for k, v := range cache.Annotations {
				nostrAnnotations[k] = v
			}
		}
	}
	// Blockchain annotations (from existing nostr-metadata.json tags)
	// These are already applied during transaction building above via TxMetadata.Tags

	// Load Odoo enrichment (second priority)
	odooCategories := map[string]string{} // stripeRef -> category
	odooPath := filepath.Join(dataDir, year, month, "finance", "odoo", "analytic-enrichment.json")
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
		categorizer := NewCategorizer(settings)
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
				}
			}

			// 1b. Auto-assign collective from Stripe metadata
			if tx.Collective == "" {
				// From payment link metadata: stripe_collective = "openletter"
				if col, ok := tx.Metadata["stripe_collective"]; ok {
					if colStr, ok := col.(string); ok && colStr != "" {
						tx.Collective = colStr
					}
				}
			}
			if tx.Collective == "" {
				// From Open Collective: stripe_to = "https://opencollective.com/openletter"
				if to, ok := tx.Metadata["stripe_to"]; ok {
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
				// Luma: stripe_event_api_id is the same UID as in events.json
				if evtID, ok := tx.Metadata["stripe_event_api_id"]; ok {
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

			// 4. Local rules (lowest priority, only if not set by higher sources)
			if tx.Category == "" {
				tx.Category = categorizer.Categorize(*tx)
			}
			if tx.Collective == "" {
				tx.Collective = categorizer.CollectiveFor(*tx)
			}
		}
	}

	runTransactionPlugins(dataDir, year, month, transactions)

	for i := range transactions {
		syncTransactionTags(&transactions[i])
	}

	// Sort by timestamp
	sort.Slice(transactions, func(i, j int) bool {
		return transactions[i].Timestamp < transactions[j].Timestamp
	})

	// Split PII from public transactions
	piiFile := TransactionsPIIFile{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Enrichments: map[string]*TransactionPII{},
	}

	publicTxs := make([]TransactionEntry, len(transactions))
	for i, tx := range transactions {
		publicTxs[i] = tx

		// Extract PII: customer name (for Stripe) and email.
		var piiName, piiEmail string
		if email, ok := tx.Metadata["email"].(string); ok && email != "" {
			piiEmail = email
		}

		// For Stripe/Monerium, counterparty may be a person's name (PII).
		// Blockchain 0x addresses are public, not PII.
		if tx.Provider == "stripe" || tx.Provider == "monerium" {
			if tx.Counterparty != "" && !strings.HasPrefix(tx.Counterparty, "0x") {
				piiName = tx.Counterparty
				// Replace with description or generic label in public version.
				if desc, ok := tx.Metadata["description"].(string); ok && desc != "" {
					publicTxs[i].Counterparty = desc
				} else if cat, ok := tx.Metadata["category"].(string); ok && cat != "" {
					publicTxs[i].Counterparty = tx.Provider + " " + cat
				} else {
					publicTxs[i].Counterparty = tx.Provider + " " + strings.ToLower(tx.Type)
				}
			}
		}

		// Strip ANY email-looking value from public metadata. Merchant-side
		// metadata often carries emails under keys like `stripe_email`,
		// `stripe_receipt_email`, `customer_email` etc., so we can't just
		// drop one well-known key — we detect by pattern and preserve
		// the first one we see in the PII file.
		if len(tx.Metadata) > 0 {
			publicMeta := make(map[string]interface{}, len(tx.Metadata))
			for k, v := range tx.Metadata {
				if s, ok := v.(string); ok && emailPattern.MatchString(s) {
					if piiEmail == "" {
						piiEmail = s
					}
					continue
				}
				publicMeta[k] = v
			}
			delete(publicMeta, "email") // legacy key, always redundant
			publicTxs[i].Metadata = publicMeta
		}

		if piiName != "" || piiEmail != "" {
			piiFile.Enrichments[tx.ID] = &TransactionPII{
				Name:  piiName,
				Email: piiEmail,
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

// ── Counterparties ──────────────────────────────────────────────────────────

func generateCounterpartiesGo(dataDir, year, month string) {
	txPath := filepath.Join(dataDir, year, month, "generated", "transactions.json")
	data, err := os.ReadFile(txPath)
	if err != nil {
		return
	}

	var txFile TransactionsFile
	if json.Unmarshal(data, &txFile) != nil || len(txFile.Transactions) == 0 {
		return
	}

	seen := map[string]bool{}
	var counterparties []CounterpartyEntry
	for _, tx := range txFile.Transactions {
		cp := tx.Counterparty
		if cp == "" || seen[cp] {
			continue
		}
		seen[cp] = true

		desc := ""
		if tx.Metadata != nil {
			if d, ok := tx.Metadata["description"]; ok {
				if s, ok := d.(string); ok {
					desc = s
				}
			}
		}

		counterparties = append(counterparties, CounterpartyEntry{
			ID:       cp,
			Metadata: CounterpartyMetadata{Description: desc},
		})
	}

	if len(counterparties) == 0 {
		return
	}

	out := CounterpartiesFile{
		Month:          fmt.Sprintf("%s-%s", year, month),
		GeneratedAt:    time.Now().UTC().Format(time.RFC3339),
		Counterparties: counterparties,
	}

	cpData, _ := json.MarshalIndent(out, "", "  ")
	writeMonthFile(dataDir, year, month, filepath.Join("generated", "counterparties.json"), cpData)
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

func generateLatestEventsGo(dataDir string, years []string) {
	now := time.Now()
	today := now.Format("2006-01-02")

	var allEvents []FullEvent

	for _, year := range years {
		yearPath := filepath.Join(dataDir, year)
		monthDirs, err := os.ReadDir(yearPath)
		if err != nil {
			continue
		}
		for _, d := range monthDirs {
			if !d.IsDir() || len(d.Name()) != 2 {
				continue
			}
			eventsPath := filepath.Join(yearPath, d.Name(), "generated", "events.json")
			data, err := os.ReadFile(eventsPath)
			if err != nil {
				continue
			}
			var ef FullEventsFile
			if json.Unmarshal(data, &ef) == nil {
				allEvents = append(allEvents, ef.Events...)
			}
		}
	}

	// Defensive dedup: if any per-month file still carries duplicate events
	// (URL + start + end), collapse them so the aggregated latest feed is
	// clean even before those months get re-synced.
	allEvents = dedupeFullEvents(allEvents)

	// Filter to upcoming public events (have a URL and startAt >= today)
	var upcoming []LatestEvent
	for _, ev := range allEvents {
		startDate := ev.StartAt
		if len(startDate) >= 10 {
			startDate = startDate[:10]
		}
		if startDate < today {
			continue
		}
		if ev.URL == "" {
			continue
		}
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

	// Sort by start date ascending
	sort.Slice(upcoming, func(i, j int) bool {
		return upcoming[i].StartAt < upcoming[j].StartAt
	})

	outputPath := filepath.Join(dataDir, "latest", "generated", "events.json")
	out := LatestEventsFile{
		GeneratedAt: now.UTC().Format(time.RFC3339),
		Count:       len(upcoming),
		Events:      upcoming,
	}
	writeJSONFile(outputPath, out)
	fmt.Printf("  ✓ latest/events.json: %d upcoming event(s)\n", len(upcoming))
}

// ── Generated README ────────────────────────────────────────────────────────

func writeGeneratedReadme(dataDir string) {
	readme := `# generated/

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
	readmePath := filepath.Join(dataDir, "generated", "README.md")
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
		return filepath.ToSlash(filepath.Join("messages", "discord", "images", attachmentID+ext))
	}
	t = t.In(BrusselsTZ())
	return filepath.ToSlash(filepath.Join(
		fmt.Sprintf("%d", t.Year()),
		fmt.Sprintf("%02d", t.Month()),
		"messages",
		"discord",
		"images",
		attachmentID+ext,
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
  %s--since%s <YYYY/MM>    Generate from a specific month onward
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
	monthPath := filepath.Join(dataDir, year, month)

	providers := []string{"stripe", "odoo"}
	for _, p := range providers {
		snapPath := filepath.Join(monthPath, "finance", p, "subscriptions.json")
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
