package cmd

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"sort"
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
	ProxyURL       string          `json:"proxyUrl"`
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
	Year      string             `json:"year"`
	Month     string             `json:"month"`
	Summary   ContributorSummary `json:"summary"`
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
	ID                string           `json:"id"`
	Username          string           `json:"username"`
	DisplayName       string           `json:"displayName"`
	Avatar            *string          `json:"avatar"`
	ContributionCount int              `json:"contributionCount"`
	JoinedAt          *string          `json:"joinedAt"`
	Introductions     []ProfileMessage `json:"introductions"`
	Contributions     []ProfileMessage `json:"contributions"`
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
	Year        string             `json:"year"`
	Summary     YearlyUsersSummary `json:"summary"`
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
	GrossAmount      float64                `json:"grossAmount"`
	NormalizedAmount float64                `json:"normalizedAmount"`
	Fee              float64                `json:"fee"`
	Type             string                 `json:"type"`
	Counterparty     string                 `json:"counterparty"`
	Timestamp        int64                  `json:"timestamp"`
	StripeChargeID   string                 `json:"stripeChargeId,omitempty"`
	Metadata         map[string]interface{} `json:"metadata,omitempty"`
}

type TransactionsFile struct {
	Year         string             `json:"year"`
	Month        string             `json:"month"`
	GeneratedAt  string             `json:"generatedAt"`
	Transactions []TransactionEntry `json:"transactions"`
}

// CounterpartyEntry for counterparties.json
type CounterpartyEntry struct {
	ID       string                  `json:"id"`
	Metadata CounterpartyMetadata    `json:"metadata"`
}

type CounterpartyMetadata struct {
	Description string  `json:"description"`
	Type        *string `json:"type"`
}

type CounterpartiesFile struct {
	Month           string              `json:"month"`
	GeneratedAt     string              `json:"generatedAt"`
	Counterparties  []CounterpartyEntry `json:"counterparties"`
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
	discordDir := filepath.Join(dataDir, year, month, "channels", "discord")
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
	msgPath := filepath.Join(dataDir, year, month, "channels", "discord", channelID, "messages.json")
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

func Generate(args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		printGenerateHelp()
		return nil
	}

	dataDir := DataDir()
	fmt.Printf("\n%s🔧 Generating derived data files...%s\n", Fmt.Bold, Fmt.Reset)
	fmt.Printf("%sDATA_DIR: %s%s\n\n", Fmt.Dim, dataDir, Fmt.Reset)

	years := getAvailableYears(dataDir)
	if len(years) == 0 {
		fmt.Println("⚠️  No data found. Run sync first.")
		return nil
	}

	fmt.Printf("📋 Found %d year(s): %s\n\n", len(years), strings.Join(years, ", "))

	settings, _ := LoadSettings()

	// 1. Generate images.json per month
	fmt.Printf("📸 Generating images...\n")
	totalImages := 0
	for _, year := range years {
		months := getAvailableMonths(dataDir, year)
		for _, month := range months {
			n := generateMonthImagesGo(dataDir, year, month)
			if n > 0 {
				fmt.Printf("  ✓ %s-%s: %d image(s)\n", year, month, n)
				totalImages += n
			}
		}
	}

	// Generate latest images
	latestDir := filepath.Join(dataDir, "latest")
	if _, err := os.Stat(latestDir); err == nil {
		n := generateLatestImagesGo(dataDir)
		totalImages += n
	}
	fmt.Printf("  %s%d total images%s\n\n", Fmt.Dim, totalImages, Fmt.Reset)

	// 2. Generate activity grid
	fmt.Printf("📊 Generating activity grids...\n")
	gridData := generateActivityGridGo(dataDir, years)
	for _, year := range years {
		generateYearActivityGridGo(dataDir, year, gridData)
	}
	fmt.Println()

	// 3. Generate monthly contributors
	fmt.Printf("👥 Generating monthly contributors...\n")
	for _, year := range years {
		months := getAvailableMonths(dataDir, year)
		for _, month := range months {
			n := generateMonthContributorsGo(dataDir, year, month, settings)
			if n > 0 {
				fmt.Printf("  ✓ %s-%s: %d contributor(s)\n", year, month, n)
			}
		}
	}
	// Also generate for latest/
	if _, err := os.Stat(latestDir); err == nil {
		n := generateMonthContributorsGo(dataDir, "latest", "", settings)
		if n > 0 {
			fmt.Printf("  ✓ latest: %d contributor(s)\n", n)
		}
	}
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
	for _, year := range years {
		generateYearlyUsersGo(dataDir, year, settings)
	}
	fmt.Println()

	// 7. Generate aggregated transactions
	fmt.Printf("💰 Generating transactions...\n")
	for _, year := range years {
		months := getAvailableMonths(dataDir, year)
		for _, month := range months {
			n := generateTransactionsGo(dataDir, year, month, settings)
			if n > 0 {
				fmt.Printf("  ✓ %s-%s: %d transaction(s)\n", year, month, n)
			}
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

	// 8. Generate counterparties
	fmt.Printf("🏢 Generating counterparties...\n")
	for _, year := range years {
		months := getAvailableMonths(dataDir, year)
		for _, month := range months {
			generateCounterpartiesGo(dataDir, year, month)
		}
	}
	// Also generate for latest/
	if _, err := os.Stat(latestDir); err == nil {
		generateCounterpartiesGo(dataDir, "latest", "")
	}
	fmt.Println()

	fmt.Printf("\n%s✅ All data generation complete!%s\n\n", Fmt.Green, Fmt.Reset)
	return nil
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

			proxyURL := fmt.Sprintf("/api/discord-image-proxy?channelId=%s&messageId=%s&attachmentId=%s&timestamp=%s",
				"", m.ID, att.ID, strings.ReplaceAll(m.Timestamp[:10], "-", ""))

			totalReactions := 0
			for _, r := range m.Reactions {
				totalReactions += r.Count
			}

			reactionsJSON, _ := json.Marshal(convertReactions(m.Reactions))

			images = append(images, ImageEntry{
				URL:            proxyURL,
				ProxyURL:       proxyURL,
				ID:             att.ID,
				Author:         ImageAuthor{ID: m.AuthorID, Username: m.AuthorUser, DisplayName: m.AuthorName, Avatar: m.AuthorAvat},
				Reactions:      reactionsJSON,
				TotalReactions: totalReactions,
				Message:        m.Content,
				Timestamp:      m.Timestamp,
				ChannelID:      "", // filled below from directory scan
				MessageID:      m.ID,
			})
		}
	}

	// Also scan per-channel to get channelID
	discordDir := filepath.Join(dataDir, year, month, "channels", "discord")
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
						proxyURL := fmt.Sprintf("/api/discord-image-proxy?channelId=%s&messageId=%s&attachmentId=%s&timestamp=%s",
							channelID, m.ID, att.ID, strings.ReplaceAll(m.Timestamp[:10], "-", ""))
						images[i].URL = proxyURL
						images[i].ProxyURL = proxyURL
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
	imgData, _ := json.MarshalIndent(out, "", "  ")
	writeMonthFile(dataDir, year, month, filepath.Join("channels", "discord", "images.json"), imgData)

	return len(images)
}

func generateLatestImagesGo(dataDir string) int {
	latestDiscord := filepath.Join(dataDir, "latest", "channels", "discord")
	entries, err := os.ReadDir(latestDiscord)
	if err != nil {
		return 0
	}

	var allImages []ImageEntry
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		channelID := e.Name()
		msgPath := filepath.Join(latestDiscord, channelID, "messages.json")
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
				proxyURL := fmt.Sprintf("/api/discord-image-proxy?channelId=%s&messageId=%s&attachmentId=%s&timestamp=%s",
					channelID, m.ID, att.ID, strings.ReplaceAll(m.Timestamp[:10], "-", ""))

				allImages = append(allImages, ImageEntry{
					URL:            proxyURL,
					ProxyURL:       proxyURL,
					ID:             att.ID,
					Author:         ImageAuthor{ID: m.AuthorID, Username: m.AuthorUser, DisplayName: m.AuthorName, Avatar: m.AuthorAvat},
					TotalReactions: 0,
					Message:        m.Content,
					Timestamp:      m.Timestamp,
					ChannelID:      channelID,
					MessageID:      m.ID,
				})
			}
		}
	}

	if len(allImages) == 0 {
		return 0
	}

	sort.Slice(allImages, func(i, j int) bool {
		return allImages[i].Timestamp > allImages[j].Timestamp
	})

	outputPath := filepath.Join(dataDir, "latest", "channels", "discord", "images.json")
	os.MkdirAll(filepath.Dir(outputPath), 0755)
	out := ImagesFile{Source: "latest", Count: len(allImages), Images: allImages}
	writeJSONFile(outputPath, out)

	fmt.Printf("  ✓ latest: %d image(s)\n", len(allImages))
	return len(allImages)
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

	outputPath := filepath.Join(dataDir, "activitygrid.json")
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
			outputPath := filepath.Join(dataDir, year, "activitygrid.json")
			os.MkdirAll(filepath.Dir(outputPath), 0755)
			writeJSONFile(outputPath, out)
			fmt.Printf("  ✓ %s activity grid\n", year)
			return
		}
	}
}

// ── Monthly contributors ────────────────────────────────────────────────────

func generateMonthContributorsGo(dataDir, year, month string, settings *Settings) int {
	discordDir := filepath.Join(dataDir, year, month, "channels", "discord")
	if _, err := os.Stat(discordDir); os.IsNotExist(err) {
		return 0
	}

	type userInfo struct {
		id, username, displayName, avatar string
		messages, mentions               int
		description                      string
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
		From  string `json:"from"`
		To    string `json:"to"`
		Value string `json:"value"`
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
	if settings != nil {
		// Try reading from settings contributionToken
		data, _ := os.ReadFile(filepath.Join(settingsDir(), "settings.json"))
		if data != nil {
			var s struct {
				ContributionToken struct {
					Decimals int `json:"decimals"`
				} `json:"contributionToken"`
			}
			if json.Unmarshal(data, &s) == nil && s.ContributionToken.Decimals > 0 {
				decimals = s.ContributionToken.Decimals
			}
		}
	}

	zeroAddr := "0x0000000000000000000000000000000000000000"

	// Build contributors
	var contributors []ContributorEntry
	for _, u := range users {
		var tokensIn, tokensOut float64

		// We don't have wallet address mapping in Go (would need Discord→wallet API calls)
		// So tokens stay at 0 unless we match addresses differently
		// For now, just count messages/mentions

		var avatarURL *string
		if u.avatar != "" {
			s := fmt.Sprintf("https://cdn.discordapp.com/avatars/%s/%s.png", u.id, u.avatar)
			avatarURL = &s
		}

		// Calculate tokens from CHT transactions if we can match addresses
		// (this is a simplified version - the TS version uses CitizenWallet API)
		_ = tokensIn
		_ = tokensOut
		_ = chtTxs
		_ = zeroAddr
		_ = decimals

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
			Address: nil,
		})
	}

	// Sort by messages desc
	sort.Slice(contributors, func(i, j int) bool {
		return contributors[i].Discord.Messages > contributors[j].Discord.Messages
	})

	summary := ContributorSummary{
		TotalContributors: len(contributors),
	}
	for _, c := range contributors {
		summary.TotalMessages += c.Discord.Messages
		summary.TotalTokensIn += c.Tokens.In
		summary.TotalTokensOut += c.Tokens.Out
		if c.Address != nil {
			summary.ContributorsWithAddr++
		}
		if c.Tokens.In > 0 || c.Tokens.Out > 0 {
			summary.ContributorsWithToken++
		}
	}

	out := MonthlyContributorsFile{
		Year:         year,
		Month:        month,
		Summary:      summary,
		Contributors: contributors,
		GeneratedAt:  time.Now().UTC().Format(time.RFC3339),
	}

	contribData, _ := json.MarshalIndent(out, "", "  ")
	writeMonthFile(dataDir, year, month, "contributors.json", contribData)

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

	outputPath := filepath.Join(dataDir, "contributors.json")
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
	globalPath := filepath.Join(dataDir, "contributors.json")
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
				imagesPath := filepath.Join(dataDir, year, month, "channels", "discord", "images.json")
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
		contribPath := filepath.Join(dataDir, year, month, "contributors.json")
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

	outputPath := filepath.Join(dataDir, year, "contributors.json")
	os.MkdirAll(filepath.Dir(outputPath), 0755)
	writeJSONFile(outputPath, out)
	fmt.Printf("  ✓ %s: %d contributors\n", year, len(contributors))
}

// ── Transactions ────────────────────────────────────────────────────────────

func generateTransactionsGo(dataDir, year, month string, settings *Settings) int {
	financeDir := filepath.Join(dataDir, year, month, "finance")
	if _, err := os.Stat(financeDir); os.IsNotExist(err) {
		return 0
	}

	var transactions []TransactionEntry

	// Process Stripe transactions
	stripeDir := filepath.Join(financeDir, "stripe")
	if entries, err := os.ReadDir(stripeDir); err == nil {
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			data, err := os.ReadFile(filepath.Join(stripeDir, e.Name()))
			if err != nil {
				continue
			}

			// Try StripeCacheFile format
			var stripeCacheFile struct {
				Transactions []struct {
					ID                string  `json:"id"`
					Amount            int64   `json:"amount"`
					Net               int64   `json:"net"`
					Fee               int64   `json:"fee"`
					Currency          string  `json:"currency"`
					Description       string  `json:"description"`
					Created           int64   `json:"created"`
					ReportingCategory string  `json:"reporting_category"`
					Type              string  `json:"type"`
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

			for _, tx := range stripeCacheFile.Transactions {
				amount := float64(tx.Amount) / 100
				fee := float64(tx.Fee) / 100
				net := float64(tx.Net) / 100
				txType := "CREDIT"
				if tx.Amount < 0 {
					txType = "DEBIT"
					amount = -amount
				}

				currency := strings.ToUpper(tx.Currency)
				if currency == "" {
					currency = "EUR"
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
					Amount:           net,
					GrossAmount:      math.Abs(amount),
					NormalizedAmount: net,
					Fee:              fee,
					Type:             txType,
					Counterparty:     tx.Description,
					Timestamp:        tx.Created,
					StripeChargeID:   tx.ID,
					Metadata: map[string]interface{}{
						"category":    tx.ReportingCategory,
						"description": tx.Description,
					},
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

				txType := "CREDIT"
				counterparty := tx.From
				if strings.EqualFold(tx.From, accountAddr) {
					txType = "DEBIT"
					counterparty = tx.To
				}

				ts := int64(0)
				fmt.Sscanf(tx.TimeStamp, "%d", &ts)

				chainStr := chain
				transactions = append(transactions, TransactionEntry{
					ID:               fmt.Sprintf("%s:%s", chain, tx.Hash[:Min(len(tx.Hash), 16)]),
					TxHash:           tx.Hash,
					Provider:         "etherscan",
					Chain:            &chainStr,
					Account:          accountAddr,
					AccountSlug:      chain,
					AccountName:      fmt.Sprintf("⛓️ %s %s", strings.Title(chain), tokenSymbol),
					Currency:         tokenSymbol,
					Value:            fmt.Sprintf("%.6f", amount),
					Amount:           amount,
					GrossAmount:      amount,
					NormalizedAmount: amount,
					Fee:              0,
					Type:             txType,
					Counterparty:     counterparty,
					Timestamp:        ts,
				})
			}
		}
	}

	// Check for known chain directories
	for _, chain := range []string{"celo", "gnosis", "ethereum"} {
		processChainDir(chain)
	}

	if len(transactions) == 0 {
		return 0
	}

	// Sort by timestamp
	sort.Slice(transactions, func(i, j int) bool {
		return transactions[i].Timestamp < transactions[j].Timestamp
	})

	out := TransactionsFile{
		Year:         year,
		Month:        month,
		GeneratedAt:  time.Now().UTC().Format(time.RFC3339),
		Transactions: transactions,
	}

	txData, _ := json.MarshalIndent(out, "", "  ")
	writeMonthFile(dataDir, year, month, "transactions.json", txData)

	return len(transactions)
}

// ── Counterparties ──────────────────────────────────────────────────────────

func generateCounterpartiesGo(dataDir, year, month string) {
	txPath := filepath.Join(dataDir, year, month, "transactions.json")
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
	writeMonthFile(dataDir, year, month, "counterparties.json", cpData)
}

// ── Helpers ─────────────────────────────────────────────────────────────────

// ── Helpers ─────────────────────────────────────────────────────────────────

func writeJSONFile(path string, v interface{}) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
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
  %schb generate%s [options]

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
  %s--help, -h%s           Show this help

%sNOTE%s
  Run after 'chb sync' to regenerate all derived data.
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
		f.Bold, f.Reset,
	)
}
