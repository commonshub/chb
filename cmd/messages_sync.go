package cmd

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
)

const discordAPIBase = "https://discord.com/api/v10"

// DiscordMessage represents a Discord message
type DiscordMessage struct {
	ID          string              `json:"id"`
	ChannelID   string              `json:"channel_id,omitempty"`
	Author      DiscordAuthor       `json:"author"`
	Content     string              `json:"content"`
	Timestamp   string              `json:"timestamp"`
	Attachments []DiscordAttachment `json:"attachments"`
	Embeds      []json.RawMessage   `json:"embeds"`
	Mentions    []DiscordAuthor     `json:"mentions"`
	Reactions   []DiscordReaction   `json:"reactions,omitempty"`
}

// DiscordAuthor represents a Discord user
type DiscordAuthor struct {
	ID         string  `json:"id"`
	Username   string  `json:"username"`
	GlobalName *string `json:"global_name"`
	Avatar     *string `json:"avatar"`
}

// DiscordAttachment represents a message attachment
type DiscordAttachment struct {
	ID          string `json:"id"`
	URL         string `json:"url"`
	ProxyURL    string `json:"proxy_url"`
	ContentType string `json:"content_type,omitempty"`
}

// DiscordReaction represents a message reaction
type DiscordReaction struct {
	Emoji DiscordEmoji `json:"emoji"`
	Count int          `json:"count"`
	Me    bool         `json:"me"`
}

// DiscordEmoji represents a Discord emoji
type DiscordEmoji struct {
	ID   *string `json:"id"`
	Name string  `json:"name"`
}

// MessagesCacheFile is the structure saved to disk
type MessagesCacheFile struct {
	Messages  []DiscordMessage `json:"messages"`
	CachedAt  string           `json:"cachedAt"`
	ChannelID string           `json:"channelId"`
}

func MessagesSync(args []string) (int, error) {
	if HasFlag(args, "--help", "-h", "help") {
		printMessagesSyncHelp()
		return 0, nil
	}

	settings, err := LoadSettings()
	if err != nil {
		return 0, fmt.Errorf("failed to load settings: %w", err)
	}

	token := os.Getenv("DISCORD_BOT_TOKEN")
	if token == "" {
		return 0, fmt.Errorf("DISCORD_BOT_TOKEN environment variable required")
	}

	force := HasFlag(args, "--force")
	monthFilter := GetOption(args, "--month")
	channelFilter := GetOption(args, "--channel")

	// Positional year/month arg (e.g. "2025" or "2025/03")
	posYear, posMonth, posFound := ParseYearMonthArg(args)

	// Check --since / --history
	resolvedSince, isSince := ResolveSinceMonth(args, "messages")
	recentStartMonth := DefaultRecentStartMonth(time.Now())
	defaultRecentWindow := !isSince && !posFound && monthFilter == ""

	fmt.Printf("\n%s💬 Syncing Discord messages%s\n", Fmt.Bold, Fmt.Reset)
	fmt.Printf("%sDATA_DIR: %s%s\n", Fmt.Dim, DataDir(), Fmt.Reset)
	fmt.Printf("%sGuild: %s%s\n\n", Fmt.Dim, settings.Discord.GuildID, Fmt.Reset)

	// Get all channel IDs from settings
	channels := GetDiscordChannelIDs(settings)
	if len(channels) == 0 {
		return 0, fmt.Errorf("no Discord channels configured in settings.json")
	}

	totalMessages := 0
	for name, channelID := range channels {
		if channelFilter != "" && channelID != channelFilter && name != channelFilter {
			continue
		}

		fmt.Printf("  #%s (%s)\n", name, channelID)

		var messages []DiscordMessage
		var err error

		if isSince {
			// --history or --since: paginate backwards
			stopMonth := ""
			if !force {
				stopMonth = findOldestCachedMonthForChannel(channelID)
			}
			messages, err = fetchAllChannelMessages(channelID, token, stopMonth)
		} else if defaultRecentWindow {
			messages, err = fetchAllChannelMessages(channelID, token, recentStartMonth)
		} else {
			// Explicit month/year filters only need the latest page unless the user
			// requested a broader historical sync.
			messages, err = fetchLatestMessages(channelID, token)
		}

		if err != nil {
			fmt.Printf("    %s✗ Error: %v%s\n", Fmt.Red, err, Fmt.Reset)
			continue
		}

		fmt.Printf("    %sFetched %d messages%s\n", Fmt.Dim, len(messages), Fmt.Reset)

		// Group by month
		byMonth := groupMessagesByMonth(messages)

		// Determine which months to save.
		// Even quick syncs should persist every month represented in the fetched page,
		// so latest/ and the canonical YYYY/MM cache stay aligned.
		var monthsToSave []string
		for ym := range byMonth {
			monthsToSave = append(monthsToSave, ym)
		}
		sort.Strings(monthsToSave)

		saved := 0
		for _, ym := range monthsToSave {
			monthMsgs := byMonth[ym]

			if monthFilter != "" && ym != monthFilter {
				continue
			}
			if defaultRecentWindow && ym < recentStartMonth {
				continue
			}
			// --since / --history filter
			if isSince && ym < resolvedSince {
				continue
			}
			// Positional year/month filter
			if posFound {
				if posMonth != "" {
					if ym != fmt.Sprintf("%s-%s", posYear, posMonth) {
						continue
					}
				} else {
					if !strings.HasPrefix(ym, posYear+"-") {
						continue
					}
				}
			}

			parts := strings.Split(ym, "-")
			if len(parts) != 2 {
				continue
			}
			year, month := parts[0], parts[1]

			// Save to data/YYYY/MM/channels/discord/{channelId}/messages.json
			dataDir := DataDir()
			relPath := filepath.Join("messages", "discord", channelID, "messages.json")

			cache := MessagesCacheFile{
				Messages:  monthMsgs,
				CachedAt:  time.Now().UTC().Format(time.RFC3339),
				ChannelID: channelID,
			}

			data, _ := json.MarshalIndent(cache, "", "  ")
			if err := writeMonthFile(dataDir, year, month, relPath, data); err != nil {
				fmt.Printf("    %s✗ Failed to write: %v%s\n", Fmt.Red, err, Fmt.Reset)
				continue
			}

			saved++
			totalMessages += len(monthMsgs)
		}

		if saved > 0 {
			fmt.Printf("    %s✓ Saved %d months%s\n", Fmt.Green, saved, Fmt.Reset)
		}

		// Write ALL fetched messages to latest/ (the full batch, not split by month).
		// This ensures latest/ has every message the API returned for this channel.
		if len(messages) > 0 {
			dataDir := DataDir()
			relPath := filepath.Join("messages", "discord", channelID, "messages.json")
			cache := MessagesCacheFile{
				Messages:  messages,
				CachedAt:  time.Now().UTC().Format(time.RFC3339),
				ChannelID: channelID,
			}
			data, _ := json.MarshalIndent(cache, "", "  ")
			latestPath := filepath.Join(dataDir, "latest", relPath)
			os.MkdirAll(filepath.Dir(latestPath), 0755)
			os.WriteFile(latestPath, data, 0644)
		}

		// Rate limit between channels
		time.Sleep(500 * time.Millisecond)
	}

	fmt.Printf("\n%s✓ Done!%s %d messages synced\n\n", Fmt.Green, Fmt.Reset, totalMessages)
	UpdateSyncSource("messages", isSince)
	UpdateSyncActivity(isSince)
	return totalMessages, nil
}

// fetchLatestMessages fetches one page (100 messages) from a Discord channel.
// No pagination — used for quick sync of latest data.
func fetchLatestMessages(channelID, token string) ([]DiscordMessage, error) {
	url := fmt.Sprintf("%s/channels/%s/messages?limit=100", discordAPIBase, channelID)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bot "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 429 {
		var rateLimitResp struct {
			RetryAfter float64 `json:"retry_after"`
		}
		json.NewDecoder(resp.Body).Decode(&rateLimitResp)
		wait := time.Duration(rateLimitResp.RetryAfter*1000+100) * time.Millisecond
		time.Sleep(wait)
		return fetchLatestMessages(channelID, token)
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("Discord API error: %d", resp.StatusCode)
	}

	var messages []DiscordMessage
	if err := json.NewDecoder(resp.Body).Decode(&messages); err != nil {
		return nil, err
	}
	return messages, nil
}

// fetchAllChannelMessages fetches messages from Discord, paginating backwards.
// If stopBeforeMonth is set (e.g. "2025-06"), stop paginating once we hit messages
// older than that month (they're already cached).
func fetchAllChannelMessages(channelID, token, stopBeforeMonth string) ([]DiscordMessage, error) {
	var allMessages []DiscordMessage
	var before string
	tz := BrusselsTZ()

	for {
		url := fmt.Sprintf("%s/channels/%s/messages?limit=100", discordAPIBase, channelID)
		if before != "" {
			url += "&before=" + before
		}

		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bot "+token)
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}

		if resp.StatusCode == 429 {
			// Rate limited
			var rateLimitResp struct {
				RetryAfter float64 `json:"retry_after"`
			}
			json.NewDecoder(resp.Body).Decode(&rateLimitResp)
			resp.Body.Close()

			wait := time.Duration(rateLimitResp.RetryAfter*1000+100) * time.Millisecond
			fmt.Printf("    %sRate limited, waiting %v%s\n", Fmt.Yellow, wait, Fmt.Reset)
			time.Sleep(wait)
			continue
		}

		if resp.StatusCode != 200 {
			resp.Body.Close()
			return nil, fmt.Errorf("Discord API error: %d", resp.StatusCode)
		}

		var messages []DiscordMessage
		if err := json.NewDecoder(resp.Body).Decode(&messages); err != nil {
			resp.Body.Close()
			return nil, err
		}
		resp.Body.Close()

		if len(messages) == 0 {
			break
		}

		// Check if we've reached cached data
		hitCached := false
		if stopBeforeMonth != "" {
			for _, msg := range messages {
				t, err := time.Parse(time.RFC3339Nano, msg.Timestamp)
				if err != nil {
					t, _ = time.Parse("2006-01-02T15:04:05+00:00", msg.Timestamp)
				}
				t = t.In(tz)
				msgYM := fmt.Sprintf("%d-%02d", t.Year(), t.Month())
				if msgYM < stopBeforeMonth {
					hitCached = true
					break
				}
			}
		}

		allMessages = append(allMessages, messages...)

		if hitCached {
			fmt.Printf("    %sReached cached data at %s, stopping%s\n", Fmt.Dim, stopBeforeMonth, Fmt.Reset)
			break
		}

		before = messages[len(messages)-1].ID

		// Rate limit
		time.Sleep(300 * time.Millisecond)
	}

	return allMessages, nil
}

func groupMessagesByMonth(messages []DiscordMessage) map[string][]DiscordMessage {
	byMonth := make(map[string][]DiscordMessage)
	tz := BrusselsTZ()

	for _, msg := range messages {
		t, err := time.Parse(time.RFC3339Nano, msg.Timestamp)
		if err != nil {
			t, err = time.Parse("2006-01-02T15:04:05+00:00", msg.Timestamp)
			if err != nil {
				continue
			}
		}
		t = t.In(tz)
		ym := fmt.Sprintf("%d-%02d", t.Year(), t.Month())
		byMonth[ym] = append(byMonth[ym], msg)
	}

	// Sort messages within each month by timestamp
	for ym := range byMonth {
		sort.Slice(byMonth[ym], func(i, j int) bool {
			return byMonth[ym][i].Timestamp < byMonth[ym][j].Timestamp
		})
	}

	return byMonth
}

func printMessagesSyncHelp() {
	f := Fmt
	fmt.Printf(`
%schb messages sync%s — Fetch Discord messages

%sUSAGE%s
  %schb messages sync%s [year[/month]] [options]

%sTIME RANGE%s
  %s(no args)%s              Fetch current month + previous month
  %s<year/month>%s           Only save messages from that month (e.g. 2025/03)
  %s<year>%s                 Only save messages from that year (e.g. 2025)
  %s--since%s YYYY/MM        Only save messages from that month onward (also: YYYYMM)
  %s--history%s              Paginate backwards, stop at oldest cached month

%sFILTERING%s
  %s--channel%s <id|name>    Fetch a specific channel only
  %s--month%s <YYYY-MM>      Alias for year/month positional arg

%sOPTIONS%s
  %s--force%s                Re-fetch and overwrite cached months
  %s--help, -h%s             Show this help

%sBEHAVIOR%s
  Messages are fetched from newest to oldest (Discord API pagination).
  Each page returns 100 messages. Data is saved per month to:
    ~/.chb/data/YYYY/MM/channels/discord/{channelId}/messages.json

  %s--history%s: paginates backwards until hitting a month with cached
  data, then stops. Saves everything from that point forward.
  Use %s--history --force%s to re-fetch and overwrite all cached months.

  If a sync fails mid-way (e.g. network error), re-run with:
    chb messages sync --channel <id> --force

%sENVIRONMENT%s
  %sDISCORD_BOT_TOKEN%s      Discord bot token (configure via chb setup)

%sEXAMPLES%s
  %schb messages sync%s                             Fetch all channels for the recent 2-month window
  %schb messages sync --history%s                   Fetch new messages since last sync
  %schb messages sync --channel general%s           Fetch only #general
  %schb messages sync --channel 129796 --force%s    Re-fetch a specific channel
  %schb messages sync --since 2024/06%s             Save messages from Jun 2024 onward
  %schb messages sync 2025%s                        Save only 2025 messages
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Dim, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
	)
}

// findOldestCachedMonthForChannel finds the oldest month that has cached
// Discord messages for any channel. Used as a stop point during pagination.
func findOldestCachedMonthForChannel(channelID string) string {
	dataDir := DataDir()
	oldest := ""

	years, err := os.ReadDir(dataDir)
	if err != nil {
		return ""
	}

	for _, yd := range years {
		if !yd.IsDir() || len(yd.Name()) != 4 {
			continue
		}
		year := yd.Name()
		if _, err := strconv.Atoi(year); err != nil {
			continue
		}

		months, _ := os.ReadDir(filepath.Join(dataDir, year))
		for _, md := range months {
			if !md.IsDir() || len(md.Name()) != 2 {
				continue
			}
			month := md.Name()

			msgPath := filepath.Join(dataDir, year, month, "messages", "discord", channelID, "messages.json")
			if _, err := os.Stat(msgPath); err == nil {
				ym := year + "-" + month
				if oldest == "" || ym < oldest {
					oldest = ym
				}
			}
		}
	}

	return oldest
}
