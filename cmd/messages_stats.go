package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	discordsource "github.com/CommonsHub/chb/sources/discord"
)

type MessagesStatsMonth struct {
	Month    string   `json:"month"`
	Count    int      `json:"count"`
	Channels []string `json:"channels"`
}

type MessagesStatsResult struct {
	Total  int                  `json:"total"`
	Months []MessagesStatsMonth `json:"months"`
}

type messagesFile struct {
	Messages []json.RawMessage `json:"messages"`
}

func MessagesStats(args []string) {
	if HasFlag(args, "--help", "-h", "help") {
		PrintMessagesStatsHelp()
		return
	}

	jsonOut := GetOption(args, "--format") == "json"
	dataDir := DataDir()

	// Build channel ID -> name map from settings
	channelNames := buildChannelNameMap()

	type monthInfo struct {
		count    int
		channels map[string]bool
	}
	monthData := map[string]*monthInfo{}
	total := 0

	yearDirs, _ := os.ReadDir(dataDir)
	for _, yd := range yearDirs {
		if !yd.IsDir() || len(yd.Name()) != 4 {
			continue
		}
		monthDirs, _ := os.ReadDir(filepath.Join(dataDir, yd.Name()))
		for _, md := range monthDirs {
			if !md.IsDir() || len(md.Name()) != 2 {
				continue
			}
			ym := yd.Name() + "-" + md.Name()
			discordDir := discordsource.Path(dataDir, yd.Name(), md.Name())
			if _, err := os.Stat(discordDir); os.IsNotExist(err) {
				continue
			}

			channelDirs, _ := os.ReadDir(discordDir)
			for _, cd := range channelDirs {
				if !cd.IsDir() {
					continue
				}
				msgPath := filepath.Join(discordDir, cd.Name(), discordsource.MessagesFile)
				data, err := os.ReadFile(msgPath)
				if err != nil {
					continue
				}
				var mf messagesFile
				if err := json.Unmarshal(data, &mf); err != nil {
					continue
				}
				n := len(mf.Messages)
				if n == 0 {
					continue
				}
				total += n
				if monthData[ym] == nil {
					monthData[ym] = &monthInfo{channels: map[string]bool{}}
				}
				monthData[ym].count += n
				name := channelNames[cd.Name()]
				if name == "" {
					name = cd.Name()
				}
				monthData[ym].channels[name] = true
			}
		}
	}

	var months []MessagesStatsMonth
	for ym, info := range monthData {
		var channels []string
		for c := range info.channels {
			channels = append(channels, c)
		}
		sort.Strings(channels)
		months = append(months, MessagesStatsMonth{
			Month:    ym,
			Count:    info.count,
			Channels: channels,
		})
	}
	sort.Slice(months, func(i, j int) bool {
		return months[i].Month > months[j].Month
	})

	if months == nil {
		months = []MessagesStatsMonth{}
	}
	result := MessagesStatsResult{Total: total, Months: months}

	if jsonOut {
		data, _ := json.MarshalIndent(result, "", "  ")
		fmt.Println(string(data))
		return
	}

	fmt.Printf("📊 Messages: %d total\n", result.Total)
	for _, m := range result.Months {
		fmt.Printf("  %s: %d messages [%s]\n", m.Month, m.Count, joinStrings(m.Channels))
	}
}

// buildChannelNameMap loads settings and builds channelID -> name map
func buildChannelNameMap() map[string]string {
	result := make(map[string]string)
	settings, err := LoadSettings()
	if err != nil {
		return result
	}
	idMap := GetDiscordChannelIDs(settings)
	// Reverse: id -> name
	for name, id := range idMap {
		// Use the shortest/simplest name for each ID
		if existing, ok := result[id]; ok {
			if len(name) < len(existing) {
				result[id] = name
			}
		} else {
			result[id] = name
		}
	}
	return result
}
