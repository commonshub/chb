package cmd

// #contributions as a public feed: YYYY/MM/<tier>/contributions.json and
// latest/<tier>/contributions.json (the last 60 days), the same in every
// tier. Who posted, who was mentioned (thanked), when, how many
// reactions, and the public copies of the photos. No message text.
//
// Only when the contributions channel (settings discord.channels
// "contributions") is one of the public channels (discord.publicChannels,
// images_public.go): its photos and their authors are already public.
// Bots are dropped. Identity is the Discord display identity
// contributors.json already publishes.

import (
	"encoding/json"
	"os"
	"sort"
	"strings"
	"time"

	discordsource "github.com/CommonsHub/chb/providers/discord"
)

const contributionsFile = "contributions.json"

// contributionsLatestDays: how far back latest/contributions.json reaches.
const contributionsLatestDays = 60

type ContributionsFile struct {
	GeneratedAt string                `json:"generatedAt"`
	ChannelID   string                `json:"channelId"`
	Messages    []ContributionMessage `json:"messages"` // newest first
}

type ContributionMessage struct {
	ID             string                 `json:"id"`
	Timestamp      string                 `json:"timestamp"`
	Author         ContributionIdentity   `json:"author"`
	Mentions       []ContributionIdentity `json:"mentions"`
	TotalReactions int                    `json:"totalReactions"`
	Images         []string               `json:"images"` // public copies, relative to the data root
}

type ContributionIdentity struct {
	ID          string `json:"id"`
	DisplayName string `json:"displayName"`
}

// contributionsChannelID returns the #contributions channel id, or "" when
// it is not configured or not public.
func contributionsChannelID() string {
	var s struct {
		Discord struct {
			Channels json.RawMessage `json:"channels"`
		} `json:"discord"`
	}
	if data, err := os.ReadFile(settingsFilePath("settings.json")); err == nil {
		_ = json.Unmarshal(data, &s)
	}
	names := map[string]string{}
	flattenDiscordChannels(s.Discord.Channels, "", names)
	id := names["contributions"]
	if id == "" || !publicPhotoChannelIDs()[id] {
		return ""
	}
	return id
}

func contributionIdentity(a discordsource.Author) ContributionIdentity {
	name := a.Username
	if a.GlobalName != nil && strings.TrimSpace(*a.GlobalName) != "" {
		name = *a.GlobalName
	}
	if containsEmail(name) { // a mailbox standing in for a name is a contact detail
		name = a.Username
		if containsEmail(name) {
			name = ""
		}
	}
	return ContributionIdentity{ID: a.ID, DisplayName: name}
}

// monthContributions reads the month's #contributions messages.
func monthContributions(dataDir, year, month, channelID string) []ContributionMessage {
	data, err := os.ReadFile(discordsource.ChannelPath(dataDir, year, month, channelID))
	if err != nil {
		return nil
	}
	var f discordsource.CacheFile
	if json.Unmarshal(data, &f) != nil {
		return nil
	}
	// Public photo copies by message, from the month's public images.json.
	photos := map[string][]string{}
	if data, err := os.ReadFile(audiencePath(dataDir, year, month, AudiencePublic, "images.json")); err == nil {
		var imgs ImagesFile
		if json.Unmarshal(data, &imgs) == nil {
			for _, img := range imgs.Images {
				if img.FilePath != "" {
					photos[img.MessageID] = append(photos[img.MessageID], img.FilePath)
				}
			}
		}
	}
	var out []ContributionMessage
	for _, m := range f.Messages {
		if m.Author.Bot || m.Author.ID == "" {
			continue
		}
		msg := ContributionMessage{
			ID:        m.ID,
			Timestamp: normaliseDiscordTimestamp(m.Timestamp),
			Author:    contributionIdentity(m.Author),
			Mentions:  []ContributionIdentity{},
			Images:    photos[m.ID],
		}
		if msg.Images == nil {
			msg.Images = []string{}
		}
		seen := map[string]bool{}
		for _, u := range m.Mentions {
			if u.Bot || u.ID == "" || seen[u.ID] {
				continue
			}
			seen[u.ID] = true
			msg.Mentions = append(msg.Mentions, contributionIdentity(u))
		}
		for _, r := range m.Reactions {
			msg.TotalReactions += r.Count
		}
		out = append(out, msg)
	}
	return out
}

// normaliseDiscordTimestamp: RFC3339 in UTC ("2025-08-01T13:42:08Z").
func normaliseDiscordTimestamp(ts string) string {
	if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
		return t.UTC().Format(time.RFC3339)
	}
	return ts
}

func sortContributionsNewestFirst(ms []ContributionMessage) {
	sort.SliceStable(ms, func(i, j int) bool {
		if ms[i].Timestamp != ms[j].Timestamp {
			return ms[i].Timestamp > ms[j].Timestamp
		}
		return ms[i].ID > ms[j].ID
	})
}

// writeContributions writes f in every tier unless the stewards copy
// already holds the same messages (no generatedAt churn: completed months'
// tier files are hashed in hashes.json).
func writeContributions(dataDir, year, month string, f ContributionsFile) bool {
	if data, err := os.ReadFile(audiencePath(dataDir, year, month, AudienceStewards, contributionsFile)); err == nil {
		var prev ContributionsFile
		if json.Unmarshal(data, &prev) == nil && prev.ChannelID == f.ChannelID {
			a, _ := json.Marshal(prev.Messages)
			b, _ := json.Marshal(f.Messages)
			if string(a) == string(b) {
				return false
			}
		}
	}
	writeTiersNoMirror(dataDir, year, month, contributionsFile, func(a Audience) interface{} { return f })
	return true
}

// generateContributions writes the file of every month that has a
// #contributions archive (cheap, and unchanged months are not rewritten)
// and the latest window. Returns the number of messages in the latest
// window.
func generateContributions(dataDir string) int {
	channelID := contributionsChannelID()
	if channelID == "" {
		return 0
	}
	now := time.Now().UTC()
	for _, ym := range dataMonthRange(dataDir) {
		s := generateScope{Year: ym[:4], Month: ym[5:]}
		if _, err := os.Stat(discordsource.ChannelPath(dataDir, s.Year, s.Month, channelID)); err != nil {
			continue
		}
		ms := monthContributions(dataDir, s.Year, s.Month, channelID)
		if ms == nil {
			ms = []ContributionMessage{}
		}
		sortContributionsNewestFirst(ms)
		writeContributions(dataDir, s.Year, s.Month, ContributionsFile{GeneratedAt: now.Format(time.RFC3339), ChannelID: channelID, Messages: ms})
	}
	// latest/: the last contributionsLatestDays days, across months.
	cutoff := now.AddDate(0, 0, -contributionsLatestDays).Format(time.RFC3339)
	var recent []ContributionMessage
	first := now.AddDate(0, 0, -contributionsLatestDays)
	for d := time.Date(first.Year(), first.Month(), 1, 0, 0, 0, 0, time.UTC); !d.After(now); d = d.AddDate(0, 1, 0) {
		for _, msg := range monthContributions(dataDir, d.Format("2006"), d.Format("01"), channelID) {
			if msg.Timestamp >= cutoff {
				recent = append(recent, msg)
			}
		}
	}
	if recent == nil {
		recent = []ContributionMessage{}
	}
	sortContributionsNewestFirst(recent)
	writeContributions(dataDir, "latest", "", ContributionsFile{GeneratedAt: now.Format(time.RFC3339), ChannelID: channelID, Messages: recent})
	return len(recent)
}
