package cmd

// #contributions and 💝praise as public feeds: YYYY/MM/<tier>/<file> and
// latest/<tier>/<file> (the last 60 days), the same in every tier. Who
// posted, who was mentioned (thanked), when, the message text, the
// reactions, and the public copies of the photos.
//
// contributions.json and praise.json share one shape and one generator
// (channelFeed). A feed is written only when its channel is one of the
// public channels (discord.publicChannels, images_public.go): Xavier
// decided the content of both channels is public. Bots are dropped. In
// the text, user/role/channel mentions and custom emoji are resolved to
// names; third-party IBANs are masked here and emails at write time.
// Identity is the Discord display identity contributors.json already
// publishes.

import (
	"encoding/json"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	discordsource "github.com/CommonsHub/chb/providers/discord"
)

const (
	contributionsFile = "contributions.json"
	praiseFile        = "praise.json"
)

// channelFeed is one public Discord channel feed.
type channelFeed struct {
	file string
	// channelKeys: settings discord.channels keys, first found wins.
	channelKeys []string
	// requireText drops messages without text (photo-only posts).
	requireText bool
}

var (
	contributionsFeed = channelFeed{file: contributionsFile, channelKeys: []string{"contributions"}}
	praiseFeed        = channelFeed{file: praiseFile, channelKeys: []string{"praise", "tokens"}, requireText: true}
)

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
	Content        string                 `json:"content"` // mentions and custom emoji resolved to names
	Mentions       []ContributionIdentity `json:"mentions"`
	TotalReactions int                    `json:"totalReactions"`
	Reactions      []ContributionReaction `json:"reactions"`
	Images         []string               `json:"images"` // public copies, relative to the data root
}

type ContributionIdentity struct {
	ID          string `json:"id"`
	DisplayName string `json:"displayName"`
	AvatarURL   string `json:"avatarUrl,omitempty"`
}

// ContributionReaction: Emoji is the unicode emoji, or ":name:" for a
// custom server emoji (then ImageURL is its picture on Discord's CDN).
type ContributionReaction struct {
	Emoji    string `json:"emoji"`
	Count    int    `json:"count"`
	ImageURL string `json:"imageUrl,omitempty"`
}

// discordSettings is the part of settings.json the feeds need.
type discordSettings struct {
	Discord struct {
		Channels json.RawMessage   `json:"channels"`
		Roles    map[string]string `json:"roles"`
	} `json:"discord"`
}

func loadDiscordSettings() discordSettings {
	var s discordSettings
	if data, err := os.ReadFile(settingsFilePath("settings.json")); err == nil {
		_ = json.Unmarshal(data, &s)
	}
	return s
}

// channelID returns the feed's channel id, or "" when it is not configured
// or not public.
func (f channelFeed) channelID() string {
	names := map[string]string{}
	flattenDiscordChannels(loadDiscordSettings().Discord.Channels, "", names)
	for _, k := range f.channelKeys {
		if id := names[k]; id != "" {
			if !publicPhotoChannelIDs()[id] {
				return ""
			}
			return id
		}
	}
	return ""
}

// contributionsChannelID returns the #contributions channel id, or "" when
// it is not configured or not public.
func contributionsChannelID() string { return contributionsFeed.channelID() }

var (
	discordUserMention    = regexp.MustCompile(`<@!?(\d+)>`)
	discordRoleMention    = regexp.MustCompile(`<@&(\d+)>`)
	discordChannelMention = regexp.MustCompile(`<#(\d+)>`)
	discordCustomEmoji    = regexp.MustCompile(`<a?:(\w+):\d+>`)
	discordTimestampTag   = regexp.MustCompile(`<t:(-?\d+)(?::[tTdDfFR])?>`)
)

// discordNames resolves role and channel ids to their settings names.
type discordNames struct {
	roles, channels map[string]string
}

func loadDiscordNames() discordNames {
	s := loadDiscordSettings()
	n := discordNames{roles: map[string]string{}, channels: map[string]string{}}
	for name, id := range s.Discord.Roles {
		n.roles[id] = name
	}
	flat := map[string]string{}
	flattenDiscordChannels(s.Discord.Channels, "", flat)
	for key, id := range flat {
		short := key[strings.LastIndex(key, ".")+1:]
		if cur, ok := n.channels[id]; !ok || short < cur {
			n.channels[id] = short
		}
	}
	return n
}

// resolveDiscordContent rewrites Discord markup to readable text:
// <@id> → @DisplayName, <@&id> → @role, <#id> → #channel,
// <:name:id> → :name:, <t:unix> → Brussels date and time. Third-party
// IBANs are masked.
func resolveDiscordContent(content string, mentions []discordsource.Author, names discordNames) string {
	users := map[string]string{}
	for _, u := range mentions {
		users[u.ID] = contributionIdentity(u).DisplayName
	}
	s := discordUserMention.ReplaceAllStringFunc(content, func(m string) string {
		if name := users[discordUserMention.FindStringSubmatch(m)[1]]; name != "" {
			return "@" + name
		}
		return "@someone"
	})
	s = discordRoleMention.ReplaceAllStringFunc(s, func(m string) string {
		if name := names.roles[discordRoleMention.FindStringSubmatch(m)[1]]; name != "" {
			return "@" + name
		}
		return "@role"
	})
	s = discordChannelMention.ReplaceAllStringFunc(s, func(m string) string {
		if name := names.channels[discordChannelMention.FindStringSubmatch(m)[1]]; name != "" {
			return "#" + name
		}
		return "#channel"
	})
	s = discordCustomEmoji.ReplaceAllString(s, ":$1:")
	s = discordTimestampTag.ReplaceAllStringFunc(s, func(m string) string {
		sec, err := strconv.ParseInt(discordTimestampTag.FindStringSubmatch(m)[1], 10, 64)
		if err != nil {
			return m
		}
		return time.Unix(sec, 0).In(BrusselsTZ()).Format("2006-01-02 15:04")
	})
	return maskBankDetails(s)
}

func contributionReactions(rs []discordsource.Reaction) []ContributionReaction {
	out := []ContributionReaction{}
	for _, r := range rs {
		if r.Count <= 0 {
			continue
		}
		cr := ContributionReaction{Emoji: r.Emoji.Name, Count: r.Count}
		if r.Emoji.ID != nil && *r.Emoji.ID != "" {
			cr.Emoji = ":" + r.Emoji.Name + ":"
			cr.ImageURL = "https://cdn.discordapp.com/emojis/" + *r.Emoji.ID + ".png"
		}
		out = append(out, cr)
	}
	return out
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
	return ContributionIdentity{ID: a.ID, DisplayName: name, AvatarURL: discordAvatarURL(a)}
}

// monthContributions reads the month's #contributions messages.
func monthContributions(dataDir, year, month, channelID string) []ContributionMessage {
	return contributionsFeed.monthMessages(dataDir, year, month, channelID, loadDiscordNames())
}

// monthMessages reads the month's messages of the feed's channel.
func (feed channelFeed) monthMessages(dataDir, year, month, channelID string, names discordNames) []ContributionMessage {
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
		if feed.requireText && strings.TrimSpace(m.Content) == "" {
			continue
		}
		msg := ContributionMessage{
			ID:        m.ID,
			Timestamp: normaliseDiscordTimestamp(m.Timestamp),
			Author:    contributionIdentity(m.Author),
			Content:   resolveDiscordContent(m.Content, m.Mentions, names),
			Mentions:  []ContributionIdentity{},
			Reactions: contributionReactions(m.Reactions),
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
	return contributionsFeed.write(dataDir, year, month, f)
}

func (feed channelFeed) write(dataDir, year, month string, f ContributionsFile) bool {
	if data, err := os.ReadFile(audiencePath(dataDir, year, month, AudienceStewards, feed.file)); err == nil {
		var prev ContributionsFile
		if json.Unmarshal(data, &prev) == nil && prev.ChannelID == f.ChannelID {
			a, _ := json.Marshal(prev.Messages)
			b, _ := json.Marshal(f.Messages)
			if string(a) == string(b) {
				return false
			}
		}
	}
	writeTiersNoMirror(dataDir, year, month, feed.file, func(a Audience) interface{} { return f })
	return true
}

// generateContributions writes the file of every month that has a
// #contributions archive (cheap, and unchanged months are not rewritten)
// and the latest window. Returns the number of messages in the latest
// window.
func generateContributions(dataDir string) int { return contributionsFeed.generate(dataDir) }

// generatePraise: the 💝praise feed (praise.json).
func generatePraise(dataDir string) int { return praiseFeed.generate(dataDir) }

func (feed channelFeed) generate(dataDir string) int {
	channelID := feed.channelID()
	if channelID == "" {
		return 0
	}
	names := loadDiscordNames()
	now := time.Now().UTC()
	for _, ym := range dataMonthRange(dataDir) {
		s := generateScope{Year: ym[:4], Month: ym[5:]}
		if _, err := os.Stat(discordsource.ChannelPath(dataDir, s.Year, s.Month, channelID)); err != nil {
			continue
		}
		ms := feed.monthMessages(dataDir, s.Year, s.Month, channelID, names)
		if ms == nil {
			ms = []ContributionMessage{}
		}
		sortContributionsNewestFirst(ms)
		feed.write(dataDir, s.Year, s.Month, ContributionsFile{GeneratedAt: now.Format(time.RFC3339), ChannelID: channelID, Messages: ms})
	}
	// latest/: the last contributionsLatestDays days, across months.
	cutoff := now.AddDate(0, 0, -contributionsLatestDays).Format(time.RFC3339)
	var recent []ContributionMessage
	first := now.AddDate(0, 0, -contributionsLatestDays)
	for d := time.Date(first.Year(), first.Month(), 1, 0, 0, 0, 0, time.UTC); !d.After(now); d = d.AddDate(0, 1, 0) {
		for _, msg := range feed.monthMessages(dataDir, d.Format("2006"), d.Format("01"), channelID, names) {
			if msg.Timestamp >= cutoff {
				recent = append(recent, msg)
			}
		}
	}
	if recent == nil {
		recent = []ContributionMessage{}
	}
	sortContributionsNewestFirst(recent)
	feed.write(dataDir, "latest", "", ContributionsFile{GeneratedAt: now.Format(time.RFC3339), ChannelID: channelID, Messages: recent})
	return len(recent)
}
