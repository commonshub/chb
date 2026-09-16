package cmd

// Door-access extraction: who opened the door, per month.
//
// The door server (github.com/commonshub/door) posts one Discord message per
// opening into the configured "door" channel. `chb messages sync` mirrors that
// channel under providers/discord/<channelID>/messages.json like any other
// channel; this generator reads the mirror and writes generated/door.json —
// per member: identity + on how many different days they opened the door.
//
// Message formats posted by the door server (server/routes/open/index.js):
//
//	🚪 Door opened by <@1234567890> via shortcut 📲          ← Discord mention
//	🚪 Door opened by alice via Citizen Wallet               ← CW username, no Discord id
//	🚪 Jane Doe opened the door for [Event](url) hosted by X ← guest via signed event link
//	🚪 Door opened using today's token                       ← anonymous, counted separately
//
// The mention variant carries the full user object in the message's mentions
// array (id, username, global_name, avatar), which is where identity comes
// from. The other variants only have a display name; they are still counted,
// keyed by that name, with an empty id.

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	discordsource "github.com/CommonsHub/chb/providers/discord"
)

const doorChannelSettingsKey = "door"

// DoorOpener is one person's door activity for a month.
type DoorOpener struct {
	ID       string   `json:"id,omitempty"`       // Discord user id ("" when only a name was posted)
	Username string   `json:"username,omitempty"` // Discord username, or the Citizen Wallet username
	Name     string   `json:"name"`               // display name (global_name > username > posted name)
	Avatar   string   `json:"avatar,omitempty"`   // CDN URL, when the Discord user has one
	Days     int      `json:"days"`               // number of DIFFERENT days with at least one opening
	Opens    int      `json:"opens"`              // total openings
	Dates    []string `json:"dates,omitempty"`    // the distinct days (YYYY-MM-DD, Brussels time)
	Via      []string `json:"via,omitempty"`      // access methods seen (shortcut, citizenwallet, event)
}

// DoorMonthFile is generated/door.json.
type DoorMonthFile struct {
	Month       string       `json:"month"`
	GeneratedAt string       `json:"generatedAt"`
	ChannelID   string       `json:"channelId"`
	Openers     []DoorOpener `json:"openers"`
	TokenOpens  int          `json:"tokenOpens,omitempty"` // anonymous "today's token" openings
	TotalOpens  int          `json:"totalOpens"`
}

var (
	doorMentionRe = regexp.MustCompile(`<@!?(\d+)>`)
	// "🚪 Door opened by alice via Citizen Wallet"
	doorCitizenWalletRe = regexp.MustCompile(`^🚪 Door opened by (.+?) via Citizen Wallet`)
	// "🚪 Jane Doe opened the door for [Event](<url>) hosted by Host"
	doorEventGuestRe = regexp.MustCompile(`^🚪 (.+?) opened the door for `)
)

// doorChannelID returns the configured door channel, "" when not set up.
func doorChannelID(settings *Settings) string {
	if settings == nil {
		return ""
	}
	return GetDiscordChannelIDs(settings)[doorChannelSettingsKey]
}

// generateMonthDoorGo writes generated/door.json for one month. Returns true
// when a file was written (i.e. the door channel is configured and mirrored).
func generateMonthDoorGo(dataDir, year, month string, settings *Settings) bool {
	channelID := doorChannelID(settings)
	if channelID == "" {
		return false
	}
	raws := readChannelMessages(dataDir, year, month, channelID)
	if raws == nil {
		return false
	}

	type agg struct {
		opener DoorOpener
		days   map[string]bool
		via    map[string]bool
	}
	byKey := map[string]*agg{}
	tokenOpens, totalOpens := 0, 0

	for _, raw := range raws {
		var msg discordsource.Message
		if json.Unmarshal(raw, &msg) != nil {
			continue
		}
		content := strings.TrimSpace(msg.Content)
		if !strings.HasPrefix(content, "🚪") {
			continue
		}

		// Anonymous token opening: counted, but there is no person to credit.
		if strings.Contains(content, "using today's token") {
			tokenOpens++
			totalOpens++
			continue
		}

		key, opener, via := doorOpenerIdentity(content, msg.Mentions)
		if key == "" {
			continue
		}
		totalOpens++

		day := doorMessageDay(msg.Timestamp)
		a := byKey[key]
		if a == nil {
			a = &agg{opener: opener, days: map[string]bool{}, via: map[string]bool{}}
			byKey[key] = a
		}
		// A Discord identity is richer than a posted name — upgrade in place
		// if a later message carries it for the same key.
		if a.opener.ID == "" && opener.ID != "" {
			a.opener = opener
		}
		a.opener.Opens++
		if day != "" {
			a.days[day] = true
		}
		if via != "" {
			a.via[via] = true
		}
	}

	if len(byKey) == 0 && tokenOpens == 0 {
		return false
	}

	openers := make([]DoorOpener, 0, len(byKey))
	for _, a := range byKey {
		o := a.opener
		o.Days = len(a.days)
		o.Dates = sortedKeys(a.days)
		o.Via = sortedKeys(a.via)
		openers = append(openers, o)
	}
	sort.Slice(openers, func(i, j int) bool {
		if openers[i].Days != openers[j].Days {
			return openers[i].Days > openers[j].Days
		}
		if openers[i].Opens != openers[j].Opens {
			return openers[i].Opens > openers[j].Opens
		}
		return strings.ToLower(openers[i].Name) < strings.ToLower(openers[j].Name)
	})

	out := DoorMonthFile{
		Month:       fmt.Sprintf("%s-%s", year, month),
		GeneratedAt: time.Now().In(BrusselsTZ()).Format(time.RFC3339),
		ChannelID:   channelID,
		Openers:     openers,
		TokenOpens:  tokenOpens,
		TotalOpens:  totalOpens,
	}
	data, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return false
	}
	writeMonthFile(dataDir, year, month, filepath.Join("generated", "door.json"), data)

	// Audience tiers: who opened the door is presence data. The public tier
	// gets counts only; members see who (identity, days, opens); stewards
	// additionally get the exact dates.
	for _, a := range Audiences {
		tiered, err := json.MarshalIndent(doorFileForAudience(out, a), "", "  ")
		if err != nil {
			continue
		}
		if err := writeAudienceFile(dataDir, year, month, a, "door.json", tiered); err != nil {
			Warnf("  %s⚠ %s%s", Fmt.Yellow, err, Fmt.Reset)
		}
	}
	return true
}

// DoorPublicFile is the public projection of a month of door openings:
// aggregate counts, no persons.
type DoorPublicFile struct {
	Month       string `json:"month"`
	GeneratedAt string `json:"generatedAt"`
	Openers     int    `json:"openers"`
	OpenDays    int    `json:"openDays"`
	TokenOpens  int    `json:"tokenOpens"`
	TotalOpens  int    `json:"totalOpens"`
}

// doorFileForAudience projects the full door file down to a tier.
func doorFileForAudience(full DoorMonthFile, a Audience) interface{} {
	switch a {
	case AudienceStewards:
		return full
	case AudienceMembers:
		openers := make([]DoorOpener, len(full.Openers))
		for i, o := range full.Openers {
			o.Dates = nil
			openers[i] = o
		}
		out := full
		out.Openers = openers
		return out
	default:
		days := map[string]bool{}
		for _, o := range full.Openers {
			for _, d := range o.Dates {
				days[d] = true
			}
		}
		return DoorPublicFile{
			Month:       full.Month,
			GeneratedAt: full.GeneratedAt,
			Openers:     len(full.Openers),
			OpenDays:    len(days),
			TokenOpens:  full.TokenOpens,
			TotalOpens:  full.TotalOpens,
		}
	}
}

// doorOpenerIdentity resolves who a door message credits.
// Returns a stable aggregation key, the identity, and the access method.
func doorOpenerIdentity(content string, mentions []discordsource.Author) (string, DoorOpener, string) {
	// 1. Discord mention — the mentions array carries the full user object.
	if len(mentions) > 0 {
		u := mentions[0]
		return "id:" + u.ID, DoorOpener{
			ID:       u.ID,
			Username: u.Username,
			Name:     discordDisplayName(u),
			Avatar:   discordAvatarURL(u),
		}, "shortcut"
	}
	// A raw <@id> whose user object was not resolved: keep the id at least.
	if m := doorMentionRe.FindStringSubmatch(content); m != nil {
		return "id:" + m[1], DoorOpener{ID: m[1], Name: "user " + m[1]}, "shortcut"
	}
	// 2. Citizen Wallet username.
	if m := doorCitizenWalletRe.FindStringSubmatch(content); m != nil {
		name := strings.TrimSpace(m[1])
		return "cw:" + strings.ToLower(name), DoorOpener{Username: name, Name: name}, "citizenwallet"
	}
	// 3. Guest via a signed event link: "<name> opened the door for <event>".
	if m := doorEventGuestRe.FindStringSubmatch(content); m != nil {
		name := strings.TrimSpace(m[1])
		if strings.EqualFold(name, "Door opened") { // defensive: never a person
			return "", DoorOpener{}, ""
		}
		return "guest:" + strings.ToLower(name), DoorOpener{Name: name}, "event"
	}
	return "", DoorOpener{}, ""
}

// doorMessageDay converts a Discord message timestamp to the Brussels
// calendar day it belongs to. "Different days" only makes sense in the
// timezone of the door.
func doorMessageDay(ts string) string {
	t, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		return ""
	}
	return t.In(BrusselsTZ()).Format("2006-01-02")
}

func discordDisplayName(u discordsource.Author) string {
	if u.GlobalName != nil && strings.TrimSpace(*u.GlobalName) != "" {
		return *u.GlobalName
	}
	return u.Username
}

// discordAvatarURL builds the CDN URL for a user's avatar — same formula the
// door server uses, so the two stay in agreement.
func discordAvatarURL(u discordsource.Author) string {
	if u.Avatar == nil || *u.Avatar == "" {
		return ""
	}
	return fmt.Sprintf("https://cdn.discordapp.com/avatars/%s/%s.png?size=128", u.ID, *u.Avatar)
}
