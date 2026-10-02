package cmd

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/CommonsHub/chb/providers/mobilizon"
)

// Which of our events go to Mobilizon, and how they look there. Everything
// here is offline: it reads the upcoming events from `chb calendars sync`,
// the group as last pulled, and what we published before, and writes the plan
// to providers/mobilizon/pending/events.json for `chb mobilizon push`.
//
// An event is identified by its Luma URL. It is also what Mobilizon sends
// people to for registering (joinOptions EXTERNAL), so an event posted by
// hand with the same Luma link is recognised as the same event.

// MobilizonPublished is what we remember about one event we pushed.
type MobilizonPublished struct {
	EventID     string `json:"eventId"`
	MobilizonID string `json:"mobilizonId"`
	UUID        string `json:"uuid"`
	Title       string `json:"title"`
	BeginsOn    string `json:"beginsOn"`
	Hash        string `json:"hash"`
	CoverImage  string `json:"coverImage,omitempty"`
	PictureUUID string `json:"pictureUuid,omitempty"`
	Draft       bool   `json:"draft,omitempty"`
	Cancelled   bool   `json:"cancelled,omitempty"`
}

// MobilizonPublishedFile is providers/mobilizon/published.json, keyed by Luma URL.
type MobilizonPublishedFile struct {
	Events map[string]MobilizonPublished `json:"events"`
}

// MobilizonAction is one step of the plan.
type MobilizonAction struct {
	Action      string             `json:"action"` // create | update | cancel
	Key         string             `json:"key"`    // the Luma URL
	EventID     string             `json:"eventId,omitempty"`
	MobilizonID string             `json:"mobilizonId,omitempty"`
	UUID        string             `json:"uuid,omitempty"`
	Reason      string             `json:"reason,omitempty"`
	Title       string             `json:"title"`
	Description string             `json:"description,omitempty"`
	BeginsOn    string             `json:"beginsOn"`
	EndsOn      string             `json:"endsOn,omitempty"`
	Address     *mobilizon.Address `json:"address,omitempty"`
	CoverImage  string             `json:"coverImage,omitempty"`
	Tags        []string           `json:"tags,omitempty"`
	Hash        string             `json:"hash,omitempty"`
}

// MobilizonSkip is an event we leave alone, with the reason.
type MobilizonSkip struct {
	Title    string `json:"title"`
	BeginsOn string `json:"beginsOn"`
	Reason   string `json:"reason"`
	UUID     string `json:"uuid,omitempty"`
}

// MobilizonPendingFile is providers/mobilizon/pending/events.json.
type MobilizonPendingFile struct {
	GeneratedAt string            `json:"generatedAt"`
	Instance    string            `json:"instance"`
	Group       string            `json:"group"`
	GroupID     string            `json:"groupId"`
	Actions     []MobilizonAction `json:"actions"`
	Skipped     []MobilizonSkip   `json:"skipped,omitempty"`
}

// MobilizonGenerate writes the plan for the next push.
func MobilizonGenerate(args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		printMobilizonHelp()
		return nil
	}
	pending, err := generateMobilizonPending(DataDir(), time.Now())
	if err != nil {
		return err
	}
	fmt.Printf("  %s✓%s Mobilizon: %s\n", Fmt.Green, Fmt.Reset, summarizeMobilizonPending(pending))
	if len(pending.Actions) > 0 {
		fmt.Printf("  %s↪ Next: chb mobilizon push --dry-run%s\n", Fmt.Dim, Fmt.Reset)
	}
	return nil
}

func summarizeMobilizonPending(p *MobilizonPendingFile) string {
	counts := map[string]int{}
	for _, a := range p.Actions {
		counts[a.Action]++
	}
	if len(p.Actions) == 0 {
		return "up to date"
	}
	var parts []string
	for _, action := range []string{"create", "update", "cancel"} {
		if counts[action] > 0 {
			parts = append(parts, fmt.Sprintf("%d to %s", counts[action], action))
		}
	}
	return strings.Join(parts, ", ")
}

func generateMobilizonPending(dataDir string, now time.Time) (*MobilizonPendingFile, error) {
	var remote MobilizonEventsFile
	if err := mobilizon.ReadJSON(mobilizon.EventsPath(dataDir), &remote); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("no Mobilizon data yet. Run `chb mobilizon pull` first")
		}
		return nil, err
	}
	published, err := loadMobilizonPublished(dataDir)
	if err != nil {
		return nil, err
	}
	events := loadUpcomingFullEvents(dataDir)
	pending := planMobilizon(events, remote.Group.Events, published.Events, mobilizonCoveredMonths(dataDir), now)
	pending.GeneratedAt = now.UTC().Format(time.RFC3339)
	pending.Instance = remote.Instance
	pending.Group = remote.Group.PreferredUsername
	pending.GroupID = remote.Group.ID
	return pending, mobilizon.WriteJSON(mobilizon.PendingPath(dataDir), pending)
}

func loadMobilizonPublished(dataDir string) (*MobilizonPublishedFile, error) {
	file := &MobilizonPublishedFile{}
	if err := mobilizon.ReadJSON(mobilizon.PublishedPath(dataDir), file); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if file.Events == nil {
		file.Events = map[string]MobilizonPublished{}
	}
	return file, nil
}

// mobilizonCoveredMonths lists the months ("2026-10") we have event data for.
// An event we published only counts as gone from Luma when its month was
// synced; otherwise a narrow `calendars sync` would cancel everything else.
func mobilizonCoveredMonths(dataDir string) map[string]bool {
	months := map[string]bool{}
	paths, _ := filepath.Glob(filepath.Join(dataDir, "[0-9][0-9][0-9][0-9]", "[0-9][0-9]", stewardsDirName, "events.json"))
	for _, p := range paths {
		monthDir := filepath.Dir(filepath.Dir(p))
		months[filepath.Base(filepath.Dir(monthDir))+"-"+filepath.Base(monthDir)] = true
	}
	return months
}

func planMobilizon(events []FullEvent, remote []mobilizon.Event, published map[string]MobilizonPublished, coveredMonths map[string]bool, now time.Time) *MobilizonPendingFile {
	pending := &MobilizonPendingFile{Actions: []MobilizonAction{}}
	remoteByURL := map[string]mobilizon.Event{}
	for _, e := range remote {
		if key := mobilizonEventKey(e.ExternalParticipationURL); key != "" {
			remoteByURL[key] = e
		}
	}

	wanted := map[string]bool{}
	for _, ev := range events {
		if !isLumaEvent(ev) || parseEventTimeBrussels(ev.StartAt).Before(now) {
			continue
		}
		action := mobilizonActionFor(ev)
		wanted[action.Key] = true

		if pub, ok := published[action.Key]; ok {
			if pub.Hash == action.Hash && !pub.Cancelled {
				continue
			}
			action.Action, action.MobilizonID, action.UUID = "update", pub.MobilizonID, pub.UUID
			action.Reason = "changed on Luma"
			if pub.Cancelled {
				action.Reason = "back on Luma"
			}
			pending.Actions = append(pending.Actions, action)
			continue
		}
		if e, ok := remoteByURL[action.Key]; ok {
			action.Action, action.MobilizonID, action.UUID = "update", e.ID, e.UUID
			action.Reason = "already on Mobilizon with this Luma link"
			pending.Actions = append(pending.Actions, action)
			continue
		}
		if e := findMobilizonLookalike(remote, action); e != nil {
			pending.Skipped = append(pending.Skipped, MobilizonSkip{
				Title: action.Title, BeginsOn: action.BeginsOn, UUID: e.UUID,
				Reason: fmt.Sprintf("posted by hand as %q", e.Title),
			})
			continue
		}
		action.Action = "create"
		pending.Actions = append(pending.Actions, action)
	}

	for key, pub := range published {
		if wanted[key] || pub.Cancelled {
			continue
		}
		begins, err := time.Parse(time.RFC3339, pub.BeginsOn)
		if err != nil || begins.Before(now) || !coveredMonths[begins.In(BrusselsTZ()).Format("2006-01")] {
			continue
		}
		pending.Actions = append(pending.Actions, MobilizonAction{
			Action: "cancel", Key: key, EventID: pub.EventID, MobilizonID: pub.MobilizonID, UUID: pub.UUID,
			Title: pub.Title, BeginsOn: pub.BeginsOn, Reason: "no longer on Luma",
		})
	}

	sort.SliceStable(pending.Actions, func(i, j int) bool { return pending.Actions[i].BeginsOn < pending.Actions[j].BeginsOn })
	return pending
}

func isLumaEvent(ev FullEvent) bool {
	return ev.CalendarSource == "luma" || mobilizonEventKey(ev.URL) != ""
}

// mobilizonEventKey normalises a Luma event URL (lu.ma or luma.com) to
// "https://luma.com/<slug>"; anything else yields "".
func mobilizonEventKey(raw string) string {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return ""
	}
	host := strings.TrimPrefix(strings.ToLower(u.Hostname()), "www.")
	if host != "luma.com" && host != "lu.ma" {
		return ""
	}
	slug := strings.Trim(u.Path, "/")
	if slug == "" || strings.Contains(slug, "/") {
		return ""
	}
	return "https://luma.com/" + slug
}

func mobilizonActionFor(ev FullEvent) MobilizonAction {
	key := mobilizonEventKey(ev.URL)
	a := MobilizonAction{
		Key:         key,
		EventID:     ev.ID,
		Title:       strings.TrimSpace(ev.Name),
		Description: mobilizonDescription(ev.Description, key),
		BeginsOn:    mobilizonTime(ev.StartAt),
		EndsOn:      mobilizonTime(ev.EndAt),
		Address:     mobilizonAddress(ev.Location),
		CoverImage:  ev.CoverImage,
		Tags:        mobilizonTags(ev.Tags),
	}
	a.Hash = hashMobilizonAction(a)
	return a
}

func mobilizonTime(value string) string {
	if value == "" {
		return ""
	}
	t := parseEventTimeBrussels(value)
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

// mobilizonDescription turns our plain-text description into the small HTML
// Mobilizon expects, and ends with the link to Luma. Descriptions from the
// calendar feed are often cut short ("…"), which the closing line accounts for.
func mobilizonDescription(text, lumaURL string) string {
	var b strings.Builder
	for _, para := range regexp.MustCompile(`\n\s*\n`).Split(strings.TrimSpace(text), -1) {
		if para = strings.TrimSpace(para); para == "" {
			continue
		}
		lines := strings.Split(para, "\n")
		for i := range lines {
			lines[i] = html.EscapeString(strings.TrimSpace(lines[i]))
		}
		b.WriteString("<p>" + strings.Join(lines, "<br>") + "</p>\n")
	}
	link := fmt.Sprintf(`<a href="%s">%s</a>`, html.EscapeString(lumaURL), html.EscapeString(lumaURL))
	if strings.HasSuffix(strings.TrimSpace(text), "…") || strings.HasSuffix(strings.TrimSpace(text), "...") {
		b.WriteString("<p>Full description and registration on Luma: " + link + "</p>")
	} else {
		b.WriteString("<p>Registration on Luma: " + link + "</p>")
	}
	return b.String()
}

var hubAddress = mobilizon.Address{
	Description: "Commons Hub Brussels",
	Street:      "51 Rue de la Madeleine - Magdalenasteenweg",
	PostalCode:  "1000",
	Locality:    "Brussels",
	Country:     "Belgium",
	Geom:        "4.3552039;50.8449817",
	Timezone:    "Europe/Brussels",
}

var postalLocality = regexp.MustCompile(`^(\d{4})\s+(.+)$`)

// mobilizonAddress maps a Luma location ("Place, Street 1, 1000 City,
// Country") to a Mobilizon address. The hub gets its exact address and map
// pin; elsewhere we fill what the text gives us.
func mobilizonAddress(location string) *mobilizon.Address {
	location = strings.TrimSpace(location)
	if location == "" {
		return nil
	}
	lower := strings.ToLower(location)
	if strings.Contains(lower, "madeleine 51") || strings.Contains(lower, "51 rue de la madeleine") {
		addr := hubAddress
		return &addr
	}
	parts := strings.Split(location, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	addr := &mobilizon.Address{Description: parts[0], Timezone: "Europe/Brussels"}
	for i, p := range parts[1:] {
		if m := postalLocality.FindStringSubmatch(p); m != nil {
			addr.PostalCode, addr.Locality = m[1], m[2]
			if rest := parts[i+2:]; len(rest) > 0 {
				addr.Country = rest[len(rest)-1]
			}
			break
		}
		if addr.Street == "" {
			addr.Street = p
		}
	}
	return addr
}

// mobilizonTags reads tag names from the event's tags, which come either as
// strings or as {name} objects.
func mobilizonTags(raw json.RawMessage) []string {
	if len(raw) == 0 {
		return nil
	}
	var names []string
	if json.Unmarshal(raw, &names) == nil {
		return names
	}
	var objects []struct {
		Name string `json:"name"`
	}
	if json.Unmarshal(raw, &objects) == nil {
		for _, o := range objects {
			if o.Name != "" {
				names = append(names, o.Name)
			}
		}
	}
	return names
}

func hashMobilizonAction(a MobilizonAction) string {
	data, _ := json.Marshal([]interface{}{a.Title, a.Description, a.BeginsOn, a.EndsOn, a.Address, a.CoverImage, a.Tags})
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:8])
}

var nonAlnum = regexp.MustCompile(`[^a-z0-9]+`)

func normaliseMobilizonTitle(s string) string {
	s = regexp.MustCompile(`\[[A-Za-z]{2}\]`).ReplaceAllString(strings.ToLower(s), "")
	return strings.TrimSpace(nonAlnum.ReplaceAllString(s, " "))
}

// findMobilizonLookalike finds a group event at the same time with a title
// that contains ours (or the other way round), e.g. one posted by hand.
func findMobilizonLookalike(remote []mobilizon.Event, a MobilizonAction) *mobilizon.Event {
	want := normaliseMobilizonTitle(a.Title)
	begins, err := time.Parse(time.RFC3339, a.BeginsOn)
	if err != nil || want == "" {
		return nil
	}
	for i, e := range remote {
		t, err := time.Parse(time.RFC3339, e.BeginsOn)
		if err != nil || !t.Equal(begins) {
			continue
		}
		have := normaliseMobilizonTitle(e.Title)
		if have != "" && (strings.Contains(have, want) || strings.Contains(want, have)) {
			return &remote[i]
		}
	}
	return nil
}
