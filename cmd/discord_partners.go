package cmd

// Discord members ↔ Odoo partners.
//
// Every Discord member with the member role (settings discord.roles.member)
// gets an Odoo partner carrying their Discord id (res.partner x_discord_id),
// so chb knows who they are and the website can ask those without an email
// for one.
//
//   - `chb discord partners pull` (network, read-only): the guild members
//     (latest/providers/discord/guild-members.json), the website's
//     emailHash → Discord id links and the emails members submitted on the
//     website (Nostr kind 30078 #t partner-email, NIP-44 to chb's
//     partner-email key, signed by the website; decrypted into
//     latest/providers/members/partner-emails.json, 0600).
//   - `chb generate` (offline) plans the Odoo changes
//     (latest/providers/odoo/<db>/pending/discord-partners.json, 0600) and
//     writes latest/members/discord-partners.json ({generatedAt, needsEmail})
//     for the website.
//   - `chb discord partners push` previews the plan; --yes applies it.
//
// Linking order for a Discord member: a partner already carrying that
// x_discord_id; the stewards' map settings/members-discord.json
// ("odoo:<partner id>" → Discord id); the website's email-hash link (exactly
// one active partner whose email hashes to it); the verified email they gave
// on the website, when exactly one partner has it. A partner whose name matches
// the member's Discord name is never linked automatically: it is listed for
// review and no partner is created. Otherwise a partner is created (no
// company, tagged "Discord", the Discord display name, no email).
//
// On existing partners chb writes only x_discord_id. An email submitted on
// the website is set only on a partner chb created (tagged "Discord") whose
// email is empty; anything else is listed for review, never overwritten.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/nbd-wtf/go-nostr/nip44"

	discordsource "github.com/CommonsHub/chb/providers/discord"
	odoosource "github.com/CommonsHub/chb/providers/odoo"
)

const (
	discordPartnerField    = "x_discord_id"
	discordPartnerTag      = "Discord"
	discordPartnersMaxRun  = 25
	partnerEmailEventKind  = 30078
	partnerEmailTag        = "partner-email"
	guildMembersFile       = "guild-members.json"
	discordPartnersRelFile = "discord-partners.json"
)

// websiteNostrPubkey: the commonshub.brussels site key; partner-email
// events signed by anyone else are ignored.
var websiteNostrPubkey = "727bdf54ac689a75cf875446dd242091d6ebfc199bd58fce1c668f8405183492"

// ── pulled data ─────────────────────────────────────────────────────────

type GuildMember struct {
	ID          string   `json:"id"`
	Username    string   `json:"username"`
	GlobalName  string   `json:"globalName,omitempty"`
	Nick        string   `json:"nick,omitempty"`
	Roles       []string `json:"roles"`
	Bot         bool     `json:"bot,omitempty"`
	JoinedAt    string   `json:"joinedAt,omitempty"`
	MemberRole  bool     `json:"memberRole"`
	DisplayName string   `json:"displayName"`
}

type GuildMembersFile struct {
	FetchedAt    string        `json:"fetchedAt"`
	GuildID      string        `json:"guildId"`
	MemberRoleID string        `json:"memberRoleId"`
	Members      []GuildMember `json:"members"`
}

type websiteDiscordLink struct {
	DiscordID string `json:"discordId"`
	EmailHash string `json:"emailHash"`
	// login, email (address confirmed with a code) or name (the member
	// confirmed a name match: weaker, never written to Odoo unreviewed).
	Source string `json:"source,omitempty"`
}

type websiteDiscordLinksFile struct {
	FetchedAt string               `json:"fetchedAt"`
	Links     []websiteDiscordLink `json:"links"`
}

// PartnerEmailSubmission: one email a member gave on the website.
type PartnerEmailSubmission struct {
	DiscordID  string `json:"discordId"`
	Email      string `json:"email"`
	VerifiedAt string `json:"verifiedAt,omitempty"`
	EventID    string `json:"eventId"`
	CreatedAt  int64  `json:"createdAt"`
}

func discordLinksPath(dataDir string) string {
	return filepath.Join(remindersDir(dataDir), "discord-links.json")
}

func partnerEmailsPath(dataDir string) string {
	return filepath.Join(remindersDir(dataDir), "partner-emails.json")
}

func discordPartnersPlanPath(dataDir string) string {
	return odoosource.Path(dataDir, "latest", "", "pending", discordPartnersRelFile)
}

func displayNameOf(m GuildMember) string {
	for _, s := range []string{m.Nick, m.GlobalName, m.Username} {
		if s = strings.TrimSpace(s); s != "" {
			return s
		}
	}
	return m.ID
}

// fetchGuildMembers lists every guild member (paged by 1000).
func fetchGuildMembers(token, guildID, memberRoleID string) ([]GuildMember, error) {
	pacer := &discordPacer{}
	var out []GuildMember
	after := "0"
	for page := 0; page < 100; page++ {
		u := fmt.Sprintf("https://discord.com/api/v10/guilds/%s/members?limit=1000&after=%s", guildID, after)
		resp, err := pacer.get(u, token)
		if err != nil {
			return nil, err
		}
		var rows []struct {
			Nick     string   `json:"nick"`
			Roles    []string `json:"roles"`
			JoinedAt string   `json:"joined_at"`
			User     struct {
				ID         string `json:"id"`
				Username   string `json:"username"`
				GlobalName string `json:"global_name"`
				Bot        bool   `json:"bot"`
			} `json:"user"`
		}
		err = json.NewDecoder(resp.Body).Decode(&rows)
		status := resp.StatusCode
		resp.Body.Close()
		if status >= 300 {
			return nil, fmt.Errorf("guild members: HTTP %d", status)
		}
		if err != nil {
			return nil, err
		}
		for _, r := range rows {
			m := GuildMember{ID: r.User.ID, Username: r.User.Username, GlobalName: r.User.GlobalName, Nick: r.Nick,
				Roles: r.Roles, Bot: r.User.Bot, JoinedAt: r.JoinedAt}
			for _, role := range r.Roles {
				if role == memberRoleID {
					m.MemberRole = true
				}
			}
			m.DisplayName = displayNameOf(m)
			out = append(out, m)
			after = r.User.ID
		}
		if len(rows) < 1000 {
			break
		}
	}
	sort.Slice(out, func(i, j int) bool { return snowflakeLess(out[i].ID, out[j].ID) })
	return out, nil
}

// ── partner-email key ───────────────────────────────────────────────────

func partnerEmailKeyPath() string {
	return filepath.Join(AppDataDir(), "keys", "partner-email.json")
}

type partnerEmailKey struct {
	PrivHex string `json:"privHex"`
	PubHex  string `json:"pubHex"`
}

func loadPartnerEmailKey() (*partnerEmailKey, error) {
	data, err := os.ReadFile(partnerEmailKeyPath())
	if err != nil {
		return nil, err
	}
	var k partnerEmailKey
	if err := json.Unmarshal(data, &k); err != nil || k.PrivHex == "" {
		return nil, fmt.Errorf("invalid %s", partnerEmailKeyPath())
	}
	return &k, nil
}

// ensurePartnerEmailKey creates the decryption-only key on first use. Its
// secret never leaves the host and is never printed.
func ensurePartnerEmailKey() (*partnerEmailKey, bool, error) {
	if k, err := loadPartnerEmailKey(); err == nil {
		return k, false, nil
	}
	sk := nostr.GeneratePrivateKey()
	pk, err := nostr.GetPublicKey(sk)
	if err != nil {
		return nil, false, err
	}
	k := &partnerEmailKey{PrivHex: sk, PubHex: pk}
	if err := os.MkdirAll(filepath.Dir(partnerEmailKeyPath()), 0o700); err != nil {
		return nil, false, err
	}
	data, _ := json.MarshalIndent(k, "", "  ")
	if err := os.WriteFile(partnerEmailKeyPath(), data, 0o600); err != nil {
		return nil, false, err
	}
	return k, true, nil
}

func nostrTagValue(ev NostrEvent, name string) string {
	for _, t := range ev.Tags {
		if len(t) >= 2 && t[0] == name {
			return t[1]
		}
	}
	return ""
}

// decryptPartnerEmail opens one website submission: NIP-44 with the
// conversation key of chb's secret and the event's ephemeral pubkey (epk).
func decryptPartnerEmail(ev NostrEvent, k *partnerEmailKey) (PartnerEmailSubmission, error) {
	var sub PartnerEmailSubmission
	if strings.ToLower(ev.PubKey) != websiteNostrPubkey || ev.Kind != partnerEmailEventKind || !nostrEventSignatureValid(ev) {
		return sub, fmt.Errorf("not a signed website event")
	}
	epk := ""
	tagged := false
	for _, t := range ev.Tags {
		if len(t) >= 2 && t[0] == "epk" {
			epk = t[1]
		}
		if len(t) >= 2 && t[0] == "t" && t[1] == partnerEmailTag {
			tagged = true
		}
	}
	if !tagged || epk == "" || nostrTagValue(ev, "p") != k.PubHex {
		return sub, fmt.Errorf("missing t/epk tag")
	}
	ck, err := nip44.GenerateConversationKey(epk, k.PrivHex)
	if err != nil {
		return sub, err
	}
	plain, err := nip44.Decrypt(ev.Content, ck)
	if err != nil {
		return sub, err
	}
	if err := json.Unmarshal([]byte(plain), &sub); err != nil {
		return sub, err
	}
	sub.Email = strings.TrimSpace(sub.Email)
	if !discordIDPattern.MatchString(sub.DiscordID) || !strings.Contains(sub.Email, "@") {
		return sub, fmt.Errorf("invalid submission")
	}
	sub.EventID, sub.CreatedAt = ev.ID, ev.CreatedAt
	return sub, nil
}

// ── plan ────────────────────────────────────────────────────────────────

type DiscordPartnerAction struct {
	Op          string `json:"op"` // link, create, set_email
	DiscordID   string `json:"discordId"`
	DisplayName string `json:"displayName"`
	PartnerID   int    `json:"partnerId,omitempty"`
	Via         string `json:"via,omitempty"` // hand, emailHash
	Email       string `json:"email,omitempty"`
}

type DiscordPartnerReview struct {
	DiscordID   string `json:"discordId"`
	DisplayName string `json:"displayName"`
	Reason      string `json:"reason"`
	PartnerIDs  []int  `json:"partnerIds,omitempty"`
}

type DiscordPartnersPlan struct {
	GeneratedAt string                 `json:"generatedAt"`
	Members     int                    `json:"members"` // with the member role
	Linked      int                    `json:"linked"`  // already carrying x_discord_id
	Actions     []DiscordPartnerAction `json:"actions"`
	Review      []DiscordPartnerReview `json:"review"`
	NeedsEmail  []string               `json:"needsEmail"`
}

func (p DiscordPartnersPlan) count(op string) int {
	n := 0
	for _, a := range p.Actions {
		if a.Op == op {
			n++
		}
	}
	return n
}

type discordPartnerInputs struct {
	Members   []GuildMember
	Partners  []OdooPartner
	TagID     int               // the "Discord" partner category, 0 if absent
	Hand      map[string]string // "odoo:<id>" → Discord id
	HashLinks map[string]string // emailHash → Discord id (unambiguous)
	NameLinks map[string]string // same, from website name matches: review only
	Salt      string
	Emails    map[string]PartnerEmailSubmission // Discord id → newest submission
}

func buildDiscordPartnersPlan(in discordPartnerInputs) DiscordPartnersPlan {
	plan := DiscordPartnersPlan{Actions: []DiscordPartnerAction{}, Review: []DiscordPartnerReview{}, NeedsEmail: []string{}}
	byDiscord := map[string][]OdooPartner{}
	byID := map[int]OdooPartner{}
	byHash := map[string][]OdooPartner{}
	byName := map[string][]OdooPartner{}
	partnersByEmail := map[string][]int{}
	for _, p := range in.Partners {
		if !p.Active {
			continue
		}
		byID[p.ID] = p
		if p.DiscordID != "" {
			byDiscord[p.DiscordID] = append(byDiscord[p.DiscordID], p)
		}
		if em := strings.ToLower(strings.TrimSpace(p.Email)); em != "" {
			partnersByEmail[em] = append(partnersByEmail[em], p.ID)
			if in.Salt != "" {
				h := hashEmail(em, in.Salt)
				byHash[h] = append(byHash[h], p)
			}
		}
		if n := normName(p.Name); n != "" {
			byName[n] = append(byName[n], p)
		}
	}
	handByDiscord := map[string][]int{}
	for k, d := range in.Hand {
		if id, err := strconv.Atoi(strings.TrimPrefix(k, "odoo:")); err == nil && strings.HasPrefix(k, "odoo:") {
			handByDiscord[d] = append(handByDiscord[d], id)
		}
	}
	hashesByDiscord := map[string][]string{}
	for h, d := range in.HashLinks {
		hashesByDiscord[d] = append(hashesByDiscord[d], h)
	}
	ids := func(ps []OdooPartner) []int {
		var out []int
		for _, p := range ps {
			out = append(out, p.ID)
		}
		sort.Ints(out)
		return out
	}
	tagged := func(p OdooPartner) bool {
		for _, c := range p.CategoryIDs {
			if in.TagID > 0 && c == in.TagID {
				return true
			}
		}
		return false
	}
	review := func(m GuildMember, reason string, pids []int) {
		plan.Review = append(plan.Review, DiscordPartnerReview{DiscordID: m.ID, DisplayName: m.DisplayName, Reason: reason, PartnerIDs: pids})
	}

	for _, m := range in.Members {
		if !m.MemberRole || m.Bot {
			continue
		}
		plan.Members++
		sub, hasSub := in.Emails[m.ID]
		var partner *OdooPartner

		switch linked := byDiscord[m.ID]; {
		case len(linked) > 1:
			review(m, "several partners carry this Discord id", ids(linked))
			continue
		case len(linked) == 1:
			plan.Linked++
			partner = &linked[0]
		}

		if partner == nil {
			var cand *OdooPartner
			via := ""
			if hp := handByDiscord[m.ID]; len(hp) == 1 {
				if p, ok := byID[hp[0]]; ok {
					cand, via = &p, "hand"
				} else {
					review(m, "members-discord.json names an unknown or archived partner", hp)
					continue
				}
			} else if len(hp) > 1 {
				review(m, "members-discord.json maps several partners to this Discord id", hp)
				continue
			}
			if cand == nil {
				var hits []OdooPartner
				seen := map[int]bool{}
				for _, h := range hashesByDiscord[m.ID] {
					for _, p := range byHash[h] {
						if !seen[p.ID] {
							seen[p.ID] = true
							hits = append(hits, p)
						}
					}
				}
				if len(hits) > 1 {
					review(m, "the Discord email matches several partners", ids(hits))
					continue
				}
				if len(hits) == 1 {
					cand, via = &hits[0], "emailHash"
				}
				if cand == nil {
					var named []int
					for h, d := range in.NameLinks {
						if d == m.ID {
							for _, p := range byHash[h] {
								named = append(named, p.ID)
							}
						}
					}
					if len(named) > 0 {
						sort.Ints(named)
						review(m, "matched by name on the website: link by hand in members-discord.json if it is them", named)
						continue
					}
				}
			}
			if cand != nil {
				if cand.DiscordID != "" && cand.DiscordID != m.ID {
					review(m, "the partner already carries another Discord id", []int{cand.ID})
					continue
				}
				plan.Actions = append(plan.Actions, DiscordPartnerAction{Op: "link", DiscordID: m.ID, DisplayName: m.DisplayName, PartnerID: cand.ID, Via: via})
				partner = cand
			}
		}

		// A verified email the member gave on the website that belongs to
		// exactly one partner links them (stronger than any name match).
		if partner == nil && hasSub {
			if owners := partnersByEmail[strings.ToLower(sub.Email)]; len(owners) == 1 {
				p := byID[owners[0]]
				if p.DiscordID != "" && p.DiscordID != m.ID {
					review(m, "the email they gave belongs to a partner with another Discord id", owners)
					continue
				}
				plan.Actions = append(plan.Actions, DiscordPartnerAction{Op: "link", DiscordID: m.ID, DisplayName: m.DisplayName, PartnerID: p.ID, Via: "email"})
				continue
			} else if len(owners) > 1 {
				review(m, "the email they gave belongs to several partners", owners)
				continue
			}
		}

		if partner == nil {
			if same := byName[normName(m.DisplayName)]; len(same) > 0 {
				review(m, "a partner has the same name: link by hand in members-discord.json if it is them", ids(same))
				continue
			}
			act := DiscordPartnerAction{Op: "create", DiscordID: m.ID, DisplayName: m.DisplayName}
			if hasSub {
				act.Email = sub.Email // matches no partner (checked above)
			} else {
				plan.NeedsEmail = append(plan.NeedsEmail, m.ID)
			}
			plan.Actions = append(plan.Actions, act)
			continue
		}

		if strings.TrimSpace(partner.Email) != "" {
			continue
		}
		if !hasSub {
			plan.NeedsEmail = append(plan.NeedsEmail, m.ID)
			continue
		}
		if others := partnersByEmail[strings.ToLower(sub.Email)]; len(others) > 0 {
			review(m, "the email they gave belongs to another partner (probable duplicate)", append([]int{partner.ID}, others...))
			continue
		}
		if !tagged(*partner) {
			review(m, "email given on the website; partner predates chb's Discord sync: set it by hand", []int{partner.ID})
			continue
		}
		plan.Actions = append(plan.Actions, DiscordPartnerAction{Op: "set_email", DiscordID: m.ID, DisplayName: m.DisplayName, PartnerID: partner.ID, Email: sub.Email})
	}
	sort.Strings(plan.NeedsEmail)
	return plan
}

// ── generate ────────────────────────────────────────────────────────────

func loadGuildMembers(dataDir string) (GuildMembersFile, bool) {
	var f GuildMembersFile
	data, err := os.ReadFile(discordsource.Path(dataDir, "latest", "", guildMembersFile))
	if err != nil || json.Unmarshal(data, &f) != nil {
		return f, false
	}
	return f, true
}

// loadWebsiteHashLinks splits the website's links: login and email links
// may link a partner, name links only put the member up for review.
func loadWebsiteHashLinks(dataDir string) (hash, name map[string]string) {
	var f websiteDiscordLinksFile
	if data, err := os.ReadFile(discordLinksPath(dataDir)); err == nil {
		_ = json.Unmarshal(data, &f)
	}
	var strong, weak []websiteDiscordLink
	for _, l := range f.Links {
		if l.Source == "name" {
			weak = append(weak, l)
		} else {
			strong = append(strong, l)
		}
	}
	return hashDiscordLinks(strong), hashDiscordLinks(weak)
}

func loadPartnerEmailSubmissions(dataDir string) map[string]PartnerEmailSubmission {
	out := map[string]PartnerEmailSubmission{}
	if data, err := os.ReadFile(partnerEmailsPath(dataDir)); err == nil {
		_ = json.Unmarshal(data, &out)
	}
	return out
}

func discordPartnerTagID(f OdooPartnersFile) int {
	for _, c := range f.Categories {
		if c.Name == discordPartnerTag {
			return c.ID
		}
	}
	return 0
}

// generateDiscordPartners writes the pending plan and needsEmail. Returns
// the step's summary.
func generateDiscordPartners(dataDir string) string {
	guild, ok := loadGuildMembers(dataDir)
	if !ok {
		return "no guild members (chb discord partners pull)"
	}
	pf, ok := loadLatestOdooPartnersFile(dataDir)
	if !ok {
		return "no Odoo partners"
	}
	if !pf.DiscordField {
		// Without x_discord_id nobody can be linked yet: asking everyone
		// for an email would ask members Odoo already knows. Publish an
		// empty needsEmail until the field exists.
		writeNeedsEmail(dataDir, time.Now().UTC().Format(time.RFC3339), []string{})
		return "Odoo has no " + discordPartnerField + " field yet (chb discord partners setup)"
	}
	plan := computeDiscordPartnersPlan(dataDir, guild, pf)
	if err := writePrivateJSON(discordPartnersPlanPath(dataDir), plan); err != nil {
		Warnf("⚠ discord partners plan: %v", err)
	}
	writeNeedsEmail(dataDir, plan.GeneratedAt, plan.NeedsEmail)
	return fmt.Sprintf("%d members, %d linked, %d to link, %d to create, %d emails to set, %d to review, %d need an email",
		plan.Members, plan.Linked, plan.count("link"), plan.count("create"), plan.count("set_email"), len(plan.Review), len(plan.NeedsEmail))
}

func computeDiscordPartnersPlan(dataDir string, guild GuildMembersFile, pf OdooPartnersFile) DiscordPartnersPlan {
	hashLinks, nameLinks := loadWebsiteHashLinks(dataDir)
	plan := buildDiscordPartnersPlan(discordPartnerInputs{
		Members: guild.Members, Partners: pf.Partners, TagID: discordPartnerTagID(pf),
		Hand: loadRecapDiscordLinks(), HashLinks: hashLinks, NameLinks: nameLinks,
		Salt: os.Getenv("EMAIL_HASH_SALT"), Emails: loadPartnerEmailSubmissions(dataDir),
	})
	plan.GeneratedAt = time.Now().UTC().Format(time.RFC3339)
	return plan
}

// writeNeedsEmail: latest/members/discord-partners.json for the website
// (Discord ids only).
func writeNeedsEmail(dataDir, generatedAt string, ids []string) {
	needs, _ := json.MarshalIndent(map[string]interface{}{"generatedAt": generatedAt, "needsEmail": ids}, "", "  ")
	if err := writeAudienceFile(dataDir, "latest", "", AudienceMembers, discordPartnersRelFile, needs); err != nil {
		Warnf("⚠ %s: %v", discordPartnersRelFile, err)
	}
}

// ── commands ────────────────────────────────────────────────────────────

// DiscordPartners is `chb discord partners [pull|push|key]`.
func DiscordPartners(args []string) error {
	sub := ""
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		sub, args = args[0], args[1:]
	}
	if HasFlag(args, "--help", "-h") || sub == "help" {
		fmt.Print(`
chb discord partners — Discord members ↔ Odoo partners (x_discord_id)

USAGE
  chb discord partners              Show the plan (written by chb generate)
  chb discord partners pull         Fetch guild members, the website's Discord
                                    links and the emails members gave (read-only)
  chb discord partners push         Preview the Odoo changes
  chb discord partners push --yes   Apply them (at most ` + strconv.Itoa(discordPartnersMaxRun) + ` per run; --limit N)
  chb discord partners setup [--yes]
                                    Create the x_discord_id field and the
                                    "Discord" partner tag in Odoo
  chb discord partners key          Print chb's partner-email Nostr pubkey
                                    (created on first use; the secret stays here)
`)
		return nil
	}
	switch sub {
	case "pull":
		return discordPartnersPull()
	case "push":
		return discordPartnersPush(args)
	case "setup":
		return discordPartnersSetup(args)
	case "key":
		k, created, err := ensurePartnerEmailKey()
		if err != nil {
			return err
		}
		fmt.Printf("  partner-email pubkey: %s%s\n", k.PubHex, map[bool]string{true: " (new)"}[created])
		return nil
	case "":
		// Offline preview, computed in memory (also before the Odoo
		// field exists, to size the change).
		dataDir := DataDir()
		guild, ok := loadGuildMembers(dataDir)
		if !ok {
			return fmt.Errorf("no guild members — run `chb discord partners pull`")
		}
		pf, ok := loadLatestOdooPartnersFile(dataDir)
		if !ok {
			return fmt.Errorf("no Odoo partners — run `chb odoo pull`")
		}
		if !pf.DiscordField {
			fmt.Printf("\n  %s(Odoo has no %s field yet: nobody counts as linked; preview only)%s\n", Fmt.Dim, discordPartnerField, Fmt.Reset)
		}
		printDiscordPartnersPlan(computeDiscordPartnersPlan(dataDir, guild, pf), !HasFlag(args, "--verbose", "-v"))
		return nil
	}
	return fmt.Errorf("unknown subcommand %q (chb discord partners --help)", sub)
}

func discordPartnersPull() error {
	dataDir := DataDir()
	settings, err := LoadSettings()
	if err != nil {
		return err
	}
	token := os.Getenv("DISCORD_BOT_TOKEN")
	roleID := settings.Discord.Roles["member"]
	if token == "" || settings.Discord.GuildID == "" || roleID == "" {
		return fmt.Errorf("needs DISCORD_BOT_TOKEN, discord.guildId and discord.roles.member")
	}
	members, err := fetchGuildMembers(token, settings.Discord.GuildID, roleID)
	if err != nil {
		return err
	}
	f := GuildMembersFile{FetchedAt: time.Now().UTC().Format(time.RFC3339), GuildID: settings.Discord.GuildID, MemberRoleID: roleID, Members: members}
	if err := discordsource.WriteJSON(dataDir, "latest", "", f, guildMembersFile); err != nil {
		return err
	}
	withRole := 0
	for _, m := range members {
		if m.MemberRole && !m.Bot {
			withRole++
		}
	}
	fmt.Printf("  ✓ guild: %d members, %d with the member role\n", len(members), withRole)

	if secret := os.Getenv("RENEW_LINK_SECRET"); secret != "" {
		var resp struct {
			Links []websiteDiscordLink `json:"links"`
		}
		if err := recapGet(secret, "/api/members/discord-links", &resp); err != nil {
			Warnf("⚠ website Discord links: %v", err)
		} else {
			if resp.Links == nil {
				resp.Links = []websiteDiscordLink{}
			}
			if err := writePrivateJSON(discordLinksPath(dataDir), websiteDiscordLinksFile{FetchedAt: time.Now().UTC().Format(time.RFC3339), Links: resp.Links}); err != nil {
				return err
			}
			fmt.Printf("  ✓ website: %d Discord link(s)\n", len(resp.Links))
		}
	}

	k, err := loadPartnerEmailKey()
	if err != nil {
		fmt.Printf("  - partner emails: no key yet (chb discord partners key)\n")
		return nil
	}
	events, ok := fetchEventsFromRelays(nostrRelayList(), map[string]interface{}{
		"kinds": []int{partnerEmailEventKind}, "authors": []string{websiteNostrPubkey},
		"#p": []string{k.PubHex}, "#t": []string{partnerEmailTag},
	})
	if ok == 0 {
		Warnf("⚠ partner emails: no relay answered")
		return nil
	}
	// A newer submission replaces an older one with the same d tag.
	newest := map[string]NostrEvent{}
	for _, ev := range events {
		d := nostrTagValue(ev, "d")
		if prev, ok := newest[d]; !ok || ev.CreatedAt > prev.CreatedAt {
			newest[d] = ev
		}
	}
	subs := loadPartnerEmailSubmissions(dataDir)
	added, bad := 0, 0
	for _, ev := range newest {
		s, err := decryptPartnerEmail(ev, k)
		if err != nil {
			bad++
			continue
		}
		if prev, ok := subs[s.DiscordID]; !ok || s.CreatedAt > prev.CreatedAt {
			if !ok || prev.Email != s.Email {
				added++
			}
			subs[s.DiscordID] = s
		}
	}
	if err := writePrivateJSON(partnerEmailsPath(dataDir), subs); err != nil {
		return err
	}
	fmt.Printf("  ✓ partner emails: %d event(s), %d new, %d rejected\n", len(events), added, bad)
	return nil
}

func loadDiscordPartnersPlan(dataDir string) (DiscordPartnersPlan, error) {
	var plan DiscordPartnersPlan
	data, err := os.ReadFile(discordPartnersPlanPath(dataDir))
	if err != nil {
		return plan, fmt.Errorf("no plan — run `chb discord partners pull` then `chb generate`")
	}
	return plan, json.Unmarshal(data, &plan)
}

// printDiscordPartnersPlan: the counts; compact lists only the reviews
// (--verbose also lists every action).
func printDiscordPartnersPlan(plan DiscordPartnersPlan, compact bool) {
	fmt.Printf("\n  Members with the Discord member role: %d (already linked: %d)\n", plan.Members, plan.Linked)
	fmt.Printf("  To link %d · to create %d · emails to set %d · to review %d · need an email %d\n\n",
		plan.count("link"), plan.count("create"), plan.count("set_email"), len(plan.Review), len(plan.NeedsEmail))
	for _, a := range plan.Actions {
		if compact {
			break
		}
		target := "new partner"
		if a.PartnerID > 0 {
			target = fmt.Sprintf("partner %d", a.PartnerID)
		}
		extra := a.Via
		if a.Email != "" {
			extra = strings.TrimSpace(extra + " email " + maskEmail(a.Email))
		}
		fmt.Printf("    %-9s %-20s %-24s %-14s %s\n", a.Op, a.DiscordID, firstN(a.DisplayName, 24), target, extra)
	}
	if len(plan.Review) > 0 {
		fmt.Printf("\n  To review (no change made):\n")
		for _, r := range plan.Review {
			fmt.Printf("    %-20s %-24s %v %s\n", r.DiscordID, firstN(r.DisplayName, 24), r.PartnerIDs, r.Reason)
		}
	}
	fmt.Println()
}

type odooSession struct {
	creds *OdooCredentials
	uid   int
}

func (s odooSession) exec(model, method string, args []interface{}, kwargs map[string]interface{}) (json.RawMessage, error) {
	return odooExec(s.creds.URL, s.creds.DB, s.uid, s.creds.Password, model, method, args, kwargs)
}

func openOdooSession() (odooSession, error) {
	creds, err := ResolveOdooCredentials()
	if err != nil {
		return odooSession{}, err
	}
	uid, err := odooAuth(creds.URL, creds.DB, creds.Login, creds.Password)
	if err != nil || uid == 0 {
		return odooSession{}, fmt.Errorf("Odoo authentication failed: %v", err)
	}
	return odooSession{creds, uid}, nil
}

// odooHasPartnerDiscordField: whether res.partner has x_discord_id.
func odooHasPartnerDiscordField(s odooSession) (bool, error) {
	raw, err := s.exec("res.partner", "fields_get", []interface{}{[]string{discordPartnerField}}, map[string]interface{}{"attributes": []string{"type"}})
	if err != nil {
		return false, err
	}
	var f map[string]interface{}
	_ = json.Unmarshal(raw, &f)
	_, ok := f[discordPartnerField]
	return ok, nil
}

func odooSearchIDs(s odooSession, model string, domain []interface{}) ([]int, error) {
	raw, err := s.exec(model, "search", []interface{}{domain}, nil)
	if err != nil {
		return nil, err
	}
	var ids []int
	return ids, json.Unmarshal(raw, &ids)
}

func discordPartnersSetup(args []string) error {
	s, err := openOdooSession()
	if err != nil {
		return err
	}
	has, err := odooHasPartnerDiscordField(s)
	if err != nil {
		return err
	}
	tags, err := odooSearchIDs(s, "res.partner.category", []interface{}{[]interface{}{"name", "=", discordPartnerTag}})
	if err != nil {
		return err
	}
	fmt.Printf("\n  Odoo %s (db %s)\n", s.creds.URL, s.creds.DB)
	fmt.Printf("    field res.partner.%s: %s\n", discordPartnerField, map[bool]string{true: "present", false: "to create (char, indexed, \"Discord ID\")"}[has])
	fmt.Printf("    partner tag %q: %s\n", discordPartnerTag, map[bool]string{true: "present", false: "to create"}[len(tags) > 0])
	if has && len(tags) > 0 {
		fmt.Println()
		return nil
	}
	if !HasFlag(args, "--yes") {
		fmt.Printf("\n  %s(preview — nothing changed; add --yes to create)%s\n\n", Fmt.Dim, Fmt.Reset)
		return nil
	}
	if !has {
		models, err := odooSearchIDs(s, "ir.model", []interface{}{[]interface{}{"model", "=", "res.partner"}})
		if err != nil || len(models) != 1 {
			return fmt.Errorf("ir.model res.partner: %v", err)
		}
		if _, err := s.exec("ir.model.fields", "create", []interface{}{map[string]interface{}{
			"model_id": models[0], "name": discordPartnerField, "field_description": "Discord ID",
			"ttype": "char", "state": "manual", "index": true, "copied": false,
		}}, nil); err != nil {
			return fmt.Errorf("create field: %v", err)
		}
		fmt.Printf("    ✓ field %s created\n", discordPartnerField)
	}
	if len(tags) == 0 {
		if _, err := s.exec("res.partner.category", "create", []interface{}{map[string]interface{}{"name": discordPartnerTag}}, nil); err != nil {
			return fmt.Errorf("create tag: %v", err)
		}
		fmt.Printf("    ✓ tag %q created\n", discordPartnerTag)
	}
	fmt.Printf("\n  Next: chb odoo pull (partners with %s), chb generate, chb discord partners push\n\n", discordPartnerField)
	return nil
}

func discordPartnersPush(args []string) error {
	dataDir := DataDir()
	plan, err := loadDiscordPartnersPlan(dataDir)
	if err != nil {
		return err
	}
	printDiscordPartnersPlan(plan, false)
	if len(plan.Actions) == 0 {
		return nil
	}
	if !HasFlag(args, "--yes") {
		fmt.Printf("  %s(preview — nothing changed in Odoo; add --yes to apply)%s\n\n", Fmt.Dim, Fmt.Reset)
		return nil
	}
	s, err := openOdooSession()
	if err != nil {
		return err
	}
	if has, err := odooHasPartnerDiscordField(s); err != nil || !has {
		return fmt.Errorf("res.partner has no %s field — run `chb discord partners setup --yes` first", discordPartnerField)
	}
	tags, err := odooSearchIDs(s, "res.partner.category", []interface{}{[]interface{}{"name", "=", discordPartnerTag}})
	if err != nil || len(tags) == 0 {
		return fmt.Errorf("no %q partner tag — run `chb discord partners setup --yes` first", discordPartnerTag)
	}
	limit := discordPartnersMaxRun
	if n, err := strconv.Atoi(GetOption(args, "--limit")); err == nil && n > 0 {
		limit = n
	}
	done, failed := 0, 0
	for _, a := range plan.Actions {
		if done >= limit {
			fmt.Printf("\n  limit %d reached; the rest on the next run\n", limit)
			break
		}
		// Re-check live: nobody else may carry this Discord id.
		holders, err := odooSearchIDs(s, "res.partner", []interface{}{[]interface{}{discordPartnerField, "=", a.DiscordID}})
		if err != nil {
			return err
		}
		var werr error
		switch a.Op {
		case "link":
			if len(holders) > 0 {
				werr = fmt.Errorf("already on partner %v", holders)
				break
			}
			_, werr = s.exec("res.partner", "write", []interface{}{[]int{a.PartnerID}, map[string]interface{}{discordPartnerField: a.DiscordID}}, nil)
		case "create":
			if len(holders) > 0 {
				werr = fmt.Errorf("already on partner %v", holders)
				break
			}
			vals := map[string]interface{}{"name": a.DisplayName, discordPartnerField: a.DiscordID, "is_company": false,
				"category_id": []interface{}{[]interface{}{4, tags[0]}}}
			if a.Email != "" {
				vals["email"] = a.Email
			}
			_, werr = s.exec("res.partner", "create", []interface{}{vals}, nil)
		case "set_email":
			if len(holders) != 1 || holders[0] != a.PartnerID {
				werr = fmt.Errorf("Discord id now on %v", holders)
				break
			}
			rows, err := odooReadMapsByIDs(s.creds, s.uid, "res.partner", []int{a.PartnerID}, []string{"email"})
			if err != nil || len(rows) != 1 || strings.TrimSpace(odooString(rows[0]["email"])) != "" {
				werr = fmt.Errorf("partner email no longer empty")
				break
			}
			_, werr = s.exec("res.partner", "write", []interface{}{[]int{a.PartnerID}, map[string]interface{}{"email": a.Email}}, nil)
		}
		if werr != nil {
			Warnf("⚠ %s %s: %v", a.Op, a.DiscordID, werr)
			failed++
			continue
		}
		done++
		fmt.Printf("    ✓ %s %s\n", a.Op, a.DiscordID)
	}
	fmt.Printf("\n  %d applied, %d skipped — run `chb odoo pull` and `chb generate` to refresh the plan\n\n", done, failed)
	return nil
}

// partnerDiscordByHash: emailHash → x_discord_id, for members.json.
func partnerDiscordIndex(dataDir, salt string) (byPartner map[int]string, byHash map[string]string) {
	byPartner, byHash = map[int]string{}, map[string]string{}
	pf, ok := loadLatestOdooPartnersFile(dataDir)
	if !ok {
		return
	}
	for _, p := range pf.Partners {
		if p.DiscordID == "" || !p.Active {
			continue
		}
		byPartner[p.ID] = p.DiscordID
		if em := strings.TrimSpace(p.Email); em != "" && salt != "" {
			byHash[hashEmail(em, salt)] = p.DiscordID
		}
	}
	return
}

// attachMemberDiscordIDs fills accounts.discord from the partners'
// x_discord_id (Odoo members by partner id, Stripe members by email hash).
func attachMemberDiscordIDs(dataDir string, members []Member) {
	byPartner, byHash := partnerDiscordIndex(dataDir, os.Getenv("EMAIL_HASH_SALT"))
	for i := range members {
		m := &members[i]
		if m.Accounts.Discord != nil && discordIDPattern.MatchString(*m.Accounts.Discord) {
			continue
		}
		d := byPartner[m.OdooPartnerID]
		if d == "" {
			d = byHash[m.Accounts.EmailHash]
		}
		if d != "" {
			id := d
			m.Accounts.Discord = &id
		}
	}
}
