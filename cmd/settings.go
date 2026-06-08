package cmd

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// defaultSettingsFS holds the public, shipped-with-the-binary defaults that
// seed APP_DATA_DIR/settings/ on first run. Each file behaves like a Debian
// conffile: unedited locals auto-track upstream changes; edited locals are
// left alone and surface as pending updates in `chb settings`.
//
//go:embed all:defaults
var defaultSettingsFS embed.FS

const (
	defaultSettingsRoot     = "defaults"
	installedDefaultsRecord = ".installed-defaults.json"
)

// AppDataDir returns the directory for app configuration/state files.
// Defaults to ~/.chb and can be overridden with APP_DATA_DIR.
func AppDataDir() string {
	if d := os.Getenv("APP_DATA_DIR"); d != "" {
		if err := os.MkdirAll(d, 0755); err != nil {
			return d
		}
		return d
	}
	home, err := os.UserHomeDir()
	if err != nil {
		dir := filepath.Join(".", ".chb")
		_ = os.MkdirAll(dir, 0755)
		return dir
	}
	dir := filepath.Join(home, ".chb")
	_ = os.MkdirAll(dir, 0755)
	return dir
}

// chbDir returns the app data directory. Kept as an internal compatibility
// wrapper for existing config helpers.
func chbDir() string {
	return AppDataDir()
}

// AppSettingsDir returns the directory for app configuration files.
// Defaults to APP_DATA_DIR/settings.
func AppSettingsDir() string {
	dir := filepath.Join(AppDataDir(), "settings")
	_ = os.MkdirAll(dir, 0755)
	return dir
}

func settingsFilePath(name string) string {
	return filepath.Join(AppSettingsDir(), name)
}

// readDefaultSettingsFile returns the bytes of an embedded default by name
// (e.g. "settings.json"). Returns (nil, error) if the file isn't shipped.
func readDefaultSettingsFile(name string) ([]byte, error) {
	return defaultSettingsFS.ReadFile(filepath.Join(defaultSettingsRoot, name))
}

// listDefaultSettingsFiles returns the basenames of every shipped default.
func listDefaultSettingsFiles() ([]string, error) {
	entries, err := defaultSettingsFS.ReadDir(defaultSettingsRoot)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		out = append(out, e.Name())
	}
	return out, nil
}

// PendingDefaultUpdate describes an embedded default whose content has
// changed compared to what we last installed AND whose local copy has been
// edited by the user. `chb settings` shows the diff so the user can decide
// whether to adopt the new defaults.
type PendingDefaultUpdate struct {
	Name          string
	LocalContent  []byte
	UpstreamBytes []byte
}

// pendingDefaultUpdates is populated by EnsureSettingsBootstrapped on every
// run. Read by `chb settings`.
var pendingDefaultUpdates []PendingDefaultUpdate

// EnsureSettingsBootstrapped reconciles APP_DATA_DIR/settings/ with the
// embedded defaults. Per file:
//
//   - missing locally       → install from embed, record its hash.
//   - identical to embed    → no-op (refresh tracker just in case).
//   - tracked-hash matches  → user hasn't edited; auto-update to new embed.
//   - tracked-hash differs  → user edited; surface as a pending update.
//
// Pending updates are stored in pendingDefaultUpdates for `chb settings`.
func EnsureSettingsBootstrapped() string {
	dir := AppSettingsDir()
	// Drain any legacy-schema fields out of on-disk files BEFORE the
	// embedded defaults overwrite an unedited local copy. The migration
	// is a no-op on already-migrated files.
	migrateLegacySettingsSchemas(dir)
	if err := reconcileDefaultSettings(dir); err != nil {
		fmt.Printf("%sCould not reconcile default settings:%s %v\n", Fmt.Yellow, Fmt.Reset, err)
	}
	return dir
}

// migrateLegacySettingsSchemas runs one-shot schema cleanups against files
// in APP_DATA_DIR/settings/ before the bootstrap reconciler runs. Each
// helper is idempotent.
func migrateLegacySettingsSchemas(dir string) {
	accountsPath := filepath.Join(dir, "accounts.json")
	if data, err := os.ReadFile(accountsPath); err == nil {
		migrateLegacyOdooJournalNames(data)
	}
	// Move per-account Odoo journal IDs into odoo-journals.json before the
	// reconciler force-overwrites accounts.json and discards them.
	migrateOdooJournalLinks(dir)
}

// forceOverwriteDefaults are embedded defaults whose content the embedded copy
// owns: on every bootstrap they overwrite the local file even if the user edited
// it (so local edits don't survive a `chb` run / upgrade). Use sparingly — only
// for files meant to be the org-wide source of truth. accounts.json is the
// canonical account list shared across machines.
var forceOverwriteDefaults = map[string]bool{
	"accounts.json": true,
}

// forceOverwriteDefaultsEnabled gates the force-overwrite behavior. Tests that
// pin a settings fixture (seedSettingsFixture) flip it off so their fixture
// survives bootstrap; production always leaves it on.
var forceOverwriteDefaultsEnabled = true

func reconcileDefaultSettings(dir string) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	tracker := loadInstalledDefaultsRecord(dir)
	pending := pendingDefaultUpdates[:0]
	trackerDirty := false

	err := fs.WalkDir(defaultSettingsFS, defaultSettingsRoot, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil || d.IsDir() {
			return walkErr
		}
		rel, err := filepath.Rel(defaultSettingsRoot, path)
		if err != nil {
			return err
		}
		embedBytes, err := defaultSettingsFS.ReadFile(path)
		if err != nil {
			return err
		}
		embedHash := sha256Hex(embedBytes)
		target := filepath.Join(dir, rel)

		localBytes, statErr := os.ReadFile(target)
		switch {
		case os.IsNotExist(statErr):
			if err := writeDefaultFile(target, embedBytes); err != nil {
				return err
			}
			tracker[rel] = embedHash
			trackerDirty = true
			fmt.Printf("  %s✓%s installed default %s\n", Fmt.Green, Fmt.Reset, rel)
			return nil
		case statErr != nil:
			return statErr
		}

		localHash := sha256Hex(localBytes)
		trackedHash, hadTracker := tracker[rel]

		if localHash == embedHash {
			if tracker[rel] != embedHash {
				tracker[rel] = embedHash
				trackerDirty = true
			}
			return nil
		}

		// local != embed
		userEdited := hadTracker && trackedHash != localHash
		// Force-overwrite files are centrally managed: the embedded default is
		// the source of truth and always wins, even over local edits.
		if !userEdited || (forceOverwriteDefaultsEnabled && forceOverwriteDefaults[rel]) {
			if err := writeDefaultFile(target, embedBytes); err != nil {
				return err
			}
			tracker[rel] = embedHash
			trackerDirty = true
			verb := "updated"
			if userEdited {
				verb = "force-updated"
			}
			fmt.Printf("  %s↑%s %s %s from embedded default\n", Fmt.Green, Fmt.Reset, verb, rel)
			return nil
		}

		// User edited locally AND embed differs from local → pending update.
		pending = append(pending, PendingDefaultUpdate{
			Name:          rel,
			LocalContent:  localBytes,
			UpstreamBytes: embedBytes,
		})
		return nil
	})
	if err != nil {
		return err
	}

	if trackerDirty {
		_ = saveInstalledDefaultsRecord(dir, tracker)
	}
	pendingDefaultUpdates = pending
	return nil
}

// PendingSettingsUpdates returns the latest list collected by
// EnsureSettingsBootstrapped. Safe to call before/after bootstrap.
func PendingSettingsUpdates() []PendingDefaultUpdate {
	out := make([]PendingDefaultUpdate, len(pendingDefaultUpdates))
	copy(out, pendingDefaultUpdates)
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func writeDefaultFile(target string, content []byte) error {
	if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
		return err
	}
	return os.WriteFile(target, content, 0644)
}

func sha256Hex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func installedDefaultsRecordPath(dir string) string {
	return filepath.Join(dir, installedDefaultsRecord)
}

func loadInstalledDefaultsRecord(dir string) map[string]string {
	out := map[string]string{}
	data, err := os.ReadFile(installedDefaultsRecordPath(dir))
	if err != nil {
		return out
	}
	_ = json.Unmarshal(data, &out)
	return out
}

func saveInstalledDefaultsRecord(dir string, record map[string]string) error {
	data, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(installedDefaultsRecordPath(dir), data, 0644)
}

// ContributionTokenSettings holds the contribution token config from settings.json
type ContributionTokenSettings struct {
	Chain                 string `json:"chain"`
	ChainID               int    `json:"chainId"`
	RpcUrl                string `json:"rpcUrl,omitempty"`
	ExplorerUrl           string `json:"explorerUrl,omitempty"`
	Address               string `json:"address"`
	Name                  string `json:"name"`
	Symbol                string `json:"symbol"`
	Decimals              int    `json:"decimals"`
	WalletManager         string `json:"walletManager,omitempty"`
	CardManagerAddress    string `json:"cardManagerAddress,omitempty"`
	CardManagerInstanceID string `json:"cardManagerInstanceId,omitempty"`
}

const (
	CalendarVisibilityAuto    = "auto"
	CalendarVisibilityPrivate = "private"
	CalendarVisibilityPublic  = "public"
	CalendarProviderICS       = "ics"
)

// CalendarSourceConfig describes one external calendar feed and how its
// entries should be classified by sync.
type CalendarSourceConfig struct {
	Slug       string `json:"slug"`
	Name       string `json:"name,omitempty"`
	Provider   string `json:"provider,omitempty"`
	URL        string `json:"url"`
	Visibility string `json:"visibility"`
	Room       string `json:"room,omitempty"`
}

// CalendarSettings holds legacy direct URLs plus normalized source configs.
type CalendarSettings struct {
	Google  string                 `json:"google"`
	Luma    string                 `json:"luma"`
	Sources []CalendarSourceConfig `json:"-"`
}

// Settings represents settings.json. Several fields are populated from
// dedicated files (accounts.json, tokens.json, calendars.json, …) rather
// than from settings.json directly — see LoadSettings.
type Settings struct {
	Calendars  CalendarSettings    `json:"calendars"`
	Discord    DiscordSettings     `json:"discord"`
	Finance    FinanceSettings     `json:"finance"`
	Membership MembershipSettings  `json:"membership"`
	Accounting *AccountingSettings `json:"accounting,omitempty"`

	// ContributionToken is derived from tokens.json (the entry marked
	// contribution=true) at load time; settings.json no longer stores it.
	ContributionToken *ContributionTokenSettings `json:"-"`

	// Tokens is loaded from tokens.json (with finance-account token
	// trackers folded in).
	Tokens []TokenConfig `json:"-"`
}

// MembershipSettings holds membership provider configuration
type MembershipSettings struct {
	Stripe struct {
		ProductID string `json:"productId"`
	} `json:"stripe"`
	Odoo OdooSettings `json:"odoo"`
}

// OdooSettings holds Odoo product configuration.
// URL and credentials come from env vars in APP_DATA_DIR/settings/config.env.
// Database is derived from the ODOO_URL hostname.
type OdooSettings struct {
	Products []OdooProduct `json:"products"`
}

// OdooProduct represents an Odoo subscription product
type OdooProduct struct {
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Interval string `json:"interval"` // "month" or "year"
}

// DiscordSettings holds Discord configuration
type DiscordSettings struct {
	GuildID  string            `json:"guildId"`
	Roles    map[string]string `json:"roles"`
	Channels json.RawMessage   `json:"channels"`
}

// FinanceSettings holds finance configuration
type FinanceSettings struct {
	Accounts    []FinanceAccount           `json:"accounts"`
	Collectives map[string]json.RawMessage `json:"collectives"`
}

// FinanceAccount represents a single finance account
type FinanceAccount struct {
	Name      string `json:"name"`
	Slug      string `json:"slug"`
	Provider  string `json:"provider"`
	Chain     string `json:"chain,omitempty"`
	ChainID   int    `json:"chainId,omitempty"`
	Address   string `json:"address,omitempty"`
	AccountID string `json:"accountId,omitempty"`
	Currency  string `json:"currency,omitempty"`
	Token     *struct {
		Address  string `json:"address"`
		Name     string `json:"name"`
		Symbol   string `json:"symbol"`
		Decimals int    `json:"decimals"`
	} `json:"token,omitempty"`
	// PriorTokens carries earlier contract versions of the same currency so the
	// sync can pull each one in addition to Token. See AccountConfig.PriorTokens.
	PriorTokens []AccountToken `json:"priorTokens,omitempty"`
}

// RoomInfo represents a single room from rooms.json
type RoomInfo struct {
	ID               string   `json:"id"`
	Name             string   `json:"name"`
	Slug             string   `json:"slug"`
	Capacity         int      `json:"capacity"`
	Description      string   `json:"description"`
	PricePerHour     float64  `json:"pricePerHour"`
	TokensPerHour    float64  `json:"tokensPerHour"`
	Features         []string `json:"features"`
	IdealFor         []string `json:"idealFor"`
	DiscordChannelID string   `json:"discordChannelId"`
	Calendar         string   `json:"calendar,omitempty"`
	GoogleCalendarID *string  `json:"googleCalendarId"`
	MembershipReq    bool     `json:"membershipRequired,omitempty"`
}

// RoomsConfig represents rooms.json
type RoomsConfig struct {
	Rooms []RoomInfo `json:"rooms"`
}

func LoadSettings() (*Settings, error) {
	dir := EnsureSettingsBootstrapped()
	data, err := os.ReadFile(filepath.Join(dir, "settings.json"))
	if err != nil {
		return nil, fmt.Errorf("failed to load settings: %w", err)
	}
	var s Settings
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, err
	}

	loadCalendarsSettings(&s)

	if configs := LoadAccountConfigs(); len(configs) > 0 {
		s.Finance.Accounts = ToFinanceAccounts(configs)
	}
	s.Tokens = loadTokenConfigsForSettings(&s)
	if len(s.Tokens) > 0 {
		s.Finance.Accounts = filterTokenTrackerFinanceAccounts(s.Finance.Accounts)
		s.Finance.Accounts = append(s.Finance.Accounts, ToFinanceTokenAccounts(s.Tokens)...)
	}

	// tokens.json is the source of truth for the contribution token; project
	// it back into the legacy field shape that the rest of the CLI reads.
	s.ContributionToken = contributionTokenSettingsFromTokens(s.Tokens)

	return &s, nil
}

// SaveAccountingSettings updates only the "accounting" key in settings.json,
// preserving all other fields.
func SaveAccountingSettings(acct *AccountingSettings) error {
	path := settingsFilePath("settings.json")

	// Load raw JSON to preserve unknown fields
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	acctJSON, err := json.Marshal(acct)
	if err != nil {
		return err
	}
	raw["accounting"] = acctJSON

	out, err := json.MarshalIndent(raw, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, out, 0644)
}

func LoadRooms() ([]RoomInfo, error) {
	data, err := os.ReadFile(settingsFilePath("rooms.json"))
	if err != nil {
		return nil, err
	}
	var rc RoomsConfig
	if err := json.Unmarshal(data, &rc); err != nil {
		return nil, err
	}
	return rc.Rooms, nil
}

func loadCalendarsSettings(settings *Settings) {
	data, err := os.ReadFile(settingsFilePath("calendars.json"))
	if err != nil {
		settings.Calendars.Sources = legacyCalendarSources(settings)
		return
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		settings.Calendars.Sources = legacyCalendarSources(settings)
		return
	}

	var sources []CalendarSourceConfig
	if v, ok := raw["sources"]; ok {
		var configured []CalendarSourceConfig
		if json.Unmarshal(v, &configured) == nil {
			for _, source := range configured {
				if normalized, ok := normalizeCalendarSource(source); ok {
					sources = append(sources, normalized)
				}
			}
		}
	}

	if v, ok := raw["google"]; ok {
		var google string
		if json.Unmarshal(v, &google) == nil {
			settings.Calendars.Google = google
			if normalized, ok := normalizeCalendarSource(CalendarSourceConfig{
				Slug:       "google",
				Name:       "google",
				Provider:   CalendarProviderICS,
				URL:        google,
				Visibility: CalendarVisibilityAuto,
			}); ok {
				sources = append(sources, normalized)
			}
		} else {
			var source CalendarSourceConfig
			if json.Unmarshal(v, &source) == nil {
				source.Slug = firstNonEmptyStr(source.Slug, "google")
				source.Name = firstNonEmptyStr(source.Name, source.Slug)
				if normalized, ok := normalizeCalendarSource(source); ok {
					settings.Calendars.Google = normalized.URL
					sources = append(sources, normalized)
				}
			}
		}
	}
	if v, ok := raw["luma"]; ok {
		var lumaICS string
		if json.Unmarshal(v, &lumaICS) == nil {
			settings.Calendars.Luma = lumaICS
			if normalized, ok := normalizeCalendarSource(CalendarSourceConfig{
				Slug:       "luma",
				Name:       "luma",
				Provider:   CalendarProviderICS,
				URL:        lumaICS,
				Visibility: CalendarVisibilityAuto,
			}); ok {
				sources = append(sources, normalized)
			}
		} else {
			var source CalendarSourceConfig
			if json.Unmarshal(v, &source) == nil {
				source.Slug = firstNonEmptyStr(source.Slug, "luma")
				source.Name = firstNonEmptyStr(source.Name, source.Slug)
				if normalized, ok := normalizeCalendarSource(source); ok {
					settings.Calendars.Luma = normalized.URL
					sources = append(sources, normalized)
				}
			}
		}
	}
	if v, ok := raw["calendars"]; ok {
		var legacy struct {
			Google string `json:"google"`
			Luma   string `json:"luma"`
		}
		if json.Unmarshal(v, &legacy) == nil {
			settings.Calendars.Google = legacy.Google
			settings.Calendars.Luma = legacy.Luma
			sources = append(sources, legacyCalendarSources(settings)...)
		}
	}

	if len(sources) == 0 {
		sources = legacyCalendarSources(settings)
	}
	settings.Calendars.Sources = dedupeCalendarSources(sources)
}

func legacyCalendarSources(settings *Settings) []CalendarSourceConfig {
	var sources []CalendarSourceConfig
	if settings.Calendars.Luma != "" {
		sources = append(sources, CalendarSourceConfig{
			Slug:       "luma",
			Name:       "luma",
			Provider:   CalendarProviderICS,
			URL:        settings.Calendars.Luma,
			Visibility: CalendarVisibilityAuto,
		})
	}
	if settings.Calendars.Google != "" {
		sources = append(sources, CalendarSourceConfig{
			Slug:       "google",
			Name:       "google",
			Provider:   CalendarProviderICS,
			URL:        settings.Calendars.Google,
			Visibility: CalendarVisibilityAuto,
		})
	}
	return sources
}

func normalizeCalendarSource(source CalendarSourceConfig) (CalendarSourceConfig, bool) {
	source.Slug = strings.TrimSpace(source.Slug)
	source.Name = strings.TrimSpace(source.Name)
	source.Provider = normalizeCalendarProvider(source.Provider, source.URL)
	source.URL = strings.TrimSpace(source.URL)
	source.Room = strings.TrimSpace(source.Room)
	if source.Slug == "" || source.URL == "" {
		return CalendarSourceConfig{}, false
	}
	if source.Name == "" {
		source.Name = source.Slug
	}
	source.Visibility = normalizeCalendarVisibility(source.Visibility)
	if source.Room != "" {
		source.Visibility = CalendarVisibilityAuto
	}
	return source, true
}

func normalizeCalendarProvider(provider, rawURL string) string {
	provider = strings.ToLower(strings.TrimSpace(provider))
	if provider != "" {
		return provider
	}
	if calendarURLLooksICS(rawURL) {
		return CalendarProviderICS
	}
	// Existing calendar configs were all ICS feeds before provider was explicit.
	return CalendarProviderICS
}

func calendarURLLooksICS(rawURL string) bool {
	rawURL = strings.ToLower(strings.TrimSpace(rawURL))
	return strings.HasPrefix(rawURL, "webcal://") ||
		strings.HasSuffix(rawURL, ".ics") ||
		strings.Contains(rawURL, ".ics?") ||
		strings.Contains(rawURL, "/ical/")
}

func normalizeCalendarVisibility(visibility string) string {
	switch strings.ToLower(strings.TrimSpace(visibility)) {
	case CalendarVisibilityPrivate:
		return CalendarVisibilityPrivate
	case CalendarVisibilityPublic:
		return CalendarVisibilityPublic
	default:
		return CalendarVisibilityAuto
	}
}

func dedupeCalendarSources(sources []CalendarSourceConfig) []CalendarSourceConfig {
	seen := map[string]bool{}
	out := make([]CalendarSourceConfig, 0, len(sources))
	for _, source := range sources {
		normalized, ok := normalizeCalendarSource(source)
		if !ok || seen[normalized.Slug] {
			continue
		}
		seen[normalized.Slug] = true
		out = append(out, normalized)
	}
	return out
}

// GetDiscordChannelIDs extracts all channel IDs from the Discord channels config
// It handles the nested structure where some entries are strings and some are objects
func GetDiscordChannelIDs(s *Settings) map[string]string {
	result := make(map[string]string)

	var channels map[string]json.RawMessage
	if err := json.Unmarshal(s.Discord.Channels, &channels); err != nil {
		return result
	}

	for name, raw := range channels {
		// Try as string first
		var strVal string
		if err := json.Unmarshal(raw, &strVal); err == nil {
			result[name] = strVal
			continue
		}
		// Try as nested object
		var nested map[string]string
		if err := json.Unmarshal(raw, &nested); err == nil {
			for subName, id := range nested {
				result[name+"/"+subName] = id
			}
		}
	}

	rooms, err := LoadRooms()
	if err != nil {
		return result
	}
	for _, room := range rooms {
		if room.DiscordChannelID == "" {
			continue
		}
		key := "room/" + room.Slug
		if key == "room/" {
			key = "room/" + room.ID
		}
		result[key] = room.DiscordChannelID
	}

	return result
}
