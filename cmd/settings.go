package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const settingsRepo = "https://raw.githubusercontent.com/CommonsHub/commonshub.brussels/main/src/settings"

var settingsFiles = []string{"settings.json", "rooms.json"}

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

func legacySettingsFilePath(name string) string {
	return filepath.Join(AppDataDir(), name)
}

func existingSettingsFilePath(name string) string {
	path := settingsFilePath(name)
	if _, err := os.Stat(path); err == nil {
		return path
	}
	legacyPath := legacySettingsFilePath(name)
	if _, err := os.Stat(legacyPath); err == nil {
		return legacyPath
	}
	return path
}

// settingsDir returns the directory to look for settings files.
// Downloads from GitHub if missing. Falls back to src/settings/ for monorepo compat.
func settingsDir() string {
	dir := AppSettingsDir()
	settingsPath := filepath.Join(dir, "settings.json")

	if _, err := os.Stat(settingsPath); err == nil {
		return dir
	}

	legacyPath := legacySettingsFilePath("settings.json")
	if _, err := os.Stat(legacyPath); err == nil {
		return AppDataDir()
	}

	if os.Getenv("APP_DATA_DIR") != "" {
		fmt.Printf("%sDownloading settings...%s\n", Fmt.Dim, Fmt.Reset)
		if err := DownloadSettings(dir); err != nil {
			fmt.Printf("%sCould not download settings:%s %v\n", Fmt.Yellow, Fmt.Reset, err)
		}
		return dir
	}

	// Try src/settings/ (monorepo compat)
	if _, err := os.Stat(filepath.Join("src", "settings", "settings.json")); err == nil {
		return filepath.Join("src", "settings")
	}

	// Download from GitHub
	fmt.Printf("%sDownloading settings...%s\n", Fmt.Dim, Fmt.Reset)
	if err := DownloadSettings(dir); err != nil {
		fmt.Printf("%sCould not download settings:%s %v\n", Fmt.Yellow, Fmt.Reset, err)
		return dir
	}

	return dir
}

// DownloadSettings fetches settings files from the commonshub.brussels repo
func DownloadSettings(dir string) error {
	client := &http.Client{Timeout: 10 * time.Second}
	os.MkdirAll(dir, 0755)

	for _, file := range settingsFiles {
		url := settingsRepo + "/" + file
		resp, err := client.Get(url)
		if err != nil {
			return fmt.Errorf("failed to download %s: %w", file, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			return fmt.Errorf("failed to download %s: HTTP %d", file, resp.StatusCode)
		}

		data, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("failed to read %s: %w", file, err)
		}

		path := filepath.Join(dir, file)
		if err := os.WriteFile(path, data, 0644); err != nil {
			return fmt.Errorf("failed to write %s: %w", file, err)
		}

		fmt.Printf("  %s✓%s %s\n", Fmt.Green, Fmt.Reset, file)
	}
	return nil
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

// Settings represents settings.json
type Settings struct {
	Luma struct {
		CalendarID string `json:"calendarId"`
	} `json:"luma"`
	Calendars         CalendarSettings           `json:"calendars"`
	Discord           DiscordSettings            `json:"discord"`
	Finance           FinanceSettings            `json:"finance"`
	Membership        MembershipSettings         `json:"membership"`
	ContributionToken *ContributionTokenSettings `json:"contributionToken"`
	Tokens            []TokenConfig              `json:"-"`
	Accounting        *AccountingSettings        `json:"accounting,omitempty"`
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
	dir := settingsDir()
	data, err := os.ReadFile(filepath.Join(dir, "settings.json"))
	if err != nil {
		return nil, fmt.Errorf("failed to load settings: %w", err)
	}
	var s Settings
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, err
	}

	loadCalendarsSettings(&s)

	// Override accounts from accounts.json if it exists
	if _, err := os.Stat(existingSettingsFilePath("accounts.json")); err == nil {
		configs := LoadAccountConfigs()
		if len(configs) > 0 {
			s.Finance.Accounts = ToFinanceAccounts(configs)
		}
	}
	s.Tokens = loadTokenConfigsForSettings(&s)
	if len(s.Tokens) > 0 {
		s.Finance.Accounts = filterTokenTrackerFinanceAccounts(s.Finance.Accounts)
		s.Finance.Accounts = append(s.Finance.Accounts, ToFinanceTokenAccounts(s.Tokens)...)
	}

	return &s, nil
}

// SaveAccountingSettings updates only the "accounting" key in settings.json,
// preserving all other fields.
func SaveAccountingSettings(acct *AccountingSettings) error {
	dir := settingsDir()
	path := filepath.Join(dir, "settings.json")

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
	data, err := os.ReadFile(existingSettingsFilePath("rooms.json"))
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
	data, err := os.ReadFile(existingSettingsFilePath("calendars.json"))
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
			parsedSource := false
			var source CalendarSourceConfig
			if json.Unmarshal(v, &source) == nil {
				source.Slug = firstNonEmptyStr(source.Slug, "luma")
				source.Name = firstNonEmptyStr(source.Name, source.Slug)
				if normalized, ok := normalizeCalendarSource(source); ok {
					settings.Calendars.Luma = normalized.URL
					sources = append(sources, normalized)
					parsedSource = true
				}
			}
			if !parsedSource {
				var legacy struct {
					CalendarID string `json:"calendarId"`
				}
				if json.Unmarshal(v, &legacy) == nil {
					settings.Luma.CalendarID = legacy.CalendarID
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
