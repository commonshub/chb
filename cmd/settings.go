package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

const settingsRepo = "https://raw.githubusercontent.com/CommonsHub/commonshub.brussels/main/src/settings"

var settingsFiles = []string{"settings.json", "rooms.json"}

// chbDir returns ~/.chb/, creating it if needed
func chbDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return "."
	}
	dir := filepath.Join(home, ".chb")
	os.MkdirAll(dir, 0755)
	return dir
}

// settingsDir returns the directory to look for settings files.
// Downloads from GitHub if missing. Falls back to src/settings/ for monorepo compat.
func settingsDir() string {
	dir := chbDir()
	settingsPath := filepath.Join(dir, "settings.json")

	if _, err := os.Stat(settingsPath); err == nil {
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

// Settings represents settings.json
type Settings struct {
	Luma struct {
		CalendarID string `json:"calendarId"`
	} `json:"luma"`
	Calendars struct {
		Google string `json:"google"`
		Luma   string `json:"luma"`
	} `json:"calendars"`
	Discord    DiscordSettings    `json:"discord"`
	Finance    FinanceSettings    `json:"finance"`
	Membership MembershipSettings `json:"membership"`
}

// MembershipSettings holds membership provider configuration
type MembershipSettings struct {
	Stripe struct {
		ProductID string `json:"productId"`
	} `json:"stripe"`
	Odoo OdooSettings `json:"odoo"`
}

// OdooSettings holds Odoo connection and product configuration
type OdooSettings struct {
	URL      string         `json:"url"`
	DB       string         `json:"db"`
	Products []OdooProduct  `json:"products"`
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
	Accounts    []FinanceAccount          `json:"accounts"`
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
	return &s, nil
}

func LoadRooms() ([]RoomInfo, error) {
	dir := settingsDir()
	data, err := os.ReadFile(filepath.Join(dir, "rooms.json"))
	if err != nil {
		return nil, err
	}
	var rc RoomsConfig
	if err := json.Unmarshal(data, &rc); err != nil {
		return nil, err
	}
	return rc.Rooms, nil
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

	return result
}
