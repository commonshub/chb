package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// settingsDir returns the directory to look for settings files.
// Checks ~/.chb/ first, falls back to src/settings/ for monorepo compat.
func settingsDir() string {
	home, err := os.UserHomeDir()
	if err == nil {
		chbDir := filepath.Join(home, ".chb")
		if _, err := os.Stat(filepath.Join(chbDir, "settings.json")); err == nil {
			return chbDir
		}
	}
	return filepath.Join("src", "settings")
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
	Discord DiscordSettings `json:"discord"`
	Finance FinanceSettings `json:"finance"`
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
		return nil, fmt.Errorf("failed to load settings: %w\nPlace settings.json in ~/.chb/ or run from the website repo", err)
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
