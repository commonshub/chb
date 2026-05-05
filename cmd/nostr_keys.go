package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/nbd-wtf/go-nostr/nip19"
)

// NostrKeys holds the Nostr identity and relay configuration.
type NostrKeys struct {
	Nsec    string   `json:"nsec"`
	Npub    string   `json:"npub"`
	PrivHex string   `json:"privHex"`
	PubHex  string   `json:"pubHex"`
	Name    string   `json:"name"`
	About   string   `json:"about"`
	Relays  []string `json:"relays"`
}

func nostrKeysPath() string {
	return settingsFilePath("nostr.json")
}

func legacyNostrKeysPath() string {
	return filepath.Join(AppDataDir(), ".nostr-keys.json")
}

// LoadNostrKeys reads the Nostr keys from disk. Returns nil if not found.
func LoadNostrKeys() *NostrKeys {
	data, err := os.ReadFile(nostrKeysPath())
	if err != nil {
		data, err = os.ReadFile(legacyNostrKeysPath())
		if err != nil {
			return nil
		}
	}
	var keys NostrKeys
	if json.Unmarshal(data, &keys) != nil {
		return nil
	}
	return &keys
}

// SaveNostrKeys writes the Nostr keys to disk with restricted permissions.
func SaveNostrKeys(keys *NostrKeys) error {
	data, err := json.MarshalIndent(keys, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(nostrKeysPath())
	os.MkdirAll(dir, 0755)
	return os.WriteFile(nostrKeysPath(), data, 0600)
}

// GenerateNostrKeyPair generates a new Nostr keypair.
func GenerateNostrKeyPair() (nsec, npub, privHex, pubHex string, err error) {
	privHex = nostr.GeneratePrivateKey()
	pubHex, err = nostr.GetPublicKey(privHex)
	if err != nil {
		return "", "", "", "", fmt.Errorf("failed to derive public key: %w", err)
	}

	nsec, err = nip19.EncodePrivateKey(privHex)
	if err != nil {
		return "", "", "", "", fmt.Errorf("failed to encode nsec: %w", err)
	}

	npub, err = nip19.EncodePublicKey(pubHex)
	if err != nil {
		return "", "", "", "", fmt.Errorf("failed to encode npub: %w", err)
	}

	return nsec, npub, privHex, pubHex, nil
}

// DecodeNsec decodes an nsec string to hex private key, and derives pubkey/npub.
func DecodeNsec(nsec string) (npub, privHex, pubHex string, err error) {
	_, decoded, err := nip19.Decode(nsec)
	if err != nil {
		return "", "", "", fmt.Errorf("invalid nsec: %w", err)
	}
	privHex, ok := decoded.(string)
	if !ok {
		return "", "", "", fmt.Errorf("invalid nsec: unexpected decoded type")
	}

	pubHex, err = nostr.GetPublicKey(privHex)
	if err != nil {
		return "", "", "", fmt.Errorf("failed to derive public key: %w", err)
	}

	npub, err = nip19.EncodePublicKey(pubHex)
	if err != nil {
		return "", "", "", fmt.Errorf("failed to encode npub: %w", err)
	}

	return npub, privHex, pubHex, nil
}

// PublishNostrProfile publishes a kind 0 (profile metadata) event to relays.
func PublishNostrProfile(keys *NostrKeys) error {
	profile := map[string]string{
		"name":  keys.Name,
		"about": keys.About,
	}
	profileJSON, _ := json.Marshal(profile)

	ev := nostr.Event{
		PubKey:    keys.PubHex,
		CreatedAt: nostr.Timestamp(time.Now().Unix()),
		Kind:      0,
		Tags:      nostr.Tags{},
		Content:   string(profileJSON),
	}
	ev.Sign(keys.PrivHex)

	relays := keys.Relays
	if len(relays) == 0 {
		relays = nostrRelays
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	published := 0
	for _, relayURL := range relays {
		relay, err := nostr.RelayConnect(ctx, relayURL)
		if err != nil {
			continue
		}
		err = relay.Publish(ctx, ev)
		relay.Close()
		if err == nil {
			published++
		}
	}

	if published == 0 {
		return fmt.Errorf("failed to publish profile to any relay")
	}

	return nil
}

// PublishNostrEvent signs and publishes a single event to the configured relays.
// Returns the list of relays that accepted the event.
func PublishNostrEvent(keys *NostrKeys, ev *nostr.Event) ([]string, error) {
	SignNostrEvent(keys, ev)
	return PublishSignedNostrEvent(keys, ev)
}

func SignNostrEvent(keys *NostrKeys, ev *nostr.Event) {
	ev.PubKey = keys.PubHex
	if ev.CreatedAt == 0 {
		ev.CreatedAt = nostr.Timestamp(time.Now().Unix())
	}
	ev.Sign(keys.PrivHex)
}

func PublishSignedNostrEvent(keys *NostrKeys, ev *nostr.Event) ([]string, error) {
	relays := keys.Relays
	if len(relays) == 0 {
		relays = nostrRelays
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var accepted []string
	for _, relayURL := range relays {
		relay, err := nostr.RelayConnect(ctx, relayURL)
		if err != nil {
			continue
		}
		err = relay.Publish(ctx, *ev)
		relay.Close()
		if err == nil {
			accepted = append(accepted, relayURL)
		}
	}

	if len(accepted) == 0 {
		return nil, fmt.Errorf("no relay accepted the event")
	}

	return accepted, nil
}
