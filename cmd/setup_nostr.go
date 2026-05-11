package cmd

import (
	"fmt"

	"github.com/charmbracelet/huh"
)

// SetupNostr runs the interactive Nostr identity setup.
func SetupNostr() error {
	fmt.Printf("\n%s🔑 Nostr Identity Setup%s\n", Fmt.Bold, Fmt.Reset)
	fmt.Println("─────────────────────────")

	// Check for existing keys
	existing := LoadNostrKeys()
	if existing != nil {
		fmt.Printf("\n  %s✓ Nostr identity already configured%s\n", Fmt.Green, Fmt.Reset)
		fmt.Printf("  %snpub: %s%s\n", Fmt.Dim, existing.Npub, Fmt.Reset)
		fmt.Printf("  %sName: %s%s\n", Fmt.Dim, existing.Name, Fmt.Reset)
		fmt.Printf("  %sRelays: %d configured%s\n\n", Fmt.Dim, len(existing.Relays), Fmt.Reset)

		var reconfigure bool
		runField(huh.NewConfirm().
			Title("Reconfigure Nostr identity?").
			Value(&reconfigure))

		if !reconfigure {
			return nil
		}
	}

	// Step 1: Relay selection
	fmt.Printf("\n%s1. Select Relays%s\n\n", Fmt.Bold, Fmt.Reset)

	defaultRelays := nostrRelays
	relayOptions := make([]huh.Option[string], len(defaultRelays))
	for i, r := range defaultRelays {
		relayOptions[i] = huh.NewOption(r, r)
	}

	var selectedRelays []string
	checkAbort(huh.NewMultiSelect[string]().
		Title("Select relays to publish to").
		Description("Use x or Space to toggle, Enter to confirm").
		Options(relayOptions...).
		Value(&selectedRelays).
		Run())

	// Option to add a custom relay
	var addCustom bool
	runField(huh.NewConfirm().
		Title("Add a custom relay?").
		Value(&addCustom))

	for addCustom {
		var customRelay string
		runField(huh.NewInput().
			Title("Relay URL (wss://...)").
			Value(&customRelay))

		if customRelay != "" {
			selectedRelays = append(selectedRelays, customRelay)
			fmt.Printf("  %s✓ Added %s%s\n", Fmt.Green, customRelay, Fmt.Reset)
		}

		runField(huh.NewConfirm().
			Title("Add another relay?").
			Value(&addCustom))
	}

	if len(selectedRelays) == 0 {
		Warnf("%s⚠ No relays selected, using defaults%s", Fmt.Yellow, Fmt.Reset)
		selectedRelays = defaultRelays
	}

	// Step 2: Key generation or import
	fmt.Printf("\n%s2. Nostr Key%s\n\n", Fmt.Bold, Fmt.Reset)

	var keyChoice string
	runField(huh.NewSelect[string]().
		Title("How would you like to set up your key?").
		Options(
			huh.NewOption("Generate new keypair", "generate"),
			huh.NewOption("Import existing nsec", "import"),
		).
		Value(&keyChoice))

	var nsec, npub, privHex, pubHex string
	var err error

	if keyChoice == "import" {
		var nsecInput string
		runField(huh.NewInput().
			Title("Paste your nsec").
			Value(&nsecInput))

		npub, privHex, pubHex, err = DecodeNsec(nsecInput)
		if err != nil {
			return fmt.Errorf("invalid nsec: %w", err)
		}
		nsec = nsecInput
	} else {
		nsec, npub, privHex, pubHex, err = GenerateNostrKeyPair()
		if err != nil {
			return fmt.Errorf("failed to generate keys: %w", err)
		}
	}

	fmt.Printf("\n  %snpub: %s%s\n", Fmt.Dim, npub, Fmt.Reset)

	// Step 3: Profile info
	fmt.Printf("\n%s3. Profile%s\n\n", Fmt.Bold, Fmt.Reset)

	var name, about string

	// Pre-fill from existing if reconfiguring
	if existing != nil {
		name = existing.Name
		about = existing.About
	}

	runField(huh.NewInput().
		Title("Display name").
		Value(&name))

	runField(huh.NewInput().
		Title("Description").
		Value(&about))

	// Save keys
	keys := &NostrKeys{
		Nsec:    nsec,
		Npub:    npub,
		PrivHex: privHex,
		PubHex:  pubHex,
		Name:    name,
		About:   about,
		Relays:  selectedRelays,
	}

	if err := SaveNostrKeys(keys); err != nil {
		return fmt.Errorf("failed to save keys: %w", err)
	}
	fmt.Printf("\n  %s✓ Keys saved to %s%s\n", Fmt.Green, nostrKeysPath(), Fmt.Reset)

	// Publish profile
	fmt.Printf("  Publishing profile to %d relays...\n", len(selectedRelays))
	if err := PublishNostrProfile(keys); err != nil {
		Warnf("  %s⚠ %v%s", Fmt.Yellow, err, Fmt.Reset)
	} else {
		fmt.Printf("  %s✓ Profile published%s\n", Fmt.Green, Fmt.Reset)
	}

	fmt.Printf("\n  %s🎉 Nostr identity configured!%s\n", Fmt.Bold, Fmt.Reset)
	fmt.Printf("  %snpub: %s%s\n", Fmt.Dim, npub, Fmt.Reset)
	fmt.Printf("  %sRelays: %d%s\n\n", Fmt.Dim, len(selectedRelays), Fmt.Reset)

	return nil
}
