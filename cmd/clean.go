package cmd

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	etherscansource "github.com/CommonsHub/chb/providers/etherscan"
)

// Clean brings the local data directory up to the current on-disk conventions:
//
//  1. Renames legacy Etherscan archives {slug}.{token}.json to the
//     address-qualified {slug}.{0xaddr}.{token}.json layout, deriving the short
//     address from the "account" field stored inside each file.
//  2. Removes legacy `sources/` directories that have been superseded by
//     `providers/` (the rename that happened when sources became providers).
//
// It is non-destructive by default beyond what is described above, prints the
// full plan first, and only touches a `sources/` directory when a sibling
// `providers/` directory exists.
func Clean(args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		printCleanHelp()
		return nil
	}
	dryRun := HasFlag(args, "--dry-run", "-n")
	assumeYes := HasFlag(args, "--yes", "-y")

	dataDir := DataDir()
	f := Fmt

	fmt.Printf("\n%s🧹 chb clean%s  %s%s%s\n\n", f.Bold, f.Reset, f.Dim, dataDir, f.Reset)

	renames, skipped := planEtherscanRenames(dataDir)
	staleDirs := planStaleSourceDirs(dataDir)
	stalePeeks := planStalePeekFiles(dataDir)

	if len(renames) == 0 && len(staleDirs) == 0 && len(stalePeeks) == 0 && len(skipped) == 0 {
		fmt.Printf("  %s✓ Nothing to clean — everything is already up to date.%s\n\n", f.Green, f.Reset)
		return nil
	}

	if len(renames) > 0 {
		fmt.Printf("  %sRename %d Etherscan file(s) → {slug}.{0xaddr}.{token}.json (under the owning account):%s\n", f.Bold, len(renames), f.Reset)
		for _, r := range renames {
			fmt.Printf("    %s%s%s\n      → %s%s%s\n", f.Dim, r.relFrom, f.Reset, f.Cyan, filepath.Base(r.to), f.Reset)
		}
		fmt.Println()
	}

	if len(skipped) > 0 {
		for _, s := range skipped {
			fmt.Printf("  %s⚠ skip %s%s\n", f.Yellow, s, f.Reset)
		}
		fmt.Println()
	}

	if len(staleDirs) > 0 {
		fmt.Printf("  %sRemove %d legacy sources/ director(ies) (replaced by providers/):%s\n", f.Bold, len(staleDirs), f.Reset)
		for _, d := range staleDirs {
			rel, err := filepath.Rel(dataDir, d)
			if err != nil {
				rel = d
			}
			fmt.Printf("    %s- %s%s\n", f.Dim, rel, f.Reset)
		}
		fmt.Println()
	}

	if len(stalePeeks) > 0 {
		fmt.Printf("  %sRemove %d stray .peek-* checkpoint(s) from the provider tree (now under latest/.cache/):%s\n", f.Bold, len(stalePeeks), f.Reset)
		for _, p := range stalePeeks {
			rel, err := filepath.Rel(dataDir, p)
			if err != nil {
				rel = p
			}
			fmt.Printf("    %s- %s%s\n", f.Dim, rel, f.Reset)
		}
		fmt.Println()
	}

	if dryRun {
		fmt.Printf("  %s(dry run — no changes made)%s\n\n", f.Dim, f.Reset)
		return nil
	}

	if !assumeYes {
		fmt.Printf("  %sProceed? [y/N] %s", f.Bold, f.Reset)
		reader := bufio.NewReader(os.Stdin)
		ans, _ := reader.ReadString('\n')
		if a := strings.ToLower(strings.TrimSpace(ans)); a != "y" && a != "yes" {
			fmt.Printf("  %sAborted.%s\n\n", f.Yellow, f.Reset)
			return nil
		}
	}

	renamed := 0
	for _, r := range renames {
		if err := os.Rename(r.from, r.to); err != nil {
			Errorf("    %s✗ rename %s: %v%s", f.Red, r.relFrom, err, f.Reset)
			continue
		}
		renamed++
	}

	removed := 0
	for _, d := range staleDirs {
		if err := os.RemoveAll(d); err != nil {
			Errorf("    %s✗ remove %s: %v%s", f.Red, d, err, f.Reset)
			continue
		}
		removed++
	}

	peeksRemoved := 0
	for _, p := range stalePeeks {
		if err := os.Remove(p); err != nil {
			Errorf("    %s✗ remove %s: %v%s", f.Red, p, err, f.Reset)
			continue
		}
		peeksRemoved++
	}

	fmt.Printf("  %s✓ Renamed %d file(s), removed %d director(ies) and %d stray checkpoint(s).%s\n\n", f.Green, renamed, removed, peeksRemoved, f.Reset)
	return nil
}

type etherscanRename struct {
	from    string
	to      string
	relFrom string
}

// etherscanAddressOwners maps a lowercased wallet address to the slug of the
// configured Etherscan account that currently owns it. Used by `chb clean` to
// re-file cache files under the account that owns the wallet — e.g. after a
// wallet migration where the old address moved to a different account.
func etherscanAddressOwners() map[string]string {
	owners := map[string]string{}
	for _, acc := range LoadAccountConfigs() {
		if acc.Provider != "etherscan" || strings.TrimSpace(acc.Address) == "" {
			continue
		}
		owners[strings.ToLower(strings.TrimSpace(acc.Address))] = acc.Slug
	}
	return owners
}

// planEtherscanRenames scans the Etherscan archives and returns the renames
// needed so every file is named {slug}.{0xaddr}.{token}.json where slug is the
// account that currently owns the embedded wallet address. This both adds the
// address to legacy {slug}.{token}.json files and re-files address-qualified
// files whose slug no longer matches the owning account. The second return is
// a list of human-readable reasons for files that can't be converted.
func planEtherscanRenames(dataDir string) (renames []etherscanRename, skipped []string) {
	owners := etherscanAddressOwners()
	_ = filepath.WalkDir(dataDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			// Don't descend into legacy sources/ trees — they're removed
			// wholesale below, no point renaming files inside them.
			if d.Name() == "sources" {
				return filepath.SkipDir
			}
			return nil
		}
		name := d.Name()
		if !strings.HasSuffix(name, ".json") {
			return nil
		}
		// Must live under providers/etherscan/<chain>/.
		chainDir := filepath.Dir(path)
		if filepath.Base(filepath.Dir(chainDir)) != etherscansource.Source {
			return nil
		}

		// Extract (slug, token) from either the legacy {slug}.{token}.json
		// (3 dot-parts) or the qualified {slug}.{addr}.{token}.json (4 parts).
		// Anything else (e.g. a bare CHT.json) is left untouched.
		parts := strings.Split(name, ".")
		var slug, token string
		switch len(parts) {
		case 3:
			slug, token = parts[0], parts[1]
		case 4:
			slug, token = parts[0], parts[2]
		default:
			return nil
		}

		rel, relErr := filepath.Rel(dataDir, path)
		if relErr != nil {
			rel = path
		}

		cache, ok := etherscansource.LoadCache(path)
		if !ok {
			skipped = append(skipped, fmt.Sprintf("%s (unreadable)", rel))
			return nil
		}
		addr := strings.TrimSpace(cache.Account)
		if addr == "" {
			skipped = append(skipped, fmt.Sprintf("%s (no account address in file)", rel))
			return nil
		}

		// Re-file under the account that currently owns this wallet; fall
		// back to the existing slug when no configured account claims it.
		ownerSlug := slug
		if owner, found := owners[strings.ToLower(addr)]; found {
			ownerSlug = owner
		}

		newName := etherscansource.FileName(ownerSlug, addr, token)
		if newName == name {
			return nil // already correctly named
		}
		to := filepath.Join(chainDir, newName)
		if _, statErr := os.Stat(to); statErr == nil {
			skipped = append(skipped, fmt.Sprintf("%s (target %s already exists)", rel, newName))
			return nil
		}
		renames = append(renames, etherscanRename{from: path, to: to, relFrom: rel})
		return nil
	})
	return renames, skipped
}

// planStalePeekFiles returns the `.peek-*` sync-checkpoint dotfiles left in the
// providers/etherscan archive tree by older versions. Peek checkpoints now live
// under latest/.cache/; these strays are pruned (they regenerate on next sync).
func planStalePeekFiles(dataDir string) []string {
	var files []string
	_ = filepath.WalkDir(dataDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			if d.Name() == "sources" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasPrefix(d.Name(), ".peek-") {
			return nil
		}
		// Only inside providers/etherscan/<chain>/.
		if filepath.Base(filepath.Dir(filepath.Dir(path))) == etherscansource.Source {
			files = append(files, path)
		}
		return nil
	})
	return files
}

// planStaleSourceDirs returns every `sources/` directory that has a sibling
// `providers/` directory (i.e. the old name that has since been replaced).
func planStaleSourceDirs(dataDir string) []string {
	var dirs []string
	_ = filepath.WalkDir(dataDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || !d.IsDir() {
			return nil
		}
		if d.Name() != "sources" {
			return nil
		}
		// Only remove when superseded by a sibling providers/ tree.
		if info, statErr := os.Stat(filepath.Join(filepath.Dir(path), "providers")); statErr == nil && info.IsDir() {
			dirs = append(dirs, path)
		}
		return filepath.SkipDir
	})
	return dirs
}

func printCleanHelp() {
	f := Fmt
	fmt.Printf(`
%schb clean%s — Bring the local data directory up to current conventions

%sUSAGE%s
  %schb clean%s [options]

%sWHAT IT DOES%s
  • Names Etherscan archives %s{slug}.{0xaddr}.{token}.json%s (e.g.
    %ssavings.0x6fdf-2cbf.EURe.json%s), deriving the address from the "account"
    field inside each file and re-filing each under the account that currently
    owns that wallet (handles wallet migrations where an address moved accounts).
  • Removes legacy %ssources/%s directories replaced by %sproviders/%s (only
    when a sibling providers/ directory exists).
  • Prunes stray %s.peek-*%s checkpoint dotfiles from the provider tree (they
    now live under latest/.cache/ and regenerate on the next sync).

%sOPTIONS%s
  %s--dry-run, -n%s   Show the plan without changing anything
  %s--yes, -y%s       Skip the confirmation prompt
  %s--help, -h%s      Show this help

%sEXAMPLES%s
  %s$ chb clean --dry-run%s     # preview the changes
  %s$ chb clean%s               # apply after confirming
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Bold, f.Reset,
		f.Dim, f.Reset,
		f.Dim, f.Reset,
	)
}
