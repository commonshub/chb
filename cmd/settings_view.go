package cmd

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/aymanbagabas/go-udiff"
)

// PrintSettings is the entrypoint for `chb settings`. It lists the files in
// APP_DATA_DIR/settings/ with a one-line summary each, then shows a diff
// for any pending update where the embedded default has changed and the
// user has local edits.
func PrintSettings() {
	dir := AppSettingsDir()
	entries, err := os.ReadDir(dir)
	if err != nil {
		fmt.Printf("%sSettings dir unavailable: %v%s\n", Fmt.Yellow, err, Fmt.Reset)
		return
	}

	var settings *Settings
	for _, e := range entries {
		if !e.IsDir() && e.Name() == "settings.json" {
			settings, _ = LoadSettings()
			break
		}
	}

	fmt.Printf("\n%sSettings:%s %s\n", Fmt.Bold, Fmt.Reset, dir)

	files := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.HasPrefix(e.Name(), ".") {
			continue
		}
		files = append(files, e.Name())
	}
	if len(files) == 0 {
		fmt.Printf("  %s(empty — nothing configured yet)%s\n\n", Fmt.Dim, Fmt.Reset)
		return
	}
	sort.Strings(files)

	width := 0
	for _, f := range files {
		if len(f) > width {
			width = len(f)
		}
	}

	pending := pendingUpdatesByName()
	for _, name := range files {
		summary := summarizeSettingsFile(name, settings)
		marker := fmt.Sprintf("%s•%s", Fmt.Dim, Fmt.Reset)
		if _, ok := pending[name]; ok {
			marker = fmt.Sprintf("%s↑%s", Fmt.Yellow, Fmt.Reset)
		}
		fmt.Printf("  %s %-*s  %s%s%s\n",
			marker, width, name, Fmt.Dim, summary, Fmt.Reset)
	}
	fmt.Println()

	updates := PendingSettingsUpdates()
	if len(updates) == 0 {
		return
	}
	fmt.Printf("%s%s from the bundled defaults:%s\n\n",
		Fmt.Yellow, Pluralize(len(updates), "pending update", ""), Fmt.Reset)
	for _, u := range updates {
		printPendingUpdate(u)
	}
}

func pendingUpdatesByName() map[string]PendingDefaultUpdate {
	out := map[string]PendingDefaultUpdate{}
	for _, u := range PendingSettingsUpdates() {
		out[u.Name] = u
	}
	return out
}

func printPendingUpdate(u PendingDefaultUpdate) {
	fmt.Printf("  %s%s%s  %s(local edits prevent auto-update)%s\n",
		Fmt.Bold, u.Name, Fmt.Reset, Fmt.Dim, Fmt.Reset)

	edits := udiff.Strings(string(u.LocalContent), string(u.UpstreamBytes))
	unified, err := udiff.ToUnified(
		"local/"+u.Name,
		"bundled/"+u.Name,
		string(u.LocalContent),
		edits,
		3,
	)
	if err != nil || unified == "" {
		fmt.Printf("    %s(no textual diff available)%s\n\n", Fmt.Dim, Fmt.Reset)
		return
	}
	for _, line := range strings.Split(strings.TrimRight(unified, "\n"), "\n") {
		switch {
		case strings.HasPrefix(line, "+"):
			fmt.Printf("    %s%s%s\n", Fmt.Green, line, Fmt.Reset)
		case strings.HasPrefix(line, "-"):
			fmt.Printf("    %s%s%s\n", Fmt.Red, line, Fmt.Reset)
		case strings.HasPrefix(line, "@@"):
			fmt.Printf("    %s%s%s\n", Fmt.Cyan, line, Fmt.Reset)
		default:
			fmt.Printf("    %s\n", line)
		}
	}
	fmt.Println()
	fmt.Printf("    %sTo adopt the new defaults, replace your local file or merge by hand.%s\n\n",
		Fmt.Dim, Fmt.Reset)
}
