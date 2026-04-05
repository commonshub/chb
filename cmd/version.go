package cmd

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"strings"
	"time"
)

// Build-time variables — can be injected via ldflags, but also
// auto-detected from Go build info (VCS stamps, pseudo-versions).
var (
	Version    string // set from main.go
	CommitSHA  string
	CommitDate string
	CommitMsg  string
)

func init() {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return
	}
	// VCS info from local git build
	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision":
			if CommitSHA == "" {
				CommitSHA = s.Value
			}
		case "vcs.time":
			if CommitDate == "" {
				if t, err := time.Parse(time.RFC3339, s.Value); err == nil {
					CommitDate = t.Format("2006-01-02 15:04")
				} else {
					CommitDate = s.Value
				}
			}
		}
	}
	// Pseudo-version fallback (from go install)
	if CommitSHA == "" && bi.Main.Version != "" && bi.Main.Version != "(devel)" {
		parts := strings.Split(bi.Main.Version, "-")
		if len(parts) == 3 {
			CommitSHA = parts[2]
			if t, err := time.Parse("20060102150405", parts[1]); err == nil {
				CommitDate = t.Format("2006-01-02 15:04")
			}
		}
	}
}

// PrintVersion prints version info to stdout.
func PrintVersion() {
	f := Fmt
	short := CommitSHA
	if len(short) > 7 {
		short = short[:7]
	}

	fmt.Printf("chb %s%s%s", f.Bold, Version, f.Reset)
	if short != "" {
		fmt.Printf(" %s(%s, %s)%s", f.Dim, short, CommitDate, f.Reset)
	}
	fmt.Println()
	fmt.Printf("  %sOS:%s    %s/%s\n", f.Cyan, f.Reset, runtime.GOOS, runtime.GOARCH)
	if CommitMsg != "" {
		fmt.Printf("  %sCommit:%s %s\n", f.Cyan, f.Reset, firstLine(CommitMsg))
	}
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}
