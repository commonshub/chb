package cmd

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// MirrorSource returns the value of the CHB_SYNC_SOURCE environment variable.
//
// Setting CHB_SYNC_SOURCE switches `chb` into a thin-client (mirror) mode:
// instead of running every provider locally (which needs API keys per
// teammate), the binary rsyncs from a trusted host that already did the work.
//
// Value format: `user@host:/abs/path/to/.chb` — the same syntax that ssh and
// rsync accept. Plain local paths (`/tmp/some/.chb`) work too, which keeps
// the end-to-end test loop cheap.
//
// Behaviour matrix (only applies when CHB_SYNC_SOURCE is set):
//
//   - `chb pull`      → rsync from remote (data + settings + outbox)
//                       instead of running every provider locally.
//   - `chb generate`  → no-op; the trusted host already generated.
//   - `chb push`      → only push Nostr (if a local NOSTR_SECRET_KEY exists).
//                       Refuse Odoo push: that requires credentials only the
//                       trusted host has.
//   - `chb sync`      → mirror-pull, then flush local Nostr if keys are
//                       present. Odoo writes refuse.
//   - read-only       → unchanged.
//     commands
//
// Per-invocation override: passing `--no-mirror` on `pull` / `sync` / `push`
// ignores CHB_SYNC_SOURCE. Useful for the trusted host running the same
// binary.
//
// SSH auth is delegated entirely to the user's ssh-agent / keys; chb does
// not manage credentials.
//
// Lock: a flock-protected lock file at $APP_DATA_DIR/.sync.lock is held for
// the entire rsync sequence so two concurrent mirror pulls can't corrupt the
// local copy.
//
// See docs/mirror-mode.md for the architecture rationale.
func MirrorSource() string {
	return strings.TrimSpace(os.Getenv("CHB_SYNC_SOURCE"))
}

// MirrorEnabled reports whether mirror mode is active for this invocation.
// It returns true only when CHB_SYNC_SOURCE is set AND --no-mirror is not
// present in args.
func MirrorEnabled(args []string) bool {
	if MirrorSource() == "" {
		return false
	}
	if HasFlag(args, "--no-mirror") {
		return false
	}
	return true
}

// FilterMirrorFlags strips mirror-mode-only flags from args before they
// reach sub-commands. Mirror flags are handled at the dispatcher level.
func FilterMirrorFlags(args []string) []string {
	return filterFlag(args, "--no-mirror")
}

// RequireOdooWriteCapability fails fast when mirror mode is active and the
// local environment doesn't have an Odoo password. The trusted host has the
// credentials; thin clients should refuse to attempt the write so the operator
// gets a clear "run this on the trusted host" message instead of a cryptic
// auth failure.
//
// Safe to call from every Odoo-write entry point (push, reconcile, journal
// sync, …); returns nil when not in mirror mode or when credentials are
// available, so existing single-host setups stay unchanged.
func RequireOdooWriteCapability() error {
	if MirrorSource() == "" {
		return nil
	}
	if strings.TrimSpace(os.Getenv("ODOO_PASSWORD")) != "" {
		return nil
	}
	return fmt.Errorf("Odoo writes are disabled in mirror mode (CHB_SYNC_SOURCE is set and ODOO_PASSWORD is unset).\n  ↪ Run this command on the trusted host, or unset CHB_SYNC_SOURCE locally if you have credentials")
}

// mirrorRunner is the shell-out indirection that lets tests stub rsync.
// Production code uses runRsyncDefault.
type mirrorRunner func(args []string, label string) error

var mirrorRunRsync mirrorRunner = runRsyncDefault

// runRsyncDefault execs the system rsync binary with the given args, streams
// stdout/stderr line-by-line (dim for stdout, yellow ⚠ prefix for stderr),
// and returns a non-nil error if the rsync exit code is non-zero. The label
// is used in the per-line prefix so the operator can tell which rsync
// invocation is talking.
func runRsyncDefault(args []string, label string) error {
	if _, err := exec.LookPath("rsync"); err != nil {
		return fmt.Errorf("rsync is required for mirror mode but was not found on PATH — install rsync (e.g. `sudo apt install rsync`) and retry")
	}
	cmd := exec.Command("rsync", args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("rsync stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("rsync stderr pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("rsync start: %w", err)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go streamRsyncLines(&wg, stdout, label, false)
	go streamRsyncLines(&wg, stderr, label, true)
	wg.Wait()
	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("rsync %s failed: %w", label, err)
	}
	return nil
}

func streamRsyncLines(wg *sync.WaitGroup, r io.Reader, label string, isErr bool) {
	defer wg.Done()
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		if isErr {
			fmt.Fprintf(os.Stderr, "  %s⚠ %s: %s%s\n", Fmt.Yellow, label, line, Fmt.Reset)
			continue
		}
		fmt.Printf("  %s%s: %s%s\n", Fmt.Dim, label, line, Fmt.Reset)
	}
}

// mirrorLockPath returns the path of the flock used to serialise mirror
// operations. One lock per APP_DATA_DIR is enough — a second concurrent
// `chb pull` waits for the first to finish before starting its own rsync.
func mirrorLockPath() string {
	return filepath.Join(AppDataDir(), ".sync.lock")
}

// withMirrorLock acquires an exclusive flock on the mirror lock file, runs
// fn, and releases the lock. Blocking — concurrent callers wait. The lock
// file is created if missing; we never delete it.
func withMirrorLock(fn func() error) error {
	if err := os.MkdirAll(AppDataDir(), 0755); err != nil {
		return err
	}
	path := mirrorLockPath()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("open mirror lock: %w", err)
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("acquire mirror lock: %w", err)
	}
	defer func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN) }()
	return fn()
}

// mirrorRemoteSubpath joins a sub-path onto the rsync source spec. Handles
// both `user@host:/abs` and plain `/abs` forms. The result is always
// terminated with the source segment caller passed (no implicit slash).
func mirrorRemoteSubpath(base, sub string) string {
	base = strings.TrimRight(base, "/")
	sub = strings.TrimLeft(sub, "/")
	if sub == "" {
		return base + "/"
	}
	return base + "/" + sub
}

// localSubpath joins a sub-path onto AppDataDir.
func localSubpath(sub string) string {
	dir := AppDataDir()
	sub = strings.TrimLeft(sub, "/")
	return filepath.Join(dir, sub)
}

// MirrorPull runs the read-side of mirror mode: it rsyncs the trusted host's
// data/ tree into the local AppDataDir and prints a compact summary. Returns
// nil when CHB_SYNC_SOURCE is unset (a no-op so the caller can invoke it
// unconditionally inside main.go's dispatch).
//
// Phase 1 scope: only data/ is mirrored (authoritative pull, --delete).
// Outbox + settings handling land in Phase 2 / Phase 3.
func MirrorPull(args []string) error {
	if !MirrorEnabled(args) {
		return nil
	}
	src := MirrorSource()
	verbose := HasFlag(args, "--verbose", "-v") || HasFlag(args, "--debug")
	started := time.Now()
	fmt.Printf("\n%sMirroring from %s%s\n\n", Fmt.Bold, src, Fmt.Reset)
	if _, err := exec.LookPath("rsync"); err != nil {
		return fmt.Errorf("rsync is required for mirror mode but was not found on PATH — install rsync (e.g. `sudo apt install rsync`) and retry")
	}
	err := withMirrorLock(func() error {
		// data/ — authoritative pull. The trusted host always wins.
		return mirrorRsyncData(src, verbose)
	})
	elapsed := time.Since(started).Round(100 * time.Millisecond)
	if err != nil {
		fmt.Fprintf(os.Stderr, "\n%s✗ Mirror pull failed after %s: %v%s\n\n", Fmt.Red, FormatElapsedFixed(elapsed), err, Fmt.Reset)
		return err
	}
	fmt.Printf("\n%s✓ Mirrored in %s%s\n\n", Fmt.Green, FormatElapsedFixed(elapsed), Fmt.Reset)
	UpdateSyncSource("pull", false)
	UpdateSyncActivity(false)
	return nil
}

// mirrorRsyncData pulls $remote/data/ → $local/data/ with --delete. The
// trusted host owns the canonical generated state; locally-modified files
// in data/ are blown away on every pull. This is intentional: data/ is a
// pure read-only mirror of provider output.
func mirrorRsyncData(src string, verbose bool) error {
	remote := mirrorRemoteSubpath(src, "data") + "/"
	local := localSubpath("data") + "/"
	if err := os.MkdirAll(local, 0755); err != nil {
		return err
	}
	rsyncArgs := baseRsyncFlags(verbose)
	rsyncArgs = append(rsyncArgs, "--delete", remote, local)
	return mirrorRunRsync(rsyncArgs, "data")
}

// PrintMirrorGenerateSkipped prints the one-liner shown when `chb generate`
// is invoked in mirror mode. The trusted host already generated everything;
// running generate locally would only fight that state.
func PrintMirrorGenerateSkipped() {
	fmt.Printf("\n  %s↳ skipped: CHB_SYNC_SOURCE is set; remote already generated%s\n\n",
		Fmt.Dim, Fmt.Reset)
}

// MirrorPushNostrOnly is the push-side of mirror mode: Odoo is refused
// (the trusted host owns it), but local Nostr annotations are signed and
// pushed up so other teammates' relays see them too. If no local Nostr
// keys are configured, this is effectively a no-op with a friendly hint.
func MirrorPushNostrOnly(args []string) error {
	if MirrorSource() == "" {
		// Defensive: callers should already check MirrorEnabled, but
		// guard anyway so an accidental direct call doesn't change
		// behaviour for normal users.
		return nil
	}
	verbose := HasFlag(args, "--verbose", "-v") || HasFlag(args, "--debug")
	keys := LoadNostrKeys()
	if keys == nil || strings.TrimSpace(keys.PrivHex) == "" {
		fmt.Printf("\n  %s↳ Odoo push skipped: no credentials on this host (run on the trusted host instead).%s\n", Fmt.Dim, Fmt.Reset)
		fmt.Printf("  %s↳ Nostr push skipped: no local Nostr keys configured (run `chb setup nostr`).%s\n\n", Fmt.Dim, Fmt.Reset)
		return nil
	}
	fmt.Printf("\n  %sMirror push — Nostr only%s\n", Fmt.Bold, Fmt.Reset)
	fmt.Printf("  %s↳ Odoo push skipped: no credentials on this host.%s\n", Fmt.Dim, Fmt.Reset)
	if verbose {
		return NostrPush(args)
	}
	// Compact: silence chatter, just print a one-line summary.
	restore := silenceStdout()
	err := NostrPush(args)
	restore()
	if err != nil {
		fmt.Fprintf(os.Stderr, "  %s✗ Nostr push: %v%s\n", Fmt.Red, err, Fmt.Reset)
		return err
	}
	fmt.Printf("  %s✓ Nostr outbox flushed%s\n\n", Fmt.Green, Fmt.Reset)
	return nil
}

// baseRsyncFlags returns the flag set every mirror rsync uses. `--archive`
// preserves timestamps/permissions; `--safe-links` refuses to follow links
// that point outside the transfer; `--info=progress2` keeps the streamed
// stdout informative without scrolling per-file noise.
func baseRsyncFlags(verbose bool) []string {
	flags := []string{
		"--archive",
		"--safe-links",
		"--partial",
		"--human-readable",
	}
	if verbose {
		flags = append(flags, "--verbose")
	} else {
		flags = append(flags, "--info=stats1,flist0,progress0")
	}
	return flags
}
