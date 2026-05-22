package cmd

import (
	"encoding/json"
	"fmt"
	"os"
)

// JSONMode reports whether the operator wants machine-readable output.
// True when:
//
//   - the --json flag is explicitly passed, OR
//   - stdout isn't a terminal (i.e. the command is being piped or
//     redirected) AND --text / --pretty isn't passed to opt out.
//
// The auto-detect arm makes `chb transactions | odoo attach …` and
// `chb transactions > out.jsonl` Just Work without --json. Callers
// that want to force the pretty-print path despite being piped can
// pass --text.
func JSONMode(args []string) bool {
	if HasFlag(args, "--text", "--pretty") {
		return false
	}
	if HasFlag(args, "--json") {
		return true
	}
	return !isStdoutTTY()
}

// isStdoutTTY reports whether os.Stdout is connected to a terminal
// (i.e. NOT piped or redirected). Mirrors isInteractiveTTY which
// checks stdin; this side is what matters for output formatting.
func isStdoutTTY() bool {
	info, err := os.Stdout.Stat()
	if err != nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) != 0
}

// EmitJSONL writes v to stdout as one JSON object on a single line,
// followed by a newline. Caller is responsible for calling this once
// per record. No array wrapper, no indentation — the line-delimited
// JSON convention that pipes into jq / odoo-cli / etc. cleanly.
func EmitJSONL(v interface{}) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetEscapeHTML(false)
	return enc.Encode(v)
}

// EmitJSON writes v to stdout as indented JSON followed by a newline.
// Use after work is done; pretty-print output should be silenced or
// redirected to stderr while a command is running in JSON mode.
func EmitJSON(v interface{}) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// EmitJSONError writes a structured error to stdout when in JSON mode and
// returns an error code-friendly message. Used by commands that want to
// surface failures in machine-readable form.
func EmitJSONError(err error) {
	_ = EmitJSON(map[string]string{"error": err.Error()})
	fmt.Fprintln(os.Stderr, err)
}

// CaptureStdout redirects os.Stdout to dst for the duration of fn, then
// restores it. Used so commands invoked in --json mode can run their
// existing fmt.Print* progress output to stderr while the JSON payload
// stays clean on stdout.
func CaptureStdout(dst *os.File, fn func()) {
	orig := os.Stdout
	os.Stdout = dst
	defer func() { os.Stdout = orig }()
	fn()
}
