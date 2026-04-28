package cmd

import (
	"encoding/json"
	"fmt"
	"os"
)

// JSONMode reports whether the global --json flag was passed.
func JSONMode(args []string) bool {
	return HasFlag(args, "--json")
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
