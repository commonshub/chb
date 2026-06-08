package cmd

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestRunWithTimeout(t *testing.T) {
	// Fast success → returns nil.
	if err := runWithTimeout("x", time.Second, func() error { return nil }); err != nil {
		t.Fatalf("fast success: %v", err)
	}
	// Fast error → propagates the fn's error verbatim.
	if err := runWithTimeout("x", time.Second, func() error { return errors.New("boom") }); err == nil || err.Error() != "boom" {
		t.Fatalf("fast error: got %v", err)
	}
	// Slow fn → a clear timeout error so the caller can move on.
	err := runWithTimeout("Nostr push", 20*time.Millisecond, func() error {
		time.Sleep(500 * time.Millisecond)
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "did not respond within") {
		t.Fatalf("slow fn should time out, got %v", err)
	}
	if !strings.Contains(err.Error(), "Nostr push") {
		t.Fatalf("timeout error should name the step: %v", err)
	}
}
