package cmd

import "testing"

func TestResolvedVersionPrefersInjectedVersion(t *testing.T) {
	got := resolvedVersion("2.3.4", "v2.3.3")
	if got != "2.3.4" {
		t.Fatalf("expected injected version, got %q", got)
	}
}

func TestResolvedVersionFallsBackToBuildInfoVersion(t *testing.T) {
	got := resolvedVersion("", "v2.3.3")
	if got != "2.3.3" {
		t.Fatalf("expected build info version, got %q", got)
	}
}

func TestResolvedVersionFallsBackToDev(t *testing.T) {
	got := resolvedVersion("", "(devel)")
	if got != "dev" {
		t.Fatalf("expected dev fallback, got %q", got)
	}
}

func TestNormalizeVersionStripsPrefix(t *testing.T) {
	got := normalizeVersion("v2.3.3")
	if got != "2.3.3" {
		t.Fatalf("expected stripped version, got %q", got)
	}
}
