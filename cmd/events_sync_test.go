package cmd

import (
	"testing"

	"github.com/CommonsHub/chb/og"
)

func TestStoreOGResultCachesPositiveImageResult(t *testing.T) {
	runCache := &eventSyncRunCache{
		og: &eventOGCache{Entries: map[string]eventOGCacheItem{}},
	}
	url := "https://luma.com/u3kbetd4"
	result := og.FetchResult{
		URL:  url,
		Meta: og.Meta{Title: "Event", Image: "https://images.example.com/event.jpg"},
	}

	runCache.storeOGResult(url, result)

	cached, ok := runCache.getCachedOGResult(url)
	if !ok {
		t.Fatal("expected positive og:image result to be cached")
	}
	if cached.Meta.Image != result.Meta.Image {
		t.Fatalf("expected cached image %q, got %q", result.Meta.Image, cached.Meta.Image)
	}
}

func TestStoreOGResultDoesNotCacheMissingImage(t *testing.T) {
	runCache := &eventSyncRunCache{
		og: &eventOGCache{Entries: map[string]eventOGCacheItem{}},
	}
	url := "https://luma.com/u3kbetd4"
	result := og.FetchResult{
		URL:       url,
		HTMLTitle: "OpenClaworking Day",
		Meta:      og.Meta{Title: "OpenClaworking Day", Description: "Coworking day"},
	}

	runCache.storeOGResult(url, result)

	if _, ok := runCache.getCachedOGResult(url); ok {
		t.Fatal("expected missing-image result to not be cached")
	}
}

func TestStoreOGResultRemovesExistingNegativeOrStaleEntry(t *testing.T) {
	runCache := &eventSyncRunCache{
		og: &eventOGCache{
			Entries: map[string]eventOGCacheItem{
				"https://luma.com/u3kbetd4": {
					Title:     strPtr("OpenClaworking Day"),
					CheckedAt: "2026-04-15T12:00:00Z",
				},
			},
		},
	}
	url := "https://luma.com/u3kbetd4"
	result := og.FetchResult{
		URL:          url,
		ErrorKind:    "request_failed",
		ErrorMessage: "timeout",
	}

	runCache.storeOGResult(url, result)

	if _, ok := runCache.og.Entries[url]; ok {
		t.Fatal("expected non-cacheable result to remove existing cached entry")
	}
}

func strPtr(s string) *string {
	return &s
}
