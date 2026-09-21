package cmd

import (
	"testing"

	"github.com/CommonsHub/chb/ical"
)

func TestExtractEventURLOnlyTrustsTheURLField(t *testing.T) {
	withField := ical.Event{URL: "https://luma.com/xh70cr0g", Description: "see https://example.org/other"}
	if got := extractEventURL(withField); got != "https://luma.com/xh70cr0g" {
		t.Fatalf("got %q, want the URL field", got)
	}
	inDescription := ical.Event{Description: "Paid booking. <a href=\"https://luma.com/xh70cr0g\">https://luma.com/xh70cr0g</a>"}
	if got := extractEventURL(inDescription); got != "" {
		t.Fatalf("a link in the description is not the event's URL, got %q", got)
	}
	inLocation := ical.Event{Location: "https://www.lesateliersdumidi.be/"}
	if got := extractEventURL(inLocation); got != "" {
		t.Fatalf("a link in the location is not the event's URL, got %q", got)
	}
	if autoCalendarEventHasPublicURL(inDescription) {
		t.Fatal("an entry without a URL field is a booking, not a public event")
	}
}
