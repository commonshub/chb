package cmd

import (
	"strings"
	"testing"
	"time"

	"github.com/CommonsHub/chb/providers/mobilizon"
)

var mobilizonNow = time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

func lumaEvent(slug, name, start string) FullEvent {
	return FullEvent{
		ID:             "evt-" + slug + "@events.lu.ma",
		Name:           name,
		Description:    "First line\nsecond line\n\nMore <soon>…",
		StartAt:        start,
		EndAt:          start[:11] + "23:00:00+02:00",
		Location:       "Commons Hub Brussels, Rue de la Madeleine 51, 1000 Bruxelles, Belgium",
		URL:            "https://luma.com/" + slug,
		CalendarSource: "luma",
	}
}

func actionsByKey(p *MobilizonPendingFile) map[string]MobilizonAction {
	out := map[string]MobilizonAction{}
	for _, a := range p.Actions {
		out[a.Key] = a
	}
	return out
}

func TestPlanMobilizonCreatesUpcomingLumaEvents(t *testing.T) {
	events := []FullEvent{
		lumaEvent("new1", "Potluck", "2026-10-02T12:30:00+02:00"),
		lumaEvent("past", "Yesterday", "2026-09-25T12:30:00+02:00"),
		{ID: "g1", Name: "Google only", StartAt: "2026-10-03T10:00:00+02:00", URL: "https://example.org/x", CalendarSource: "google"},
	}
	p := planMobilizon(events, nil, nil, nil, mobilizonNow)
	if len(p.Actions) != 1 || p.Actions[0].Action != "create" || p.Actions[0].Key != "https://luma.com/new1" {
		t.Fatalf("want one create for new1, got %+v", p.Actions)
	}
	a := p.Actions[0]
	if a.BeginsOn != "2026-10-02T10:30:00Z" {
		t.Errorf("beginsOn = %s, want UTC", a.BeginsOn)
	}
	if a.Address == nil || a.Address.Geom != hubAddress.Geom {
		t.Errorf("hub location should map to the hub address, got %+v", a.Address)
	}
}

func TestPlanMobilizonUpdatesOnlyWhenChanged(t *testing.T) {
	ev := lumaEvent("same", "Talk", "2026-10-02T18:00:00+02:00")
	published := map[string]MobilizonPublished{
		"https://luma.com/same": {MobilizonID: "7", UUID: "u7", Hash: mobilizonActionFor(ev).Hash},
	}
	if p := planMobilizon([]FullEvent{ev}, nil, published, nil, mobilizonNow); len(p.Actions) != 0 {
		t.Fatalf("unchanged event should not be planned, got %+v", p.Actions)
	}
	ev.Name = "Talk (moved)"
	p := planMobilizon([]FullEvent{ev}, nil, published, nil, mobilizonNow)
	if len(p.Actions) != 1 || p.Actions[0].Action != "update" || p.Actions[0].MobilizonID != "7" {
		t.Fatalf("want update of 7, got %+v", p.Actions)
	}
}

func TestPlanMobilizonAdoptsEventWithSameLumaLink(t *testing.T) {
	ev := lumaEvent("abc", "Walk", "2026-10-02T16:00:00+02:00")
	remote := []mobilizon.Event{{ID: "9", UUID: "u9", Title: "Walk", ExternalParticipationURL: "https://lu.ma/abc"}}
	a := actionsByKey(planMobilizon([]FullEvent{ev}, remote, nil, nil, mobilizonNow))["https://luma.com/abc"]
	if a.Action != "update" || a.MobilizonID != "9" {
		t.Fatalf("want update of the existing event, got %+v", a)
	}
}

func TestPlanMobilizonSkipsEventPostedByHand(t *testing.T) {
	ev := lumaEvent("did", "Digital Independence Day", "2026-10-04T12:00:00+02:00")
	remote := []mobilizon.Event{{ID: "1", UUID: "u1", Title: "[EN] Digital Independence Day", BeginsOn: "2026-10-04T10:00:00Z", ExternalParticipationURL: "https://luma.com/other"}}
	p := planMobilizon([]FullEvent{ev}, remote, nil, nil, mobilizonNow)
	if len(p.Actions) != 0 || len(p.Skipped) != 1 || p.Skipped[0].UUID != "u1" {
		t.Fatalf("want the event skipped, got actions %+v skipped %+v", p.Actions, p.Skipped)
	}
}

func TestPlanMobilizonCancelsOnlyWithinSyncedMonths(t *testing.T) {
	published := map[string]MobilizonPublished{
		"https://luma.com/gone":    {MobilizonID: "1", UUID: "u1", Title: "Gone", BeginsOn: "2026-10-09T10:30:00Z"},
		"https://luma.com/later":   {MobilizonID: "2", UUID: "u2", Title: "Later", BeginsOn: "2027-03-05T10:30:00Z"},
		"https://luma.com/over":    {MobilizonID: "3", UUID: "u3", Title: "Over", BeginsOn: "2026-09-01T10:30:00Z"},
		"https://luma.com/already": {MobilizonID: "4", UUID: "u4", Title: "Already", BeginsOn: "2026-10-10T10:30:00Z", Cancelled: true},
	}
	p := planMobilizon(nil, nil, published, map[string]bool{"2026-10": true, "2026-09": true}, mobilizonNow)
	if len(p.Actions) != 1 || p.Actions[0].Action != "cancel" || p.Actions[0].MobilizonID != "1" {
		t.Fatalf("want only 'gone' cancelled, got %+v", p.Actions)
	}
}

func TestMobilizonEventKey(t *testing.T) {
	cases := map[string]string{
		"https://lu.ma/abc":          "https://luma.com/abc",
		"https://www.luma.com/abc/":  "https://luma.com/abc",
		"https://luma.com/abc?tk=1":  "https://luma.com/abc",
		"https://luma.com/event/x/y": "",
		"https://example.org/abc":    "",
		"":                           "",
	}
	for in, want := range cases {
		if got := mobilizonEventKey(in); got != want {
			t.Errorf("mobilizonEventKey(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestMobilizonAddressElsewhere(t *testing.T) {
	a := mobilizonAddress("Smart cooperative, Rue Coenraets 72, 1060 Saint-Gilles, Belgium")
	want := mobilizon.Address{Description: "Smart cooperative", Street: "Rue Coenraets 72", PostalCode: "1060", Locality: "Saint-Gilles", Country: "Belgium", Timezone: "Europe/Brussels"}
	if a == nil || *a != want {
		t.Fatalf("got %+v, want %+v", a, want)
	}
	if mobilizonAddress("") != nil {
		t.Error("empty location should give no address")
	}
}

func TestMobilizonDescription(t *testing.T) {
	got := mobilizonDescription("First line\nsecond line\n\nMore <soon>…", "https://luma.com/x")
	for _, want := range []string{
		"<p>First line<br>second line</p>",
		"<p>More &lt;soon&gt;…</p>",
		`Full description and registration on Luma: <a href="https://luma.com/x">`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("description missing %q:\n%s", want, got)
		}
	}
	if full := mobilizonDescription("Complete.", "https://luma.com/x"); !strings.Contains(full, "<p>Registration on Luma:") {
		t.Errorf("complete description should only point to registration:\n%s", full)
	}
}

func TestMobilizonDraftsToPublish(t *testing.T) {
	published := &MobilizonPublishedFile{Events: map[string]MobilizonPublished{
		"https://luma.com/a": {MobilizonID: "1", Draft: true},
		"https://luma.com/b": {MobilizonID: "2", Draft: true},
		"https://luma.com/c": {MobilizonID: "3"},
	}}
	got := mobilizonDraftsToPublish(published, []MobilizonAction{{Key: "https://luma.com/b"}})
	if len(got) != 1 || got[0].MobilizonID != "1" || got[0].Action != "publish" {
		t.Fatalf("want publish of a only, got %+v", got)
	}
}
