package og

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestExtractMeta(t *testing.T) {
	tests := []struct {
		name string
		html string
		want Meta
	}{
		{
			name: "luma style",
			html: `<html><head>
				<meta property="og:title" content="Regen day: Body Oracle · Luma" />
				<meta property="og:description" content="A body oracle session at Commons Hub" />
				<meta name="image" property="og:image" content="https://og.luma.com/event-cover.jpg" />
			</head></html>`,
			want: Meta{
				Title:       "Regen day: Body Oracle · Luma",
				Description: "A body oracle session at Commons Hub",
				Image:       "https://og.luma.com/event-cover.jpg",
			},
		},
		{
			name: "eventbrite style",
			html: `<html><head>
				<meta property="og:title" content="Tech Meetup Brussels" />
				<meta property="og:description" content="Join us for a tech meetup" />
				<meta property="og:image" content="https://img.evbuc.com/cover.jpg" />
			</head></html>`,
			want: Meta{
				Title:       "Tech Meetup Brussels",
				Description: "Join us for a tech meetup",
				Image:       "https://img.evbuc.com/cover.jpg",
			},
		},
		{
			name: "meetup style",
			html: `<html><head>
				<meta property="og:site_name" content="Meetup" />
				<meta property="og:type" content="article" />
				<meta property="og:title" content="Python Workshop | Meetup" />
				<meta property="og:url" content="https://www.meetup.com/pyladies/events/123/" />
				<meta property="og:image" content="https://secure.meetupstatic.com/photos/event.jpeg" />
				<meta property="og:description" content="Learn Python basics" />
			</head></html>`,
			want: Meta{
				Title:       "Python Workshop | Meetup",
				Description: "Learn Python basics",
				Image:       "https://secure.meetupstatic.com/photos/event.jpeg",
			},
		},
		{
			name: "name attribute for description",
			html: `<html><head>
				<meta property="og:title" content="Event Title" />
				<meta name="og:description" content="Description via name attr" />
				<meta property="og:image" content="https://example.com/img.jpg" />
			</head></html>`,
			want: Meta{
				Title:       "Event Title",
				Description: "Description via name attr",
				Image:       "https://example.com/img.jpg",
			},
		},
		{
			name: "first og:image wins",
			html: `<html><head>
				<meta property="og:image" content="https://example.com/first.jpg" />
				<meta property="og:image" content="https://example.com/second.jpg" />
			</head></html>`,
			want: Meta{Image: "https://example.com/first.jpg"},
		},
		{
			name: "no og tags",
			html: `<html><head><title>Plain page</title></head></html>`,
			want: Meta{},
		},
		{
			name: "empty content ignored",
			html: `<html><head>
				<meta property="og:title" content="" />
				<meta property="og:title" content="Real Title" />
			</head></html>`,
			want: Meta{Title: "Real Title"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExtractMeta(tt.html)
			if got != tt.want {
				t.Errorf("ExtractMeta() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestFetchLive(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live HTTP tests in short mode")
	}

	tests := []struct {
		name string
		url  string
	}{
		{"luma", "https://luma.com/7af7l7pf"},
		{"meetup", "https://www.meetup.com/pyladies-brussels/events/313805745/"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := Fetch(tt.url)
			if m.Title == "" {
				t.Errorf("expected non-empty og:title from %s", tt.url)
			}
			if m.Image == "" {
				t.Errorf("expected non-empty og:image from %s", tt.url)
			}
			t.Logf("%s: title=%q image=%q", tt.name, m.Title, m.Image)
		})
	}
}

func TestFetchWithServer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(`<html><head>
			<meta property="og:title" content="Test Event" />
			<meta property="og:description" content="A test event" />
			<meta property="og:image" content="https://example.com/test.jpg" />
		</head></html>`))
	}))
	defer srv.Close()

	m := Fetch(srv.URL)
	if m.Title != "Test Event" {
		t.Errorf("Title = %q, want %q", m.Title, "Test Event")
	}
	if m.Description != "A test event" {
		t.Errorf("Description = %q, want %q", m.Description, "A test event")
	}
	if m.Image != "https://example.com/test.jpg" {
		t.Errorf("Image = %q, want %q", m.Image, "https://example.com/test.jpg")
	}
}

func TestFetchNon200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
	}))
	defer srv.Close()

	m := Fetch(srv.URL)
	if m != (Meta{}) {
		t.Errorf("expected empty Meta for 404, got %+v", m)
	}
}
