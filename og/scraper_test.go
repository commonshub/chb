package og

import (
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
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
	restore := withMockTransport(func(req *http.Request) (*http.Response, error) {
		return newTestResponse(req, http.StatusOK, "text/html", `<html><head>
			<meta property="og:title" content="Test Event" />
			<meta property="og:description" content="A test event" />
			<meta property="og:image" content="https://example.com/test.jpg" />
		</head></html>`), nil
	})
	defer restore()

	m := Fetch("https://example.com/event")
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
	restore := withMockTransport(func(req *http.Request) (*http.Response, error) {
		return newTestResponse(req, http.StatusNotFound, "text/html", ""), nil
	})
	defer restore()

	m := Fetch("https://example.com/missing")
	if m != (Meta{}) {
		t.Errorf("expected empty Meta for 404, got %+v", m)
	}
}

func TestFetchDetailedMissingOGImage(t *testing.T) {
	restore := withMockTransport(func(req *http.Request) (*http.Response, error) {
		return newTestResponse(req, http.StatusOK, "text/html; charset=utf-8", `<html><head>
			<title>OpenClaworking Day</title>
			<meta property="og:title" content="OpenClaworking Day" />
			<meta property="og:description" content="Coworking session at Commons Hub" />
		</head></html>`), nil
	})
	defer restore()

	result := FetchDetailed("https://example.com/openclaworking-day")
	if result.ErrorKind != "" {
		t.Fatalf("expected no fetch error, got %s: %s", result.ErrorKind, result.ErrorMessage)
	}
	if result.Meta.Image != "" {
		t.Fatalf("expected missing og:image, got %q", result.Meta.Image)
	}
	if result.HTMLTitle != "OpenClaworking Day" {
		t.Fatalf("expected HTML title to be captured, got %q", result.HTMLTitle)
	}
	if len(result.MetaTags) != 2 {
		t.Fatalf("expected 2 meta tags, got %d", len(result.MetaTags))
	}
}

func TestFetchDetailedCloudflareChallenge(t *testing.T) {
	restore := withMockTransport(func(req *http.Request) (*http.Response, error) {
		resp := newTestResponse(req, http.StatusServiceUnavailable, "text/html", `<html><head><title>Just a moment...</title></head><body>challenge-platform</body></html>`)
		resp.Header.Set("Server", "cloudflare")
		return resp, nil
	})
	defer restore()

	result := FetchDetailed("https://example.com/protected")
	if result.ErrorKind != "http_error" {
		t.Fatalf("expected http_error, got %q", result.ErrorKind)
	}
	if !result.CloudflareChallenge {
		t.Fatal("expected Cloudflare challenge to be detected")
	}
}

func TestFetchDetailedNonHTMLResponse(t *testing.T) {
	restore := withMockTransport(func(req *http.Request) (*http.Response, error) {
		return newTestResponse(req, http.StatusOK, "application/pdf", "%PDF-1.4"), nil
	})
	defer restore()

	result := FetchDetailed("https://example.com/file.pdf")
	if result.ErrorKind != "non_html_response" {
		t.Fatalf("expected non_html_response, got %q", result.ErrorKind)
	}
}

func TestFetchDetailedWithDebugWritesDomainLogForMissingImage(t *testing.T) {
	restore := withMockTransport(func(req *http.Request) (*http.Response, error) {
		return newTestResponse(req, http.StatusOK, "text/html; charset=utf-8", `<html><head>
			<title>OpenClaworking Day</title>
			<meta property="og:title" content="OpenClaworking Day" />
		</head></html>`), nil
	})
	defer restore()

	debugDir := t.TempDir()
	result := FetchDetailedWithOptions("https://luma.com/u3kbetd4", FetchOptions{
		Debug:    true,
		DebugDir: debugDir,
	})
	if result.DebugLogPath == "" {
		t.Fatal("expected debug log path to be set")
	}
	wantPath := filepath.Join(debugDir, "debug.luma.com.log")
	if result.DebugLogPath != wantPath {
		t.Fatalf("expected debug log path %q, got %q", wantPath, result.DebugLogPath)
	}
	data, err := os.ReadFile(wantPath)
	if err != nil {
		t.Fatalf("read debug log: %v", err)
	}
	content := string(data)
	if !strings.Contains(content, "GET https://luma.com/u3kbetd4") {
		t.Fatalf("expected request line in debug log, got %q", content)
	}
	if !strings.Contains(content, "-- Response Body --") {
		t.Fatalf("expected response body section in debug log, got %q", content)
	}
	if !strings.Contains(content, "OpenClaworking Day") {
		t.Fatalf("expected html body content in debug log, got %q", content)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

func withMockTransport(fn roundTripFunc) func() {
	original := ogHTTPClient.Transport
	ogHTTPClient.Transport = fn
	return func() {
		ogHTTPClient.Transport = original
	}
}

func newTestResponse(req *http.Request, status int, contentType, body string) *http.Response {
	resp := &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    req,
	}
	if contentType != "" {
		resp.Header.Set("Content-Type", contentType)
	}
	return resp
}
