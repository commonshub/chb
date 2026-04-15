package og

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"golang.org/x/net/html"
)

var ogHTTPClient = &http.Client{Timeout: 12 * time.Second}

// Meta holds Open Graph metadata extracted from a page.
type Meta struct {
	Title       string
	Description string
	Image       string
}

// MetaTag captures a single HTML meta tag.
type MetaTag struct {
	Name     string
	Property string
	Content  string
}

// FetchResult holds metadata plus debugging information about the fetch.
type FetchResult struct {
	URL                 string
	FinalURL            string
	StatusCode          int
	ContentType         string
	HTMLTitle           string
	Meta                Meta
	MetaTags            []MetaTag
	CloudflareChallenge bool
	ErrorKind           string
	ErrorMessage        string
}

// Fetch fetches OG metadata (title, description, image) from a URL.
func Fetch(pageURL string) Meta {
	return FetchDetailed(pageURL).Meta
}

// FetchDetailed fetches metadata and returns structured diagnostics.
func FetchDetailed(pageURL string) FetchResult {
	result := FetchResult{URL: strings.TrimSpace(pageURL)}
	if result.URL == "" {
		result.ErrorKind = "invalid_url"
		result.ErrorMessage = "empty URL"
		return result
	}

	parsedURL, err := url.Parse(result.URL)
	if err != nil || parsedURL.Scheme == "" || parsedURL.Host == "" {
		result.ErrorKind = "invalid_url"
		result.ErrorMessage = fmt.Sprintf("invalid URL: %s", result.URL)
		return result
	}

	req, err := http.NewRequest("GET", result.URL, nil)
	if err != nil {
		result.ErrorKind = "invalid_url"
		result.ErrorMessage = err.Error()
		return result
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; chb/1.0)")

	resp, err := ogHTTPClient.Do(req)
	if err != nil {
		result.ErrorKind = "request_failed"
		result.ErrorMessage = err.Error()
		return result
	}
	defer resp.Body.Close()

	result.StatusCode = resp.StatusCode
	result.ContentType = resp.Header.Get("Content-Type")
	if resp.Request != nil && resp.Request.URL != nil {
		result.FinalURL = resp.Request.URL.String()
	}
	if result.FinalURL == "" {
		result.FinalURL = result.URL
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		result.ErrorKind = "read_failed"
		result.ErrorMessage = err.Error()
		return result
	}

	bodyStr := string(body)
	result.CloudflareChallenge = looksLikeCloudflareChallenge(resp, bodyStr)

	if resp.StatusCode != http.StatusOK {
		result.ErrorKind = "http_error"
		if result.CloudflareChallenge {
			result.ErrorMessage = fmt.Sprintf("HTTP %d (Cloudflare challenge detected)", resp.StatusCode)
		} else {
			result.ErrorMessage = fmt.Sprintf("HTTP %d", resp.StatusCode)
		}
		return result
	}

	if !looksLikeHTML(result.ContentType, bodyStr) {
		result.ErrorKind = "non_html_response"
		if result.ContentType != "" {
			result.ErrorMessage = fmt.Sprintf("response is not HTML (Content-Type: %s)", result.ContentType)
		} else {
			result.ErrorMessage = "response is not HTML"
		}
		return result
	}

	result.Meta, result.MetaTags, result.HTMLTitle = ExtractMetaDetails(bodyStr)
	return result
}

// FetchOGImage fetches the og:image from a URL (kept for backward compatibility).
func FetchOGImage(pageURL string) string {
	return Fetch(pageURL).Image
}

// ExtractMeta parses HTML and returns OG metadata.
func ExtractMeta(htmlContent string) Meta {
	meta, _, _ := ExtractMetaDetails(htmlContent)
	return meta
}

// ExtractMetaDetails parses HTML and returns OG metadata, matching meta tags, and the HTML <title>.
func ExtractMetaDetails(htmlContent string) (Meta, []MetaTag, string) {
	var (
		meta      Meta
		metaTags  []MetaTag
		htmlTitle string
	)

	tokenizer := html.NewTokenizer(strings.NewReader(htmlContent))
	insideTitle := false

	for {
		tt := tokenizer.Next()
		switch tt {
		case html.ErrorToken:
			return meta, metaTags, strings.TrimSpace(htmlTitle)
		case html.StartTagToken:
			t := tokenizer.Token()
			if t.Data == "title" {
				insideTitle = true
				continue
			}
			if t.Data != "meta" {
				continue
			}
			if tag, ok := parseMetaTag(t); ok {
				metaTags = append(metaTags, tag)
				applyMetaTag(&meta, tag)
			}
		case html.SelfClosingTagToken:
			t := tokenizer.Token()
			if t.Data != "meta" {
				continue
			}
			if tag, ok := parseMetaTag(t); ok {
				metaTags = append(metaTags, tag)
				applyMetaTag(&meta, tag)
			}
		case html.TextToken:
			if insideTitle && htmlTitle == "" {
				htmlTitle = strings.TrimSpace(tokenizer.Token().Data)
			}
		case html.EndTagToken:
			t := tokenizer.Token()
			if t.Data == "title" {
				insideTitle = false
			}
		}
	}
}

func parseMetaTag(t html.Token) (MetaTag, bool) {
	var tag MetaTag
	for _, a := range t.Attr {
		switch strings.ToLower(a.Key) {
		case "property":
			tag.Property = strings.TrimSpace(a.Val)
		case "name":
			tag.Name = strings.TrimSpace(a.Val)
		case "content":
			tag.Content = strings.TrimSpace(a.Val)
		}
	}
	if tag.Name == "" && tag.Property == "" {
		return MetaTag{}, false
	}
	return tag, true
}

func applyMetaTag(meta *Meta, tag MetaTag) {
	if meta == nil || tag.Content == "" {
		return
	}
	switch {
	case strings.EqualFold(tag.Property, "og:image"):
		if meta.Image == "" {
			meta.Image = tag.Content
		}
	case strings.EqualFold(tag.Property, "og:title"):
		if meta.Title == "" {
			meta.Title = tag.Content
		}
	case strings.EqualFold(tag.Property, "og:description"), strings.EqualFold(tag.Name, "og:description"):
		if meta.Description == "" {
			meta.Description = tag.Content
		}
	}
}

func looksLikeHTML(contentType, body string) bool {
	contentType = strings.ToLower(strings.TrimSpace(contentType))
	if strings.Contains(contentType, "text/html") || strings.Contains(contentType, "application/xhtml+xml") {
		return true
	}
	trimmed := strings.TrimSpace(strings.ToLower(body))
	return strings.HasPrefix(trimmed, "<!doctype html") ||
		strings.HasPrefix(trimmed, "<html") ||
		strings.Contains(trimmed, "<head")
}

func looksLikeCloudflareChallenge(resp *http.Response, body string) bool {
	if resp == nil {
		return false
	}
	server := strings.ToLower(resp.Header.Get("Server"))
	body = strings.ToLower(body)
	if strings.Contains(server, "cloudflare") {
		if resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusServiceUnavailable || strings.Contains(body, "just a moment") {
			return true
		}
	}
	return strings.Contains(body, "cf-browser-verification") ||
		strings.Contains(body, "challenge-platform") ||
		strings.Contains(body, "attention required! | cloudflare") ||
		strings.Contains(body, "just a moment...")
}
