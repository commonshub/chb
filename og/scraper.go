package og

import (
	"io"
	"net/http"
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

// Fetch fetches OG metadata (title, description, image) from a URL.
func Fetch(pageURL string) Meta {
	req, err := http.NewRequest("GET", pageURL, nil)
	if err != nil {
		return Meta{}
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; chb/1.0)")

	resp, err := ogHTTPClient.Do(req)
	if err != nil {
		return Meta{}
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return Meta{}
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return Meta{}
	}

	return ExtractMeta(string(body))
}

// FetchOGImage fetches the og:image from a URL (kept for backward compatibility).
func FetchOGImage(pageURL string) string {
	return Fetch(pageURL).Image
}

// ExtractMeta parses HTML and returns OG metadata.
func ExtractMeta(htmlContent string) Meta {
	var m Meta
	tokenizer := html.NewTokenizer(strings.NewReader(htmlContent))
	for {
		tt := tokenizer.Next()
		switch tt {
		case html.ErrorToken:
			return m
		case html.SelfClosingTagToken, html.StartTagToken:
			t := tokenizer.Token()
			if t.Data != "meta" {
				continue
			}
			var property, name, content string
			for _, a := range t.Attr {
				switch a.Key {
				case "property":
					property = a.Val
				case "name":
					name = a.Val
				case "content":
					content = a.Val
				}
			}
			if content == "" {
				continue
			}
			switch {
			case property == "og:image":
				if m.Image == "" {
					m.Image = content
				}
			case property == "og:title":
				if m.Title == "" {
					m.Title = content
				}
			case property == "og:description" || name == "og:description":
				if m.Description == "" {
					m.Description = content
				}
			}
		}
	}
}
