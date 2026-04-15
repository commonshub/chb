package cmd

import (
	"fmt"
	"strings"

	"github.com/CommonsHub/chb/og"
)

func Tools(args []string) error {
	if len(args) == 0 || HasFlag(args, "--help", "-h", "help") {
		PrintToolsHelp()
		return nil
	}

	switch {
	case strings.EqualFold(args[0], "getUrlMetadata"), strings.EqualFold(args[0], "get-url-metadata"):
		return GetURLMetadata(args[1:])
	default:
		return fmt.Errorf("unknown tools command: %s", args[0])
	}
}

func GetURLMetadata(args []string) error {
	if len(args) == 0 || HasFlag(args, "--help", "-h", "help") {
		PrintGetURLMetadataHelp()
		return nil
	}

	verbose := HasFlag(args, "--verbose", "-v")

	var targetURL string
	for _, arg := range args {
		if strings.HasPrefix(arg, "-") {
			continue
		}
		targetURL = arg
		break
	}
	if strings.TrimSpace(targetURL) == "" {
		return fmt.Errorf("usage: chb tools getUrlMetadata <url> [--verbose]")
	}

	result := og.FetchDetailed(targetURL)

	fmt.Printf("URL: %s\n", result.URL)
	if result.FinalURL != "" && result.FinalURL != result.URL {
		fmt.Printf("Final URL: %s\n", result.FinalURL)
	}
	if result.StatusCode != 0 {
		fmt.Printf("HTTP Status: %d\n", result.StatusCode)
	}
	if result.ContentType != "" {
		fmt.Printf("Content-Type: %s\n", result.ContentType)
	}
	if result.CloudflareChallenge {
		fmt.Printf("Cloudflare Challenge: yes\n")
	}
	if result.HTMLTitle != "" {
		fmt.Printf("HTML Title: %s\n", result.HTMLTitle)
	}

	if result.Meta.Title != "" {
		fmt.Printf("Title: %s\n", result.Meta.Title)
	} else {
		fmt.Printf("Title: not found\n")
	}
	if result.Meta.Description != "" {
		fmt.Printf("Description: %s\n", result.Meta.Description)
	} else {
		fmt.Printf("Description: not found\n")
	}
	if result.Meta.Image != "" {
		fmt.Printf("OG Image: %s\n", result.Meta.Image)
	} else {
		fmt.Printf("OG Image: not found\n")
	}

	if result.ErrorKind != "" {
		fmt.Printf("Error: %s\n", result.ErrorMessage)
	} else if result.CloudflareChallenge {
		fmt.Printf("Error: Cloudflare challenge page detected\n")
	} else if result.Meta.Image == "" {
		fmt.Printf("Error: no og:image meta tag found on the fetched HTML page\n")
	}

	if verbose {
		fmt.Printf("\nMeta Tags:\n")
		if len(result.MetaTags) == 0 {
			fmt.Printf("  (none)\n")
		} else {
			for _, tag := range result.MetaTags {
				var attrs []string
				if tag.Name != "" {
					attrs = append(attrs, fmt.Sprintf("name=%q", tag.Name))
				}
				if tag.Property != "" {
					attrs = append(attrs, fmt.Sprintf("property=%q", tag.Property))
				}
				if tag.Content != "" {
					attrs = append(attrs, fmt.Sprintf("content=%q", tag.Content))
				}
				fmt.Printf("  - %s\n", strings.Join(attrs, " "))
			}
		}
	}

	if result.ErrorKind != "" {
		return fmt.Errorf("%s", result.ErrorMessage)
	}
	return nil
}
