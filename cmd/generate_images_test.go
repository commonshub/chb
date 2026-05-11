package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGenerateMonthImagesGoUsesOriginalURLAndRelativeFilePath(t *testing.T) {
	tests := []struct {
		name         string
		year         string
		month        string
		wantFilePath string
	}{
		{
			name:         "monthly",
			year:         "2026",
			month:        "04",
			wantFilePath: "2026/04/sources/discord/images/att-1.png",
		},
		{
			name:         "latest",
			year:         "latest",
			month:        "",
			wantFilePath: "2026/04/sources/discord/images/att-1.png",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dataDir := t.TempDir()
			discordDir := filepath.Join(dataDir, tt.year, tt.month, "sources", "discord", "chan-1")
			if err := os.MkdirAll(discordDir, 0755); err != nil {
				t.Fatalf("mkdir: %v", err)
			}

			msgData := []byte(`{
			  "messages": [
			    {
			      "id": "msg-1",
			      "channel_id": "chan-1",
			      "author": {
			        "id": "user-1",
			        "username": "alice",
			        "global_name": "Alice <Admin>",
			        "avatar": "avatar-1"
			      },
			      "content": "<tag>& photo",
			      "timestamp": "2026-04-13T12:00:00.000000+00:00",
			      "attachments": [
			        {
			          "id": "att-1",
			          "url": "https://cdn.discordapp.com/attachments/file.png?ex=1&is=2",
			          "content_type": "image/png"
			        }
			      ],
			      "reactions": [
			        {
			          "emoji": {"name": "🔥"},
			          "count": 2
			        }
			      ]
			    }
			  ]
			}`)
			if err := os.WriteFile(filepath.Join(discordDir, "messages.json"), msgData, 0644); err != nil {
				t.Fatalf("write messages: %v", err)
			}

			if n := generateMonthImagesGo(dataDir, tt.year, tt.month); n != 1 {
				t.Fatalf("generateMonthImagesGo() = %d, want 1", n)
			}

			outPath := filepath.Join(dataDir, tt.year, tt.month, "generated", "images.json")
			outData, err := os.ReadFile(outPath)
			if err != nil {
				t.Fatalf("read images.json: %v", err)
			}
			out := string(outData)

			if strings.Contains(out, `"proxyUrl"`) {
				t.Fatalf("images.json unexpectedly contains proxyUrl: %s", out)
			}
			if strings.Contains(out, `\u`) {
				t.Fatalf("images.json unexpectedly contains unicode escapes: %s", out)
			}
			if !strings.Contains(out, `"url": "https://cdn.discordapp.com/attachments/file.png?ex=1&is=2"`) {
				t.Fatalf("images.json missing original url: %s", out)
			}
			if !strings.Contains(out, `"filePath": "`+tt.wantFilePath+`"`) {
				t.Fatalf("images.json missing relative file path %q: %s", tt.wantFilePath, out)
			}
			if !strings.Contains(out, `"message": "<tag>& photo"`) {
				t.Fatalf("images.json escaped message content unexpectedly: %s", out)
			}
		})
	}
}

func TestGenerateMonthImagesGoIncludesAllSourceDiscordChannels(t *testing.T) {
	dataDir := t.TempDir()

	writeDiscordSourceMessagesFixture(t, dataDir, "2026", "04", "chan-1", `{
	  "messages": [{
	    "id": "msg-1",
	    "channel_id": "chan-1",
	    "author": {"id": "user-1", "username": "alice"},
	    "content": "first",
	    "timestamp": "2026-04-13T12:00:00.000000+00:00",
	    "attachments": [{"id": "att-1", "url": "https://cdn.discordapp.com/a.png", "content_type": "image/png"}]
	  }]
	}`)
	writeDiscordSourceMessagesFixture(t, dataDir, "2026", "04", "chan-2", `{
	  "messages": [{
	    "id": "msg-2",
	    "channel_id": "chan-2",
	    "author": {"id": "user-2", "username": "bob"},
	    "content": "second",
	    "timestamp": "2026-04-14T12:00:00.000000+00:00",
	    "attachments": [
	      {"id": "att-2", "url": "https://cdn.discordapp.com/b.webp"},
	      {"id": "att-doc", "url": "https://cdn.discordapp.com/readme.txt", "content_type": "text/plain"}
	    ]
	  }]
	}`)

	if n := generateMonthImagesGo(dataDir, "2026", "04"); n != 2 {
		t.Fatalf("generateMonthImagesGo() = %d, want 2", n)
	}

	outData, err := os.ReadFile(filepath.Join(dataDir, "2026", "04", "generated", "images.json"))
	if err != nil {
		t.Fatalf("read images.json: %v", err)
	}
	var out ImagesFile
	if err := json.Unmarshal(outData, &out); err != nil {
		t.Fatalf("unmarshal images.json: %v", err)
	}
	if out.Count != 2 || len(out.Images) != 2 {
		t.Fatalf("expected 2 images, got count=%d len=%d", out.Count, len(out.Images))
	}
	byID := map[string]ImageEntry{}
	for _, img := range out.Images {
		byID[img.ID] = img
	}
	if byID["att-1"].ChannelID != "chan-1" {
		t.Fatalf("att-1 channel = %q, want chan-1", byID["att-1"].ChannelID)
	}
	if byID["att-2"].ChannelID != "chan-2" {
		t.Fatalf("att-2 channel = %q, want chan-2", byID["att-2"].ChannelID)
	}
	if _, ok := byID["att-doc"]; ok {
		t.Fatalf("non-image attachment was included: %#v", byID["att-doc"])
	}
}

func writeDiscordSourceMessagesFixture(t *testing.T, dataDir, year, month, channelID, payload string) {
	t.Helper()
	discordDir := filepath.Join(dataDir, year, month, "sources", "discord", channelID)
	if err := os.MkdirAll(discordDir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(discordDir, "messages.json"), []byte(payload), 0644); err != nil {
		t.Fatalf("write messages: %v", err)
	}
}
