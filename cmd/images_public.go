package cmd

// Photos from public channels, served from public/.
//
// images.json entries carry a signed Discord URL that expires within a day,
// and a filePath into providers/discord/images/ (0700, never served). For
// the channels listed in settings.json `discord.publicChannels`, `chb
// images sync` copies each downloaded photo to
// YYYY/MM/public/images/<attachment id>.<ext> (0755 dirs, 0644 files),
// and the public and members images.json point filePath there. Photos from
// any other channel never appear in public/images.json and are removed from
// public/images/ if a channel stops being public.

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// defaultPublicPhotoChannels applies when settings.json has no
// `discord.publicChannels`: the channels every member of the Discord server
// can read, minus #introductions (portraits).
var defaultPublicPhotoChannels = []string{"general", "activities.contributions", "activities.tokens"}

// publicPhotoChannelIDs resolves `discord.publicChannels` (names from
// `discord.channels`, dotted for nested groups, a leaf name, or raw ids)
// to channel ids. Reads settings.json directly (no reconciliation).
func publicPhotoChannelIDs() map[string]bool {
	var s struct {
		Discord struct {
			Channels       json.RawMessage `json:"channels"`
			PublicChannels *[]string       `json:"publicChannels"`
		} `json:"discord"`
	}
	if data, err := os.ReadFile(settingsFilePath("settings.json")); err == nil {
		_ = json.Unmarshal(data, &s)
	}
	wanted := defaultPublicPhotoChannels
	if s.Discord.PublicChannels != nil {
		wanted = *s.Discord.PublicChannels
	}
	names := map[string]string{}
	flattenDiscordChannels(s.Discord.Channels, "", names)
	out := map[string]bool{}
	for _, w := range wanted {
		w = strings.TrimPrefix(strings.TrimSpace(w), "#")
		if id, ok := names[w]; ok {
			out[id] = true
		} else if w != "" && strings.Trim(w, "0123456789") == "" {
			out[w] = true
		}
	}
	return out
}

// flattenDiscordChannels maps "general", "activities.potluck" and the leaf
// "potluck" to their channel ids.
func flattenDiscordChannels(raw json.RawMessage, prefix string, out map[string]string) {
	var m map[string]json.RawMessage
	if json.Unmarshal(raw, &m) != nil {
		return
	}
	for k, v := range m {
		var id string
		if json.Unmarshal(v, &id) == nil {
			out[prefix+k] = id
			if _, taken := out[k]; !taken {
				out[k] = id
			}
			continue
		}
		flattenDiscordChannels(v, prefix+k+".", out)
	}
}

// imageMonthDir is the YYYY/MM an image belongs to: the one of its
// providers filePath (Brussels date of the message).
func imageMonthDir(img ImageEntry) (string, string, bool) {
	parts := strings.Split(filepath.ToSlash(img.FilePath), "/")
	if len(parts) >= 2 && isYearSegment(parts[0]) && isMonthSegment(parts[1]) {
		return parts[0], parts[1], true
	}
	return "", "", false
}

func publicImagesDir(dataDir, year, month string) string {
	return filepath.Join(dataDir, year, month, AudiencePublic.Dir(), "images")
}

// findByIDPrefix returns the file in dir named <id> or <id>.<ext>.
func findByIDPrefix(dir, id string) string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return ""
	}
	for _, e := range entries {
		if !e.IsDir() && (e.Name() == id || strings.HasPrefix(e.Name(), id+".")) {
			return e.Name()
		}
	}
	return ""
}

// publicImageRelPath is the public copy of an image, relative to DATA_DIR,
// or "" when there is none (not a public channel, or not copied yet).
func publicImageRelPath(dataDir string, img ImageEntry, public map[string]bool) string {
	if !public[img.ChannelID] {
		return ""
	}
	year, month, ok := imageMonthDir(img)
	if !ok {
		return ""
	}
	name := findByIDPrefix(publicImagesDir(dataDir, year, month), img.ID)
	if name == "" {
		return ""
	}
	return filepath.ToSlash(filepath.Join(year, month, AudiencePublic.Dir(), "images", name))
}

// publishPublicImages copies the month's downloaded photos from public
// channels to public/images/ and removes copies of photos that are not
// (or no longer) from a public channel. Returns (copied, removed).
func publishPublicImages(dataDir string, images []ImageEntry, public map[string]bool, force bool) (int, int) {
	copied, removed := 0, 0
	keep := map[string]map[string]bool{} // public images dir → ids to keep
	for _, img := range images {
		year, month, ok := imageMonthDir(img)
		if !ok {
			continue
		}
		dir := publicImagesDir(dataDir, year, month)
		if keep[dir] == nil {
			keep[dir] = map[string]bool{}
		}
		if !public[img.ChannelID] {
			continue
		}
		keep[dir][img.ID] = true
		srcDir := filepath.Dir(filepath.Join(dataDir, filepath.FromSlash(img.FilePath)))
		name := findByIDPrefix(srcDir, img.ID)
		if name == "" {
			continue // not downloaded (yet)
		}
		dst := filepath.Join(dir, name)
		if !force && fileExists(dst) {
			continue
		}
		if err := copyPublicAsset(filepath.Join(srcDir, name), dst); err != nil {
			Warnf("  %s⚠ public photo %s: %v%s", Fmt.Yellow, img.ID, err, Fmt.Reset)
			continue
		}
		copied++
	}
	for dir, ids := range keep {
		entries, _ := os.ReadDir(dir)
		for _, e := range entries {
			id := strings.SplitN(e.Name(), ".", 2)[0]
			if !ids[id] {
				if os.Remove(filepath.Join(dir, e.Name())) == nil {
					removed++
				}
			}
		}
	}
	return copied, removed
}

// copyPublicAsset copies a binary file into the public tier (0755/0644).
func copyPublicAsset(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), AudiencePublic.DirMode()); err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	tmp := dst + ".tmp"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, AudiencePublic.FileMode())
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		os.Remove(tmp)
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, dst); err != nil {
		return err
	}
	if base, ok := dataBaseForPath(dst); ok {
		_ = applyDataPathPolicy(base, dst, false)
	}
	return nil
}
