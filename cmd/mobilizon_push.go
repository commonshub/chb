package cmd

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"github.com/CommonsHub/chb/providers/mobilizon"
)

// MobilizonPush applies providers/mobilizon/pending/events.json. It is the
// only command that writes to Mobilizon.
func MobilizonPush(args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		printMobilizonHelp()
		return nil
	}
	dryRun := HasFlag(args, "--dry-run")
	draft := HasFlag(args, "--draft")
	assumeYes := HasFlag(args, "--yes", "-y")
	dataDir := DataDir()

	var pending MobilizonPendingFile
	if err := mobilizon.ReadJSON(mobilizon.PendingPath(dataDir), &pending); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("nothing planned yet. Run `chb mobilizon pull` and `chb mobilizon generate` first")
		}
		return err
	}
	published, err := loadMobilizonPublished(dataDir)
	if err != nil {
		return err
	}
	actions := pending.Actions
	if !draft {
		actions = append(actions, mobilizonDraftsToPublish(published, actions)...)
	}

	printMobilizonPlan(&pending, actions, draft)
	if len(actions) == 0 {
		return nil
	}
	if dryRun {
		fmt.Printf("\n  %s↪ Re-run without --dry-run to apply.%s\n\n", Fmt.Dim, Fmt.Reset)
		return nil
	}
	if !assumeYes {
		if !isInteractiveTTY() {
			fmt.Printf("\n  %sRefusing to write without --yes on a non-interactive shell.%s\n\n", Fmt.Yellow, Fmt.Reset)
			return nil
		}
		fmt.Printf("\n  %sApply %s on %s @%s?%s [y/N] ", Fmt.Bold, Pluralize(len(actions), "change", ""), pending.Instance, pending.Group, Fmt.Reset)
		resp, _ := bufio.NewReader(os.Stdin).ReadString('\n')
		if r := strings.TrimSpace(strings.ToLower(resp)); r != "y" && r != "yes" {
			fmt.Println("  Aborted.")
			return nil
		}
	}

	cfg := loadMobilizonConfig()
	if err := cfg.requireCredentials(); err != nil {
		return err
	}
	if pending.Instance != "" && strings.TrimRight(cfg.URL, "/") != pending.Instance {
		return fmt.Errorf("the plan is for %s but MOBILIZON_URL is %s. Pull and generate again", pending.Instance, cfg.URL)
	}
	client := mobilizon.NewClient(cfg.URL)
	if err := client.Login(cfg.Email, cfg.Password); err != nil {
		return err
	}
	actorID, err := client.LoggedPersonID()
	if err != nil {
		return err
	}

	fmt.Println()
	failed := 0
	for _, a := range actions {
		sl := NewStatusLine(fmt.Sprintf("%s %s", a.Action, a.Title))
		pub, err := applyMobilizonAction(client, a, published.Events[a.Key], actorID, pending.GroupID, draft, sl)
		if err != nil {
			failed++
			sl.Final(StepMark(err, nil), "error: "+err.Error())
			continue
		}
		published.Events[a.Key] = pub
		if err := mobilizon.WriteJSON(mobilizon.PublishedPath(dataDir), published); err != nil {
			sl.Final(StepMark(err, nil), "error: "+err.Error())
			return err
		}
		sl.Final(StepMark(nil, nil), strings.TrimRight(cfg.URL, "/")+"/events/"+pub.UUID)
	}

	// Refresh the mirror and the plan, so the next push starts from what
	// Mobilizon now holds. Failed actions stay in the plan.
	if _, err := pullMobilizonGroup(client, cfg.Group); err != nil {
		Warnf("could not refresh the Mobilizon mirror: %v", err)
	} else if next, err := generateMobilizonPending(dataDir, time.Now()); err != nil {
		Warnf("could not refresh the Mobilizon plan: %v", err)
	} else {
		fmt.Printf("\n  Mobilizon: %s\n", summarizeMobilizonPending(next))
	}
	if failed > 0 {
		return fmt.Errorf("%s failed", Pluralize(failed, "change", ""))
	}
	return nil
}

// mobilizonDraftsToPublish adds a publish step for events we created as
// drafts and have no other change for.
func mobilizonDraftsToPublish(published *MobilizonPublishedFile, planned []MobilizonAction) []MobilizonAction {
	inPlan := map[string]bool{}
	for _, a := range planned {
		inPlan[a.Key] = true
	}
	var out []MobilizonAction
	for key, pub := range published.Events {
		if !pub.Draft || pub.Cancelled || inPlan[key] {
			continue
		}
		out = append(out, MobilizonAction{
			Action: "publish", Key: key, EventID: pub.EventID, MobilizonID: pub.MobilizonID, UUID: pub.UUID,
			Title: pub.Title, BeginsOn: pub.BeginsOn, Reason: "draft",
		})
	}
	return out
}

func printMobilizonPlan(p *MobilizonPendingFile, actions []MobilizonAction, draft bool) {
	f := Fmt
	fmt.Printf("\n  %sMobilizon @%s%s  %s\n", f.Bold, p.Group, f.Reset, p.Instance)
	if len(actions) == 0 {
		fmt.Printf("  Nothing to push.\n")
	}
	if len(actions) > 0 {
		fmt.Printf("\n  %s%-8s %-17s %-50s %s%s\n", f.Dim, "ACTION", "WHEN", "TITLE", "NOTE", f.Reset)
		for _, a := range actions {
			action := a.Action
			if action == "create" && draft {
				action = "draft"
			}
			fmt.Printf("  %-8s %-17s %-50s %s%s%s\n", action, formatMobilizonWhen(a.BeginsOn), clip(a.Title, 50), f.Dim, a.Reason, f.Reset)
		}
	}
	if len(p.Skipped) > 0 {
		fmt.Printf("\n  %sSkipped%s\n", f.Dim, f.Reset)
		for _, s := range p.Skipped {
			fmt.Printf("  %s%-17s %-50s %s%s\n", f.Dim, formatMobilizonWhen(s.BeginsOn), clip(s.Title, 50), s.Reason, f.Reset)
		}
	}
}

func formatMobilizonWhen(value string) string {
	t, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return value
	}
	return t.In(BrusselsTZ()).Format("Mon 02 Jan 15:04")
}

func clip(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n-1]) + "…"
}

func applyMobilizonAction(client *mobilizon.Client, a MobilizonAction, pub MobilizonPublished, actorID, groupID string, draft bool, sl *StatusLine) (MobilizonPublished, error) {
	switch a.Action {
	case "cancel":
		_, err := client.UpdateEvent(a.MobilizonID, mobilizon.EventInput{Status: "CANCELLED"})
		pub.Cancelled = true
		return pub, err
	case "publish":
		published := false
		_, err := client.UpdateEvent(a.MobilizonID, mobilizon.EventInput{Draft: &published})
		pub.Draft = false
		return pub, err
	}

	in := mobilizon.EventInput{
		Title:                    a.Title,
		Description:              a.Description,
		BeginsOn:                 a.BeginsOn,
		EndsOn:                   a.EndsOn,
		Address:                  a.Address,
		ExternalParticipationURL: a.Key,
		Tags:                     a.Tags,
		Status:                   "CONFIRMED",
		OrganizerActorID:         actorID,
		AttributedToID:           groupID,
	}
	if a.Action == "create" || !draft {
		// Creating: draft or not as asked. Updating: publish unless --draft,
		// which leaves the draft state as it is.
		d := draft
		in.Draft = &d
	}

	pictureUUID := pub.PictureUUID
	if a.CoverImage != "" && (a.CoverImage != pub.CoverImage || pictureUUID == "") {
		sl.SetSubtask("uploading cover")
		if id, err := uploadMobilizonCover(client, actorID, a.CoverImage, a.Title); err != nil {
			Warnf("%s: cover not uploaded: %v", a.Title, err)
		} else {
			pictureUUID = id
		}
	}
	in.PictureMediaUUID = pictureUUID

	var ev *mobilizon.Event
	var err error
	if a.Action == "create" {
		sl.SetSubtask("creating")
		ev, err = client.CreateEvent(in)
	} else {
		sl.SetSubtask("updating")
		ev, err = client.UpdateEvent(a.MobilizonID, in)
	}
	if err != nil {
		return pub, err
	}
	return MobilizonPublished{
		EventID:     a.EventID,
		MobilizonID: ev.ID,
		UUID:        ev.UUID,
		Title:       a.Title,
		BeginsOn:    a.BeginsOn,
		Hash:        a.Hash,
		CoverImage:  a.CoverImage,
		PictureUUID: pictureUUID,
		Draft:       ev.Draft,
	}, nil
}

func uploadMobilizonCover(client *mobilizon.Client, actorID, imageURL, title string) (string, error) {
	resp, err := http.Get(imageURL)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("HTTP %d fetching %s", resp.StatusCode, imageURL)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
	if err != nil {
		return "", err
	}
	ext := ".jpg"
	switch ct := resp.Header.Get("Content-Type"); {
	case strings.Contains(ct, "png"):
		ext = ".png"
	case strings.Contains(ct, "webp"):
		ext = ".webp"
	case strings.Contains(ct, "avif"):
		ext = ".avif"
	}
	name := strings.TrimSuffix(path.Base(strings.SplitN(imageURL, "?", 2)[0]), path.Ext(imageURL)) + ext
	return client.UploadMedia(actorID, name, title, data)
}
