package cmd

import (
	"fmt"
	"time"

	"github.com/CommonsHub/chb/providers/mobilizon"
)

// MobilizonEventsFile is the on-disk shape of providers/mobilizon/events.json.
type MobilizonEventsFile struct {
	PulledAt string          `json:"pulledAt"`
	Instance string          `json:"instance"`
	Group    mobilizon.Group `json:"group"`
}

// MobilizonPull mirrors the group's events into providers/mobilizon/events.json.
// It logs in when credentials are set, so drafts the account can see are
// included; otherwise it reads the public listing.
func MobilizonPull(args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		printMobilizonHelp()
		return nil
	}
	cfg := loadMobilizonConfig()
	sl := NewStatusLine("Mobilizon")
	client := mobilizon.NewClient(cfg.URL)
	if cfg.Email != "" && cfg.Password != "" {
		sl.SetSubtask("logging in")
		if err := client.Login(cfg.Email, cfg.Password); err != nil {
			sl.Final(StepMark(err, nil), "error: "+err.Error())
			return err
		}
	}
	sl.SetSubtask("fetching @" + cfg.Group)
	group, err := pullMobilizonGroup(client, cfg.Group)
	if err != nil {
		sl.Final(StepMark(err, nil), "error: "+err.Error())
		return err
	}
	sl.Final(StepMark(nil, nil), fmt.Sprintf("@%s: %s", cfg.Group, Pluralize(len(group.Events), "event", "")))
	fmt.Printf("  %s↪ Next: chb mobilizon generate%s\n", Fmt.Dim, Fmt.Reset)
	return nil
}

func pullMobilizonGroup(client *mobilizon.Client, username string) (*mobilizon.Group, error) {
	group, err := client.GroupEvents(username)
	if err != nil {
		return nil, err
	}
	file := MobilizonEventsFile{
		PulledAt: time.Now().UTC().Format(time.RFC3339),
		Instance: client.BaseURL,
		Group:    *group,
	}
	return group, mobilizon.WriteJSON(mobilizon.EventsPath(DataDir()), file)
}
