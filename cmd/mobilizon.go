package cmd

import (
	"fmt"
	"os"
)

// Mobilizon is a target: `pull` mirrors the group's events, `generate` plans
// what our public Luma events need on Mobilizon, `push` applies that plan.
// See docs/mobilizon.md.

const (
	defaultMobilizonURL   = "https://mobilizon.be"
	defaultMobilizonGroup = "commonshub_bxl"
)

type mobilizonConfig struct {
	URL      string
	Group    string
	Email    string
	Password string
}

func loadMobilizonConfig() mobilizonConfig {
	cfg := mobilizonConfig{
		URL:      os.Getenv("MOBILIZON_URL"),
		Group:    os.Getenv("MOBILIZON_GROUP"),
		Email:    os.Getenv("MOBILIZON_EMAIL"),
		Password: os.Getenv("MOBILIZON_PASSWORD"),
	}
	if cfg.URL == "" {
		cfg.URL = defaultMobilizonURL
	}
	if cfg.Group == "" {
		cfg.Group = defaultMobilizonGroup
	}
	return cfg
}

func (c mobilizonConfig) requireCredentials() error {
	if c.Email == "" || c.Password == "" {
		return fmt.Errorf("MOBILIZON_EMAIL and MOBILIZON_PASSWORD are not set.\n\n" +
			"Add them to settings/config.env (or the environment) for an account that is an\n" +
			"admin of the group, then re-run.")
	}
	return nil
}

// Mobilizon dispatches `chb mobilizon <verb>`.
func Mobilizon(args []string) error {
	if len(args) == 0 || HasFlag(args[:1], "--help", "-h", "help") {
		printMobilizonHelp()
		return nil
	}
	switch args[0] {
	case "pull":
		return MobilizonPull(args[1:])
	case "generate":
		return MobilizonGenerate(args[1:])
	case "pending":
		return MobilizonPush(append([]string{"--dry-run"}, args[1:]...))
	case "push":
		return MobilizonPush(args[1:])
	default:
		return fmt.Errorf("unknown subcommand %q\n\nExample:\n  chb mobilizon pull\n  chb mobilizon push --dry-run\n\n(run with --help for all options)", args[0])
	}
}

func printMobilizonHelp() {
	f := Fmt
	fmt.Printf(`
%schb mobilizon%s — Publish our public Luma events to a Mobilizon group

%sUSAGE%s
  %schb mobilizon pull%s              Mirror the group's events
  %schb mobilizon generate%s          Plan creates, updates and cancellations (offline)
  %schb mobilizon pending%s           Show the plan (same as push --dry-run)
  %schb mobilizon push%s [options]    Apply the plan

%sPUSH OPTIONS%s
  %s--dry-run%s           Show what would change, write nothing
  %s--draft%s             Create new events as drafts, to review them in Mobilizon first
  %s--yes, -y%s           Don't ask for confirmation
  %s--help, -h%s          Show this help

%sCONFIGURATION%s (settings/config.env or environment)
  MOBILIZON_EMAIL, MOBILIZON_PASSWORD   an admin of the group (push only)
  MOBILIZON_URL                          default %s
  MOBILIZON_GROUP                        default %s

%sEXAMPLES%s
  %schb calendars sync && chb mobilizon pull && chb mobilizon generate%s
  %schb mobilizon push --draft%s        first run: review the drafts in Mobilizon
  %schb mobilizon push --yes%s          unattended, e.g. from a timer
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset, f.Cyan, f.Reset, f.Cyan, f.Reset, f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset, f.Yellow, f.Reset, f.Yellow, f.Reset, f.Yellow, f.Reset,
		f.Bold, f.Reset,
		defaultMobilizonURL, defaultMobilizonGroup,
		f.Bold, f.Reset,
		f.Cyan, f.Reset, f.Cyan, f.Reset, f.Cyan, f.Reset,
	)
}
