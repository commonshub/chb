package mobilizon

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type recorded struct {
	auth      string
	query     string
	variables map[string]interface{}
	file      string
}

func fakeServer(t *testing.T, reply func(r recorded) string) (*httptest.Server, *[]recorded) {
	t.Helper()
	var calls []recorded
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := recorded{auth: r.Header.Get("Authorization")}
		if strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/") {
			if err := r.ParseMultipartForm(1 << 20); err != nil {
				t.Fatal(err)
			}
			rec.query = r.FormValue("query")
			_ = json.Unmarshal([]byte(r.FormValue("variables")), &rec.variables)
			f, _, err := r.FormFile("file")
			if err == nil {
				b, _ := io.ReadAll(f)
				rec.file = string(b)
			}
		} else {
			var body struct {
				Query     string                 `json:"query"`
				Variables map[string]interface{} `json:"variables"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			rec.query, rec.variables = body.Query, body.Variables
		}
		calls = append(calls, rec)
		_, _ = io.WriteString(w, reply(rec))
	}))
	t.Cleanup(srv.Close)
	return srv, &calls
}

func TestLoginThenAuthorizedCreate(t *testing.T) {
	srv, calls := fakeServer(t, func(r recorded) string {
		switch {
		case strings.Contains(r.query, "login("):
			return `{"data":{"login":{"accessToken":"tok"}}}`
		case strings.Contains(r.query, "createEvent("):
			return `{"data":{"createEvent":{"id":"5","uuid":"u5","draft":true}}}`
		}
		return `{"errors":[{"message":"unexpected"}]}`
	})
	c := NewClient(srv.URL + "/")
	if err := c.Login("a@b.c", "pw"); err != nil {
		t.Fatal(err)
	}
	draft := true
	ev, err := c.CreateEvent(EventInput{Title: "T", Description: "<p>D</p>", BeginsOn: "2026-10-02T10:00:00Z", OrganizerActorID: "1", AttributedToID: "2", Draft: &draft})
	if err != nil {
		t.Fatal(err)
	}
	if ev.UUID != "u5" || !ev.Draft {
		t.Errorf("got %+v", ev)
	}
	create := (*calls)[1]
	if create.auth != "Bearer tok" {
		t.Errorf("create sent auth %q", create.auth)
	}
	if !strings.Contains(create.query, "$title: String!") || !strings.Contains(create.query, "joinOptions: EXTERNAL") {
		t.Errorf("unexpected create query: %s", create.query)
	}
	if create.variables["draft"] != true || create.variables["attributedToId"] != "2" {
		t.Errorf("variables = %v", create.variables)
	}
	if _, ok := create.variables["endsOn"]; ok {
		t.Error("empty endsOn should be left out")
	}
}

func TestCancelSendsOnlyStatus(t *testing.T) {
	srv, calls := fakeServer(t, func(recorded) string {
		return `{"data":{"updateEvent":{"id":"5","uuid":"u5","status":"CANCELLED"}}}`
	})
	if _, err := NewClient(srv.URL).UpdateEvent("5", EventInput{Status: "CANCELLED"}); err != nil {
		t.Fatal(err)
	}
	vars := (*calls)[0].variables
	if len(vars) != 2 || vars["eventId"] != "5" || vars["status"] != "CANCELLED" {
		t.Errorf("variables = %v", vars)
	}
}

func TestUploadMediaSendsFilePart(t *testing.T) {
	srv, calls := fakeServer(t, func(recorded) string {
		return `{"data":{"uploadMedia":{"uuid":"m1"}}}`
	})
	id, err := NewClient(srv.URL).UploadMedia("1", "cover.png", "Alt", []byte("PNG"))
	if err != nil || id != "m1" {
		t.Fatalf("id=%q err=%v", id, err)
	}
	if got := (*calls)[0]; got.file != "PNG" || got.variables["file"] != "file" {
		t.Errorf("upload = %+v", got)
	}
}

func TestGroupEventsPagesAndReportsErrors(t *testing.T) {
	srv, _ := fakeServer(t, func(r recorded) string {
		if r.variables["page"].(float64) == 1 {
			events := make([]string, 50)
			for i := range events {
				events[i] = `{"id":"x"}`
			}
			return `{"data":{"group":{"id":"g","preferredUsername":"hub","organizedEvents":{"total":51,"elements":[` + strings.Join(events, ",") + `]}}}}`
		}
		return `{"data":{"group":{"id":"g","preferredUsername":"hub","organizedEvents":{"total":51,"elements":[{"id":"last"}]}}}}`
	})
	g, err := NewClient(srv.URL).GroupEvents("hub")
	if err != nil {
		t.Fatal(err)
	}
	if len(g.Events) != 51 || g.Events[50].ID != "last" {
		t.Errorf("got %d events", len(g.Events))
	}

	bad, _ := fakeServer(t, func(recorded) string { return `{"errors":[{"message":"Group not found"}]}` })
	if _, err := NewClient(bad.URL).GroupEvents("nope"); err == nil || !strings.Contains(err.Error(), "Group not found") {
		t.Errorf("err = %v", err)
	}
}
