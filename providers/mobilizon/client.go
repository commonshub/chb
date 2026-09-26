package mobilizon

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
	"time"
)

// Client talks to the GraphQL API of one Mobilizon instance.
type Client struct {
	BaseURL string
	HTTP    *http.Client
	token   string
}

func NewClient(baseURL string) *Client {
	return &Client{
		BaseURL: strings.TrimRight(baseURL, "/"),
		HTTP:    &http.Client{Timeout: 60 * time.Second},
	}
}

type gqlError struct {
	Message string `json:"message"`
}

type gqlResponse struct {
	Data   json.RawMessage `json:"data"`
	Errors []gqlError      `json:"errors"`
}

// Do runs one GraphQL operation and decodes its data into out (may be nil).
func (c *Client) Do(query string, variables map[string]interface{}, out interface{}) error {
	body, err := json.Marshal(map[string]interface{}{"query": query, "variables": variables})
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", c.BaseURL+"/api", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	return c.send(req, out)
}

// DoUpload runs a GraphQL operation with one file attached. Absinthe's
// upload format: the Upload variable holds the name of a multipart field.
func (c *Client) DoUpload(query string, variables map[string]interface{}, field, filename string, file []byte, out interface{}) error {
	vars, err := json.Marshal(variables)
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	_ = w.WriteField("query", query)
	_ = w.WriteField("variables", string(vars))
	part, err := w.CreateFormFile(field, filename)
	if err != nil {
		return err
	}
	if _, err := part.Write(file); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	req, err := http.NewRequest("POST", c.BaseURL+"/api", &buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	return c.send(req, out)
}

func (c *Client) send(req *http.Request, out interface{}) error {
	req.Header.Set("Accept", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	var res gqlResponse
	if err := json.Unmarshal(raw, &res); err != nil {
		return fmt.Errorf("mobilizon: HTTP %d: %s", resp.StatusCode, truncate(string(raw), 200))
	}
	if len(res.Errors) > 0 {
		msgs := make([]string, len(res.Errors))
		for i, e := range res.Errors {
			msgs[i] = e.Message
		}
		return fmt.Errorf("mobilizon: %s", strings.Join(msgs, "; "))
	}
	if out != nil && len(res.Data) > 0 {
		return json.Unmarshal(res.Data, out)
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// Login exchanges email and password for an access token used by later calls.
func (c *Client) Login(email, password string) error {
	var data struct {
		Login struct {
			AccessToken string `json:"accessToken"`
		} `json:"login"`
	}
	err := c.Do(`mutation($email: String!, $password: String!) {
		login(email: $email, password: $password) { accessToken }
	}`, map[string]interface{}{"email": email, "password": password}, &data)
	if err != nil {
		return err
	}
	c.token = data.Login.AccessToken
	return nil
}

// Address is Mobilizon's postal address, as read and as written.
type Address struct {
	Description string `json:"description,omitempty"`
	Street      string `json:"street,omitempty"`
	PostalCode  string `json:"postalCode,omitempty"`
	Locality    string `json:"locality,omitempty"`
	Country     string `json:"country,omitempty"`
	Geom        string `json:"geom,omitempty"`
	Timezone    string `json:"timezone,omitempty"`
}

// Event is a group event as pulled.
type Event struct {
	ID                       string   `json:"id"`
	UUID                     string   `json:"uuid"`
	URL                      string   `json:"url"`
	Title                    string   `json:"title"`
	BeginsOn                 string   `json:"beginsOn"`
	EndsOn                   string   `json:"endsOn"`
	Status                   string   `json:"status"`
	Draft                    bool     `json:"draft"`
	JoinOptions              string   `json:"joinOptions"`
	ExternalParticipationURL string   `json:"externalParticipationUrl"`
	PhysicalAddress          *Address `json:"physicalAddress"`
}

// Group is the group we publish to, with its events.
type Group struct {
	ID                string  `json:"id"`
	PreferredUsername string  `json:"preferredUsername"`
	Name              string  `json:"name"`
	Events            []Event `json:"events"`
}

const eventFields = `id uuid url title beginsOn endsOn status draft joinOptions externalParticipationUrl
	physicalAddress { description street postalCode locality country geom timezone }`

// GroupEvents lists every event the group organises, paging through them.
func (c *Client) GroupEvents(username string) (*Group, error) {
	const pageSize = 50
	group := &Group{}
	for page := 1; ; page++ {
		var data struct {
			Group *struct {
				ID                string `json:"id"`
				PreferredUsername string `json:"preferredUsername"`
				Name              string `json:"name"`
				OrganizedEvents   struct {
					Total    int     `json:"total"`
					Elements []Event `json:"elements"`
				} `json:"organizedEvents"`
			} `json:"group"`
		}
		err := c.Do(`query($name: String!, $page: Int, $limit: Int) {
			group(preferredUsername: $name) {
				id preferredUsername name
				organizedEvents(page: $page, limit: $limit) { total elements { `+eventFields+` } }
			}
		}`, map[string]interface{}{"name": username, "page": page, "limit": pageSize}, &data)
		if err != nil {
			return nil, err
		}
		if data.Group == nil {
			return nil, fmt.Errorf("mobilizon: group @%s not found", username)
		}
		group.ID, group.PreferredUsername, group.Name = data.Group.ID, data.Group.PreferredUsername, data.Group.Name
		group.Events = append(group.Events, data.Group.OrganizedEvents.Elements...)
		if len(data.Group.OrganizedEvents.Elements) < pageSize || len(group.Events) >= data.Group.OrganizedEvents.Total {
			return group, nil
		}
	}
}

// LoggedPersonID is the profile (actor) the credentials act as.
func (c *Client) LoggedPersonID() (string, error) {
	var data struct {
		LoggedPerson struct {
			ID string `json:"id"`
		} `json:"loggedPerson"`
	}
	if err := c.Do(`{ loggedPerson { id } }`, nil, &data); err != nil {
		return "", err
	}
	return data.LoggedPerson.ID, nil
}

// UploadMedia stores a picture and returns its media UUID.
func (c *Client) UploadMedia(actorID, name, alt string, file []byte) (string, error) {
	var data struct {
		UploadMedia struct {
			UUID string `json:"uuid"`
		} `json:"uploadMedia"`
	}
	err := c.DoUpload(`mutation($actorId: ID, $name: String!, $alt: String, $file: Upload!) {
		uploadMedia(actorId: $actorId, name: $name, alt: $alt, file: $file) { uuid }
	}`, map[string]interface{}{"actorId": actorID, "name": name, "alt": alt, "file": "file"}, "file", name, file, &data)
	if err != nil {
		return "", err
	}
	return data.UploadMedia.UUID, nil
}

// EventInput is what we write on create and update. Empty fields are left
// out of the request, so an update that only sets Status changes nothing else.
type EventInput struct {
	Title                    string
	Description              string
	BeginsOn                 string
	EndsOn                   string
	Address                  *Address
	ExternalParticipationURL string
	Tags                     []string
	PictureMediaUUID         string
	Status                   string // CONFIRMED or CANCELLED
	OrganizerActorID         string
	AttributedToID           string
	// Draft is nil to leave the draft state alone, true to keep the event
	// a draft, false to publish it.
	Draft *bool
}

func (in EventInput) variables() map[string]interface{} {
	v := map[string]interface{}{}
	set := func(key, value string) {
		if value != "" {
			v[key] = value
		}
	}
	set("title", in.Title)
	set("description", in.Description)
	set("beginsOn", in.BeginsOn)
	set("endsOn", in.EndsOn)
	set("organizerActorId", in.OrganizerActorID)
	set("attributedToId", in.AttributedToID)
	set("externalParticipationUrl", in.ExternalParticipationURL)
	set("status", in.Status)
	if in.Tags != nil {
		v["tags"] = in.Tags
	}
	if in.Draft != nil {
		v["draft"] = *in.Draft
	}
	if in.Address != nil {
		v["physicalAddress"] = in.Address
	}
	if in.PictureMediaUUID != "" {
		v["picture"] = map[string]string{"mediaUuid": in.PictureMediaUUID}
	}
	return v
}

const eventVarDefs = `$title: String, $description: String, $beginsOn: DateTime, $endsOn: DateTime,
	$organizerActorId: ID, $attributedToId: ID, $externalParticipationUrl: String, $tags: [String],
	$physicalAddress: AddressInput, $picture: MediaInput, $draft: Boolean, $status: EventStatus`

const eventArgs = `title: $title, description: $description, beginsOn: $beginsOn, endsOn: $endsOn,
	organizerActorId: $organizerActorId, attributedToId: $attributedToId,
	externalParticipationUrl: $externalParticipationUrl, joinOptions: EXTERNAL,
	tags: $tags, physicalAddress: $physicalAddress, picture: $picture,
	draft: $draft, status: $status, visibility: PUBLIC`

// CreateEvent creates an event and returns it.
func (c *Client) CreateEvent(in EventInput) (*Event, error) {
	var data struct {
		CreateEvent Event `json:"createEvent"`
	}
	// createEvent declares title, description, beginsOn and organizerActorId
	// non-null; the variables are typed to match.
	q := strings.NewReplacer(
		"$title: String,", "$title: String!,",
		"$description: String,", "$description: String!,",
		"$beginsOn: DateTime,", "$beginsOn: DateTime!,",
		"$organizerActorId: ID,", "$organizerActorId: ID!,",
	).Replace(eventVarDefs)
	err := c.Do(`mutation(`+q+`) { createEvent(`+eventArgs+`) { `+eventFields+` } }`, in.variables(), &data)
	if err != nil {
		return nil, err
	}
	return &data.CreateEvent, nil
}

// UpdateEvent overwrites an event's fields and returns it.
func (c *Client) UpdateEvent(eventID string, in EventInput) (*Event, error) {
	var data struct {
		UpdateEvent Event `json:"updateEvent"`
	}
	vars := in.variables()
	vars["eventId"] = eventID
	err := c.Do(`mutation($eventId: ID!, `+eventVarDefs+`) { updateEvent(eventId: $eventId, `+eventArgs+`) { `+eventFields+` } }`, vars, &data)
	if err != nil {
		return nil, err
	}
	return &data.UpdateEvent, nil
}
