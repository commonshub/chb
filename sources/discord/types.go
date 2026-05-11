package discord

import "encoding/json"

// Message represents a Discord message.
type Message struct {
	ID          string            `json:"id"`
	ChannelID   string            `json:"channel_id,omitempty"`
	Author      Author            `json:"author"`
	Content     string            `json:"content"`
	Timestamp   string            `json:"timestamp"`
	Attachments []Attachment      `json:"attachments"`
	Embeds      []json.RawMessage `json:"embeds"`
	Mentions    []Author          `json:"mentions"`
	Reactions   []Reaction        `json:"reactions,omitempty"`
}

type Author struct {
	ID         string  `json:"id"`
	Username   string  `json:"username"`
	GlobalName *string `json:"global_name"`
	Avatar     *string `json:"avatar"`
}

type Attachment struct {
	ID          string `json:"id"`
	URL         string `json:"url"`
	ProxyURL    string `json:"proxy_url"`
	ContentType string `json:"content_type,omitempty"`
}

type Reaction struct {
	Emoji Emoji `json:"emoji"`
	Count int   `json:"count"`
	Me    bool  `json:"me"`
}

type Emoji struct {
	ID   *string `json:"id"`
	Name string  `json:"name"`
}

type CacheFile struct {
	Messages  []Message `json:"messages"`
	CachedAt  string    `json:"cachedAt"`
	ChannelID string    `json:"channelId"`
}
