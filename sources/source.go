package sources

type File struct {
	Name        string
	Description string
	Private     bool
}

type Source interface {
	Name() string
	Files() []File
}

type ProgressEvent struct {
	Source  string
	Step    string
	Detail  string
	Month   string
	File    string
	Current int
	Total   int
	Done    bool
}

type ProgressFunc func(ProgressEvent)
