package internal

type Unit struct {
	Id   string `json:"unitId"`
	Name string `json:"name"`

	// optional
	Description       string   `json:"description,omitempty"`
	Hints             []string `json:"hints,omitempty"`
	TaskSnippetId     string   `json:"taskSnippetId,omitempty"`
	SolutionSnippetId string   `json:"solutionSnippetId,omitempty"`
	TaskName          string   `json:"-"`
	SolutionName      string   `json:"-"`

	// optional, user-specific
	UserSnippetId string `json:"userSnippetId,omitempty"`
	IsCompleted   string `json:"is_completed,omitempty"`
}

type NodeType int

const (
	NODE_UNDEFINED NodeType = iota
	NODE_UNIT
	NODE_GROUP
)

type Group struct {
	Name  string `json:"name"`
	Nodes []Node `json:"nodes"`
}

type Node struct {
	Type  NodeType `json:"type"`
	Group *Group   `json:"group,omitempty"`
	Unit  *Unit    `json:"unit,omitempty"`
}

type Module struct {
	Id         string `json:"moduleId"`
	Name       string `json:"name"`
	Complexity string `json:"complexity"`
	Nodes      []Node `json:"nodes"`
}

type ContentTree struct {
	Sdk     Sdk      `json:"sdk"`
	Modules []Module `json:"modules"`
}

type CodeMessage struct {
	Code    string `json:"code"`
	Message string `json:"message,omitempty"`
}
