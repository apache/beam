package internal

type Unit struct {
	Id   string `json:"unitId"`
	Name string `json:"name"`

	// optional
	Description       []byte   `json:"description,omitempty"`
	Hints             [][]byte `json:"hints,omitempty"`
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

func (t NodeType) String() string {
	switch t {
	case NODE_UNDEFINED:
		return "NODE_UNDEFINED"
	case NODE_UNIT:
		return "NODE_UNIT"
	case NODE_GROUP:
		return "NODE_GROUP"
	}
	panic("Unknown node type")
}

type Group struct {
	Name  string `json:"name"`
	Nodes []Node `json:"items"`
}

type Node struct {
	Type  NodeType
	Group Group
	Unit  Unit
}

type Module struct {
	Id         string `json:"moduleId"`
	Name       string `json:"name"`
	Complexity string `json:"complexity"`
	Nodes      []Node `json:"items"`
}

type ContentTree struct {
	Sdk     Sdk      `json:"sdk"`
	Modules []Module `json:"modules"`
}

type CodeMessage struct {
	Code    string `json:"code"`
	Message string `json:"message,omitempty"`
}
