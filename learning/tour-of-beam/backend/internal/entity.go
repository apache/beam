package internal

type Unit struct {
	Id          string `json:"unitId"`
	Name        string `json:"name"`
	IsCompleted bool   `json:"completed"`
}

type Module struct {
	Id         string        `json:"moduleId"`
	Name       string        `json:"name"`
	Complexity string        `json:"complexity"`
	Units      []UnitContent `json:"units"`
}

type ContentTree struct {
	Sdk     Sdk      `json:"sdk"`
	Modules []Module `json:"modules"`
}

type UnitContent struct {
	Id                string `json:"unitId"`
	Name              string `json:"name"`
	Description       string `json:"description,omitempty"`
	Hint              string `json:"hint"`
	TaskSnippetId     string `json:"taskSnippetId"`
	SolutionSnippetId string `json:"solutionSnippetId"`
	UserSnippetId     string `json:"userSnippetId"`

	TaskName     string `json:"-"`
	SolutionName string `json:"-"`
}

type CodeMessage struct {
	Code    string `json:"code"`
	Message string `json:"message,omitempty"`
}
