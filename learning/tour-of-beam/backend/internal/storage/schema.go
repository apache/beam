package storage

type Unit struct {
	Id          string `json:"unitId"`
	Name        string `json:"name"`
	IsCompleted bool   `json:"completed"`
}

type Module struct {
	Id    string `json:"moduleId"`
	Name  string `json:"name"`
	Units []Unit `json:"units"`
}

type ContentTree struct {
	Modules []Module `json:"modules"`
}

type UnitContent struct {
	Unit
	Description         string `json:"description"`
	Hint                string `json:"hint"`
	AssignmentSnippetId string `json:"assignment"`
	SolutionSnippetId   string `json:"solution"`
	UserSnippetId       string `json:"userSnippet"`
}
