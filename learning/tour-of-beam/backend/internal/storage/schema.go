package storage

const (
	PgNamespace = "Playground"

	TbLearningPathKind   = "tb_learning_path"
	TbLearningModuleKind = "tb_learning_module"
	TbLearningUnitKind   = "tb_learning_unit"
)

// tb_learning_path
type TbLearningPath struct {
	Sdk     string             `datastore:"sdk"`
	Modules []TbLearningModule `datastore:"-"`
}

// tb_learning_modules
type TbLearningModule struct {
	Id    string           `datastore:"id"`
	Name  string           `datastore:"name"`
	Order int              `datastore:"order"`
	Units []TbLearningUnit `datastore:"-"`
}

// tb_learning_units
type TbLearningUnit struct {
	Id    string `datastore:"id"`
	Name  string `datastore:"name"`
	Order int    `datastore:"order"`

	Description string `datastore:"description,noindex"`
	Hint        string `datastore:"hint,noindex"`

	TaskSnippetId     string `datastore:"taskSnippetId`
	SolutionSnippetId string `datastore:"solutionSnippetId`

	Complexity int `datastore:"complexity"`
}
