package storage

import "cloud.google.com/go/datastore"

const (
	PgNamespace = "Playground"

	TbLearningPathKind   = "tb_learning_path"
	TbLearningModuleKind = "tb_learning_module"
	TbLearningUnitKind   = "tb_learning_unit"

	PgSnippetsKind = "pg_snippets"
	PgSdksKind     = "pg_sdks"
)

// tb_learning_path
type TbLearningPath struct {
	Key     *datastore.Key     `datastore:"__key__"`
	Name    string             `datastore:"name"`
	Sdk     *datastore.Key     `datastore:"sdk"`
	Modules []TbLearningModule `datastore:"-"`
}

// tb_learning_modules
type TbLearningModule struct {
	Key        *datastore.Key   `datastore:"__key__"`
	Order      int              `datastore:"order"`
	Name       string           `datastore:"name"`
	Complexity int              `datastore:"complexity"`
	Units      []TbLearningUnit `datastore:"-"`
}

// tb_learning_units
type TbLearningUnit struct {
	Key   *datastore.Key `datastore:"__key__"`
	Order int            `datastore:"order"`

	Name        string `datastore:"name"`
	Description string `datastore:"description,noindex"`
	Hint        string `datastore:"hint,noindex"`

	TaskSnippetId     string `datastore:"taskSnippetId`
	SolutionSnippetId string `datastore:"solutionSnippetId`
}

type PgSnippets struct {
	Key    *datastore.Key `datastore:"__key__"`
	Origin string         `datastore:"origin"`
	Sdk    *datastore.Key `datastore:"sdk"`
}
