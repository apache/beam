package storage

import (
	"fmt"

	tob "beam.apache.org/learning/tour-of-beam/backend/internal"
	"cloud.google.com/go/datastore"
)

func sdk2Key(sdk tob.Sdk) string {
	switch sdk {
	case tob.SDK_GO:
		return "SDK_GO"
	case tob.SDK_PYTHON:
		return "SDK_PYTHON"
	case tob.SDK_JAVA:
		return "SDK_JAVA"
	case tob.SDK_SCIO:
		return "SDK_SCIO"
	}
	panic(fmt.Sprintf("Undefined key for sdk: %s", sdk))
}

func pgNameKey(kind, nameId string, parentKey *datastore.Key) (key *datastore.Key) {
	key = datastore.NameKey(kind, nameId, parentKey)
	key.Namespace = PgNamespace
	return key
}

// Get entity key from sdk & entity ID
// SDK_JAVA_{entityID}
func datastoreKey(kind string, sdk tob.Sdk, id string, parent *datastore.Key) *datastore.Key {
	name := fmt.Sprintf("%s_%s", sdk2Key(sdk), id)
	return pgNameKey(kind, name, parent)
}

func MakeDatastoreUnit(unit *tob.Unit, order, level int) *TbLearningUnit {
	return &TbLearningUnit{
		Id:   unit.Id,
		Name: unit.Name,

		Description:       unit.Description,
		Hints:             unit.Hints,
		TaskSnippetId:     unit.TaskSnippetId,
		SolutionSnippetId: unit.SolutionSnippetId,

		Order: order,
		Level: level,
	}
}

func MakeDatastoreGroup(group *tob.Group, order, level int) *TbLearningGroup {
	return &TbLearningGroup{
		Name: group.Name,

		Order: order,
		Level: level,
	}
}

func MakeDatastoreModule(mod *tob.Module, order int) *TbLearningModule {
	return &TbLearningModule{
		Id:         mod.Id,
		Name:       mod.Name,
		Complexity: mod.Complexity,

		Order: order,
	}
}
