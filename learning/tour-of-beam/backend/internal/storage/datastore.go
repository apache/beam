package storage

import (
	"context"
	"fmt"

	tob "beam.apache.org/learning/tour-of-beam/backend/internal"
	"cloud.google.com/go/datastore"
)

func PgNameKey(kind, nameId string, parentKey *datastore.Key) (key *datastore.Key) {
	key = datastore.NameKey(kind, nameId, parentKey)
	key.Namespace = PgNamespace
	return key
}

type DatastoreDb struct {
	Client *datastore.Client
}

func (d *DatastoreDb) GetContentTree(ctx context.Context, sdk tob.Sdk) (tree tob.ContentTree, err error) {
	var tbLP TbLearningPath

	lpKey := PgNameKey(TbLearningPathKind, sdk.String(), nil)
	if err := d.Client.Get(ctx, lpKey, &tbLP); err != nil {
		return tree, fmt.Errorf("error querying learning_path: %w", err)
	}
	// index.yaml should be applied for this query to work
	queryModules := datastore.NewQuery(TbLearningModuleKind).
		Namespace(PgNamespace).Ancestor(lpKey).Order("order")
	moduleKeys, err := d.Client.GetAll(ctx, queryModules, &tbLP.Modules)
	if err != nil {
		return tree, fmt.Errorf("error querying modules: %w", err)
	}

	if len(moduleKeys) != len(tbLP.Modules) {
		return tree, fmt.Errorf("invariant violation: keys %v vs objs %v", len(moduleKeys), len(tbLP.Modules))
	}

	for i := 0; i < len(moduleKeys); i++ {
		// index.yaml should be applied for this query to work
		queryUnits := datastore.NewQuery(TbLearningUnitKind).
			Namespace(PgNamespace).Ancestor(moduleKeys[i]).Order("order")
		if _, err = d.Client.GetAll(ctx, queryUnits, &tbLP.Modules[i].Units); err != nil {
			return tree, fmt.Errorf("getting units of module %v: %w", moduleKeys[i], err)
		}
	}
	return tbLP.ToEntity(), nil
}

// check if the interface is implemented
var _ Iface = &DatastoreDb{}
