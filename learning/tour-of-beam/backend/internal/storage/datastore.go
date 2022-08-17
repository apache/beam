package storage

import (
	"context"
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

type DatastoreDb struct {
	Client *datastore.Client
}

// Get learning content tree for SDK
func (d *DatastoreDb) GetContentTree(ctx context.Context, sdk tob.Sdk) (tree tob.ContentTree, err error) {
	var tbLP TbLearningPath

	_, err = d.Client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		// TODO search by last revision
		lpKey := pgNameKey(TbLearningPathKind, sdk2Key(sdk), nil)
		if err := d.Client.Get(ctx, lpKey, &tbLP); err != nil {
			return fmt.Errorf("error querying learning_path: %w", err)
		}
		// index.yaml should be applied for this query to work
		queryModules := datastore.NewQuery(TbLearningModuleKind).
			Namespace(PgNamespace).Ancestor(lpKey).Order("order").Transaction(tx)
		moduleKeys, err := d.Client.GetAll(ctx, queryModules, &tbLP.Modules)
		if err != nil {
			return fmt.Errorf("error querying modules: %w", err)
		}

		if len(moduleKeys) != len(tbLP.Modules) {
			return fmt.Errorf("invariant violation: keys %v vs objs %v", len(moduleKeys), len(tbLP.Modules))
		}

		for i := 0; i < len(moduleKeys); i++ {
			// index.yaml should be applied for this query to work
			queryUnits := datastore.NewQuery(TbLearningUnitKind).
				Namespace(PgNamespace).Ancestor(moduleKeys[i]).Order("order").Transaction(tx)
			if _, err = d.Client.GetAll(ctx, queryUnits, &tbLP.Modules[i].Units); err != nil {
				return fmt.Errorf("getting units of module %v: %w", moduleKeys[i], err)
			}
		}
		return nil
	}, datastore.ReadOnly)
	if err != nil {
		return tree, err
	}

	return tbLP.ToEntity(), nil
}

func (d *DatastoreDb) clearContentTree(ctx context.Context, tx *datastore.Transaction) error {
	d.Client.DeleteMulti()
	return nil
}

// Create a learning content tree with given revision
func (d *DatastoreDb) saveContentTree(ctx context.Context, tree tob.ContentTree, revision int) error {
	d.Client.DeleteMulti()
	return nil
}

func (d *DatastoreDb) SaveContentTrees(ctx context.Context, trees []tob.ContentTree) error {
	_, err := d.Client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		for _, tree := range trees {
			if err := d.saveContentTree(ctx, tree, tx); err != nil {
				return err
			}
		}
		return nil
	})

	return err
}

// check if the interface is implemented
var _ Iface = &DatastoreDb{}
