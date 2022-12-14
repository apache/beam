// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	tob "beam.apache.org/learning/tour-of-beam/backend/internal"
	"cloud.google.com/go/datastore"
)

type DatastoreDb struct {
	Client *datastore.Client
}

// Query modules structure and content (recursively).
func (d *DatastoreDb) collectModules(ctx context.Context, tx *datastore.Transaction,
	rootKey *datastore.Key,
) ([]tob.Module, error) {
	// Custom index.yaml should be applied for this query to work
	// (Ancestor + Order)
	modules := make([]tob.Module, 0)
	var tbMods []TbLearningModule
	queryModules := datastore.NewQuery(TbLearningModuleKind).
		Namespace(PgNamespace).
		Ancestor(rootKey).
		Order("order").
		Transaction(tx)
	_, err := d.Client.GetAll(ctx, queryModules, &tbMods)
	if err != nil {
		return modules, fmt.Errorf("error querying modules for %v: %w", rootKey, err)
	}

	for _, tbMod := range tbMods {
		mod := tob.Module{Id: tbMod.Id, Title: tbMod.Title, Complexity: tbMod.Complexity}
		mod.Nodes, err = d.collectNodes(ctx, tx, tbMod.Key, 0)
		if err != nil {
			return modules, err
		}
		modules = append(modules, mod)
	}
	return modules, nil
}

// Get a group recursively.
// Params:
// - parentKey
// - level: depth of a node's children
// Recursively query/collect for each subgroup key, with level = level + 1.
func (d *DatastoreDb) collectNodes(ctx context.Context, tx *datastore.Transaction,
	parentKey *datastore.Key, level int,
) (nodes []tob.Node, err error) {
	var tbNodes []TbLearningNode

	// Custom index.yaml should be applied for this query to work
	queryNodes := datastore.NewQuery(TbLearningNodeKind).
		Namespace(PgNamespace).
		Ancestor(parentKey).
		FilterField("level", "=", level).
		Project("type", "id", "title").
		Order("order").
		Transaction(tx)
	if _, err = d.Client.GetAll(ctx, queryNodes, &tbNodes); err != nil {
		return nodes, fmt.Errorf("getting children of node %v: %w", parentKey, err)
	}

	// traverse the nodes which are groups, with level=level+1
	nodes = make([]tob.Node, 0, len(tbNodes))
	for _, tbNode := range tbNodes {
		node := FromDatastoreNode(tbNode)
		if node.Type == tob.NODE_GROUP {
			node.Group.Nodes, err = d.collectNodes(ctx, tx, tbNode.Key, level+1)
		}
		if err != nil {
			return nodes, err
		}
		nodes = append(nodes, node)
	}

	return nodes, nil
}

// Get learning content tree for SDK.
func (d *DatastoreDb) GetContentTree(ctx context.Context, sdk tob.Sdk) (tree tob.ContentTree, err error) {
	var tbLP TbLearningPath
	tree.Sdk = sdk

	_, err = d.Client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		rootKey := pgNameKey(TbLearningPathKind, sdk.StorageID(), nil)
		if err := d.Client.Get(ctx, rootKey, &tbLP); err != nil {
			return fmt.Errorf("error querying learning_path: %w", err)
		}
		tree.Modules, err = d.collectModules(ctx, tx, rootKey)
		if err != nil {
			return err
		}
		return nil
	}, datastore.ReadOnly)

	return tree, err
}

// Helper to clear all ToB Datastore entities related to a particular SDK
// They have one common ancestor key in tb_learning_path.
func (d *DatastoreDb) clearContentTree(ctx context.Context, tx *datastore.Transaction, sdk tob.Sdk) error {
	rootKey := pgNameKey(TbLearningPathKind, sdk.StorageID(), nil)
	q := datastore.NewQuery("").
		Namespace(PgNamespace).
		Ancestor(rootKey).
		KeysOnly().
		Transaction(tx)
	keys, err := d.Client.GetAll(ctx, q, nil)
	if err != nil {
		return err
	}

	for _, key := range keys {
		log.Println("deleting ", key)
	}

	err = tx.DeleteMulti(keys)
	if err != nil {
		return err
	}
	return tx.Delete(rootKey)
}

// Serialize a content tree to Datastore.
func (d *DatastoreDb) saveContentTree(tx *datastore.Transaction, tree *tob.ContentTree) error {
	sdk := tree.Sdk

	saveUnit := func(unit *tob.Unit, order, level int, parentKey *datastore.Key) error {
		unitKey := datastoreKey(TbLearningNodeKind, tree.Sdk, unit.Id, parentKey)
		_, err := tx.Put(unitKey, MakeUnitNode(unit, order, level))
		if err != nil {
			return fmt.Errorf("failed to put unit: %w", err)
		}
		return nil
	}

	// transaction-wide autoincremented Id
	// could have used numericID keys, if there was no transaction:
	// incomplete keys are resolved after Tx commit, and
	// we need to reference them in child nodes
	var groupId int = 0
	genGroupKey := func(parentKey *datastore.Key) *datastore.Key {
		groupId++
		return datastoreKey(TbLearningNodeKind,
			tree.Sdk, fmt.Sprintf("group%v", groupId), parentKey)
	}

	var saveNode func(tob.Node, int, int, *datastore.Key) error
	saveGroup := func(group *tob.Group, order, level int, parentKey *datastore.Key) error {
		groupKey := genGroupKey(parentKey)
		if _, err := tx.Put(groupKey, MakeGroupNode(group, order, level)); err != nil {
			return fmt.Errorf("failed to put group: %w", err)
		}
		for order, node := range group.Nodes {
			if err := saveNode(node, order, level+1, groupKey); err != nil {
				return err
			}
		}
		return nil
	}

	saveNode = func(node tob.Node, order, level int, parentKey *datastore.Key) error {
		if node.Type == tob.NODE_UNIT {
			return saveUnit(node.Unit, order, level, parentKey)
		} else if node.Type == tob.NODE_GROUP {
			return saveGroup(node.Group, order, level, parentKey)
		}

		return fmt.Errorf("unknown datastore node type: %v", node.Type)
	}

	rootKey := pgNameKey(TbLearningPathKind, tree.Sdk.StorageID(), nil)
	tbLP := TbLearningPath{Title: tree.Sdk.String()}
	if _, err := tx.Put(rootKey, &tbLP); err != nil {
		return fmt.Errorf("failed to put learning_path: %w", err)
	}

	for order, mod := range tree.Modules {
		modKey := datastoreKey(TbLearningModuleKind, sdk, mod.Id, rootKey)
		if _, err := tx.Put(modKey, MakeDatastoreModule(&mod, order)); err != nil {
			return fmt.Errorf("failed to put module: %w", err)
		}
		for order, node := range mod.Nodes {
			if err := saveNode(node, order /*module node is at level 0*/, 0, modKey); err != nil {
				return err
			}
		}
	}

	return nil
}

// Re-create content trees for each SDK in separate transaction.
func (d *DatastoreDb) SaveContentTrees(ctx context.Context, trees []tob.ContentTree) error {
	for _, tree := range trees {
		log.Println("Saving sdk tree", tree.Sdk)
		_, err := d.Client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
			if err := d.clearContentTree(ctx, tx, tree.Sdk); err != nil {
				return err
			}
			return d.saveContentTree(tx, &tree)
		})
		if err != nil {
			return err
		}
	}

	return nil
}

// get a custom projection of a learning unit
func (d *DatastoreDb) getUnit(ctx context.Context, sdk tob.Sdk, unitId string,
	projectionFunc func(*datastore.Query) *datastore.Query) (unit *tob.Unit, err error) {
	var tbNodes []TbLearningNode
	rootKey := pgNameKey(TbLearningPathKind, sdk.StorageID(), nil)

	query := datastore.NewQuery(TbLearningNodeKind).
		Namespace(PgNamespace).
		Ancestor(rootKey).
		FilterField("id", "=", unitId)
	query = projectionFunc(query)

	_, err = d.Client.GetAll(ctx, query, &tbNodes)
	if err != nil {
		return nil, fmt.Errorf("query unit failed: %w", err)
	}

	switch {
	case len(tbNodes) == 0:
		return nil, nil
	case len(tbNodes) > 1:
		return nil, fmt.Errorf("query by unitId returned %v units", len(tbNodes))
	}

	node := FromDatastoreNode(tbNodes[0])
	if node.Type != tob.NODE_UNIT {
		return nil, fmt.Errorf("wrong node type: %v, unit expected", node.Type)
	}
	return node.Unit, nil
}

// Get learning unit content by unitId
func (d *DatastoreDb) GetUnitContent(ctx context.Context, sdk tob.Sdk, unitId string) (unit *tob.Unit, err error) {
	return d.getUnit(ctx, sdk, unitId, func(q *datastore.Query) *datastore.Query {
		return q
	})
}

// Check if the unit exists, returns ErrNoUnit if not
func (d *DatastoreDb) CheckUnitExists(ctx context.Context, sdk tob.Sdk, unitId string) (err error) {
	unit, err := d.getUnit(ctx, sdk, unitId, func(q *datastore.Query) *datastore.Query {
		return q.Project("__key__", "type")
	})
	if err != nil {
		return err
	}
	if unit == nil {
		return tob.ErrNoUnit
	}
	return nil
}

func (d *DatastoreDb) SaveUser(ctx context.Context, uid string) error {
	userKey := pgNameKey(TbUserKind, uid, nil)

	_, err := d.Client.Put(ctx, userKey, &TbUser{UID: uid, LastVisitAt: time.Now()})
	if err != nil {
		return fmt.Errorf("failed to create tb_user: %w", err)
	}

	return nil
}

func (d *DatastoreDb) GetUserProgress(ctx context.Context, sdk tob.Sdk, uid string) (*tob.SdkProgress, error) {
	userKey := pgNameKey(TbUserKind, uid, nil)
	err := d.Client.Get(ctx, userKey, &TbUser{})
	if errors.Is(err, datastore.ErrNoSuchEntity) {
		return nil, tob.ErrNoUser
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get user: %w", err)
	}

	var tbUnits []TbUnitProgress
	query := datastore.NewQuery(TbUserProgressKind).
		Namespace(PgNamespace).
		Ancestor(userKey).
		FilterField("sdk", "=", rootSdkKey(sdk))

	_, err = d.Client.GetAll(ctx, query, &tbUnits)
	if err != nil {
		return nil, fmt.Errorf("query progress failed: %w", err)
	}

	sdkProgress := &tob.SdkProgress{Units: make([]tob.UnitProgress, 0)}
	for _, up := range tbUnits {
		sdkProgress.Units = append(sdkProgress.Units, FromDatastoreUserProgress(up))
	}

	return sdkProgress, nil
}

func (d *DatastoreDb) upsertUnitProgress(ctx context.Context, sdk tob.Sdk, unitId, uid string,
	applyChanges func(*TbUnitProgress) error) error {
	userKey := pgNameKey(TbUserKind, uid, nil)
	progressKey := datastoreKey(TbUserProgressKind, sdk, unitId, userKey)

	_, err := d.Client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		// default entity values
		progress := TbUnitProgress{
			Sdk:            rootSdkKey(sdk),
			UnitID:         unitId,
			PersistenceKey: tob.GeneratePersistentKey(),
		}

		if err := tx.Get(progressKey, &progress); err != nil && err != datastore.ErrNoSuchEntity {
			return err
		}
		if err := applyChanges(&progress); err != nil {
			return err
		}
		if _, err := tx.Put(progressKey, &progress); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to upsert tb_user_progress: %w", err)
	}
	return nil
}

func (d *DatastoreDb) SetUnitComplete(ctx context.Context, sdk tob.Sdk, unitId, uid string) error {
	return d.upsertUnitProgress(ctx, sdk, unitId, uid, func(p *TbUnitProgress) error {
		p.IsCompleted = true
		return nil
	})
}

func (d *DatastoreDb) SaveUserSnippetId(
	ctx context.Context, sdk tob.Sdk, unitId, uid string, externalSave func(string) (string, error),
) error {
	applyChanges := func(p *TbUnitProgress) error {
		snippetId, err := externalSave(p.PersistenceKey)
		if err != nil {
			return err
		}

		p.SnippetId = snippetId
		return nil
	}
	return d.upsertUnitProgress(ctx, sdk, unitId, uid, applyChanges)
}

func (d *DatastoreDb) DeleteProgress(ctx context.Context, uid string) error {
	userKey := pgNameKey(TbUserKind, uid, nil)

	_, err := d.Client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		query := datastore.NewQuery(TbUserProgressKind).
			Namespace(PgNamespace).
			Ancestor(userKey).
			KeysOnly().
			Transaction(tx)
		keys, err := d.Client.GetAll(ctx, query, nil)
		if err != nil {
			return fmt.Errorf("query tb_user_progress: %w", err)
		}
		log.Printf("deleting %v tb_user_progress entities\n", len(keys))
		if err := tx.DeleteMulti(keys); err != nil {
			return fmt.Errorf("delete %v enitities tb_user_progress: %w", len(keys), err)
		}
		if err := tx.Delete(userKey); err != nil {
			return fmt.Errorf("delete tb_user: %w", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}
	return nil
}

// check if the interface is implemented.
var _ Iface = &DatastoreDb{}
