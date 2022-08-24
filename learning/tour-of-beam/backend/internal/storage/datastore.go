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
	"fmt"
	"log"

	tob "beam.apache.org/learning/tour-of-beam/backend/internal"
	"cloud.google.com/go/datastore"
)

type DatastoreDb struct {
	Client *datastore.Client
}

// Query modules structure and content (recursively)
func (d *DatastoreDb) collectModules(ctx context.Context, tx *datastore.Transaction,
	rootKey *datastore.Key) ([]tob.Module, error) {
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
		mod := tob.Module{Id: tbMod.Id, Name: tbMod.Name, Complexity: tbMod.Complexity}
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
// Recursively query/collect for each subgroup key, with level = level + 1
// If no subgroups found, query units and return as a group of units
// TODO: allow for mixed groups&units nodes
func (d *DatastoreDb) collectNodes(ctx context.Context, tx *datastore.Transaction,
	parentKey *datastore.Key, level int) (nodes []tob.Node, err error) {

	var allGroups []TbLearningGroup

	queryGroups := datastore.NewQuery(TbLearningGroupKind).
		Namespace(PgNamespace).
		Ancestor(parentKey).
		FilterField("level", "=", level).
		Order("order").
		Transaction(tx)
	if _, err := d.Client.GetAll(ctx, queryGroups, &allGroups); err != nil {
		return nodes, fmt.Errorf("getting groups of node %v: %w", parentKey, err)
	}

	if len(allGroups) > 0 {
		// node of groups
		for _, tbGroup := range allGroups {
			node := tob.Node{Type: tob.NODE_GROUP}
			node.Group = &tob.Group{Name: tbGroup.Name}
			node.Group.Nodes, err = d.collectNodes(ctx, tx, tbGroup.Key, level+1)
			if err != nil {
				return nodes, err
			}
			nodes = append(nodes, node)
		}
		return nodes, err
	}

	// node of units
	var allUnits []TbLearningUnit

	// Custom index.yaml should be applied for this query to work
	queryUnits := datastore.NewQuery(TbLearningUnitKind).
		Namespace(PgNamespace).
		Ancestor(parentKey).
		FilterField("level", "=", level).
		Project("id", "name").
		Order("order").
		Transaction(tx)
	if _, err = d.Client.GetAll(ctx, queryUnits, &allUnits); err != nil {
		return nodes, fmt.Errorf("getting units of node %v: %w", parentKey, err)
	}

	for _, tbUnit := range allUnits {
		node := tob.Node{Type: tob.NODE_UNIT}
		node.Unit = &tob.Unit{Id: tbUnit.Id, Name: tbUnit.Name}
		nodes = append(nodes, node)
	}
	return nodes, nil
}

// Get learning content tree for SDK
func (d *DatastoreDb) GetContentTree(ctx context.Context, sdk tob.Sdk) (tree tob.ContentTree, err error) {
	var tbLP TbLearningPath
	tree.Sdk = sdk

	_, err = d.Client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		rootKey := pgNameKey(TbLearningPathKind, sdk2Key(sdk), nil)
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
func (d *DatastoreDb) clearContentTree(ctx context.Context, tx *datastore.Transaction, sdk tob.Sdk) error {
	rootKey := pgNameKey(TbLearningPathKind, sdk2Key(sdk), nil)
	q := datastore.NewQuery("").
		Namespace(PgNamespace).
		Ancestor(rootKey).
		KeysOnly().
		Transaction(tx)
	keys, err := d.Client.GetAll(ctx, q, nil)
	if err != nil {
		return err
	}

	err = tx.DeleteMulti(keys)
	if err != nil {
		return err
	}
	return tx.Delete(rootKey)
}

// Serialize a content tree to Datastore
func (d *DatastoreDb) saveContentTree(tx *datastore.Transaction, tree *tob.ContentTree) error {
	sdk := tree.Sdk

	saveUnit := func(unit *tob.Unit, order, level int, parentKey *datastore.Key) error {
		unitKey := datastoreKey(TbLearningUnitKind, tree.Sdk, unit.Id, parentKey)
		_, err := tx.Put(unitKey, MakeDatastoreUnit(unit, order, level))
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
		return datastoreKey(TbLearningGroupKind,
			tree.Sdk, fmt.Sprintf("group%v", groupId), parentKey)
	}

	var saveNode func(tob.Node, int, int, *datastore.Key) error
	saveGroup := func(group *tob.Group, order, level int, parentKey *datastore.Key) error {
		groupKey := genGroupKey(parentKey)
		if _, err := tx.Put(groupKey, MakeDatastoreGroup(group, order, level)); err != nil {
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

		panic("Unknown node type")
	}

	rootKey := pgNameKey(TbLearningPathKind, sdk2Key(tree.Sdk), nil)
	tbLP := TbLearningPath{Name: tree.Sdk.String()}
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

// Re-create content trees for each SDK in separate transaction
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

// check if the interface is implemented
var _ Iface = &DatastoreDb{}
