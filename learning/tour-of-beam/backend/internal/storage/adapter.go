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
	"fmt"

	tob "beam.apache.org/learning/tour-of-beam/backend/internal"
	"cloud.google.com/go/datastore"
)

func pgNameKey(kind, nameId string, parentKey *datastore.Key) (key *datastore.Key) {
	key = datastore.NameKey(kind, nameId, parentKey)
	key.Namespace = PgNamespace
	return key
}

// Get entity key from sdk & entity ID
// SDK_JAVA_{entityID}.
func datastoreKey(kind string, sdk tob.Sdk, id string, parent *datastore.Key) *datastore.Key {
	name := fmt.Sprintf("%s_%s", sdk.StorageID(), id)
	return pgNameKey(kind, name, parent)
}

func rootSdkKey(sdk tob.Sdk) *datastore.Key {
	return pgNameKey(PgSdksKind, sdk.StorageID(), nil)
}

func MakeUnitNode(unit *tob.Unit, order, level int) *TbLearningNode {
	if unit == nil {
		return nil
	}
	return &TbLearningNode{
		Id:    unit.Id,
		Title: unit.Title,

		Type:  tob.NODE_UNIT,
		Order: order,
		Level: level,

		Unit: &TbLearningUnit{
			Id:    unit.Id,
			Title: unit.Title,

			Description:       unit.Description,
			Hints:             unit.Hints,
			TaskSnippetId:     unit.TaskSnippetId,
			SolutionSnippetId: unit.SolutionSnippetId,
		},
	}
}

func MakeGroupNode(group *tob.Group, order, level int) *TbLearningNode {
	if group == nil {
		return nil
	}
	return &TbLearningNode{
		Id:    group.Id,
		Title: group.Title,

		Type:  tob.NODE_GROUP,
		Order: order,
		Level: level,

		Group: &TbLearningGroup{
			Title: group.Title,
		},
	}
}

// Depending on the projection, we either convert TbLearningUnit to a model
// Or we use common fields Id, Title to make it.
func FromDatastoreUnit(tbUnit *TbLearningUnit, id, title string) *tob.Unit {
	if tbUnit == nil {
		return &tob.Unit{Id: id, Title: title}
	}
	return &tob.Unit{
		Id:                tbUnit.Id,
		Title:             tbUnit.Title,
		Description:       tbUnit.Description,
		Hints:             tbUnit.Hints,
		TaskSnippetId:     tbUnit.TaskSnippetId,
		SolutionSnippetId: tbUnit.SolutionSnippetId,
	}
}

// Depending on the projection, we either convert TbLearningGroup to a model
// Or we use common fields Id, Title to make it.
func FromDatastoreGroup(tbGroup *TbLearningGroup, id, title string) *tob.Group {
	if tbGroup == nil {
		return &tob.Group{Id: id, Title: title}
	}
	return &tob.Group{
		Id:    tbGroup.Id,
		Title: tbGroup.Title,
	}
}

func FromDatastoreNode(tbNode TbLearningNode) tob.Node {
	node := tob.Node{
		Type: tbNode.Type,
	}
	switch tbNode.Type {
	case tob.NODE_GROUP:
		node.Group = FromDatastoreGroup(tbNode.Group, tbNode.Id, tbNode.Title)
	case tob.NODE_UNIT:
		node.Unit = FromDatastoreUnit(tbNode.Unit, tbNode.Id, tbNode.Title)
	default:
		panic("undefined node type")
	}
	return node
}

func MakeDatastoreModule(mod *tob.Module, order int) *TbLearningModule {
	return &TbLearningModule{
		Id:         mod.Id,
		Title:      mod.Title,
		Complexity: mod.Complexity,

		Order: order,
	}
}

func FromDatastoreUserProgress(tbUP TbUnitProgress) tob.UnitProgress {
	return tob.UnitProgress{
		Id:            tbUP.UnitID,
		IsCompleted:   tbUP.IsCompleted,
		UserSnippetId: tbUP.SnippetId,
	}
}
