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

func sdkToKey(sdk tob.Sdk) string {
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
// SDK_JAVA_{entityID}.
func datastoreKey(kind string, sdk tob.Sdk, id string, parent *datastore.Key) *datastore.Key {
	name := fmt.Sprintf("%s_%s", sdkToKey(sdk), id)
	return pgNameKey(kind, name, parent)
}

func MakeUnitNode(unit *tob.Unit, order, level int) *TbLearningNode {
	if unit == nil {
		return nil
	}
	return &TbLearningNode{
		Id:   unit.Id,
		Name: unit.Name,

		Type:  tob.NODE_UNIT,
		Order: order,
		Level: level,

		Unit: &TbLearningUnit{
			Id:   unit.Id,
			Name: unit.Name,

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
		// ID doesn't make much sense for groups,
		// but we have to define it to include in queries
		Id:   group.Name,
		Name: group.Name,

		Type:  tob.NODE_GROUP,
		Order: order,
		Level: level,

		Group: &TbLearningGroup{
			Name: group.Name,
		},
	}
}

// Depending on the projection, we either convert TbLearningUnit to a model
// Or we use common fields Id, Name to make it.
func FromDatastoreUnit(tbUnit *TbLearningUnit, id, name string) *tob.Unit {
	if tbUnit == nil {
		return &tob.Unit{Id: id, Name: name}
	}
	return &tob.Unit{
		Id:                tbUnit.Id,
		Name:              tbUnit.Name,
		Description:       tbUnit.Description,
		Hints:             tbUnit.Hints,
		TaskSnippetId:     tbUnit.TaskSnippetId,
		SolutionSnippetId: tbUnit.SolutionSnippetId,
	}
}

// Depending on the projection, we either convert TbLearningGroup to a model
// Or we use common field Name to make it.
func FromDatastoreGroup(tbGroup *TbLearningGroup, name string) *tob.Group {
	if tbGroup == nil {
		return &tob.Group{Name: name}
	}
	return &tob.Group{
		Name: tbGroup.Name,
	}
}

func FromDatastoreNode(tbNode TbLearningNode) tob.Node {
	node := tob.Node{
		Type: tbNode.Type,
	}
	switch tbNode.Type {
	case tob.NODE_GROUP:
		node.Group = FromDatastoreGroup(tbNode.Group, tbNode.Name)
	case tob.NODE_UNIT:
		node.Unit = FromDatastoreUnit(tbNode.Unit, tbNode.Id, tbNode.Name)
	default:
		panic("undefined node type")
	}
	return node
}

func MakeDatastoreModule(mod *tob.Module, order int) *TbLearningModule {
	return &TbLearningModule{
		Id:         mod.Id,
		Name:       mod.Name,
		Complexity: mod.Complexity,

		Order: order,
	}
}
