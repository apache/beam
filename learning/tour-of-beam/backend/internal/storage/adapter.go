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
	if unit == nil {
		return nil
	}
	return &TbLearningUnit{
		Id:   unit.Id,
		Name: unit.Name,

		Description:       unit.Description,
		Hints:             unit.Hints,
		TaskSnippetId:     unit.TaskSnippetId,
		SolutionSnippetId: unit.SolutionSnippetId,

		Order:    order,
		Level:    level,
		NodeType: tob.NODE_UNIT,
	}
}

func FromDatastoreUnit(tbUnit *TbLearningUnit) *tob.Unit {
	if tbUnit == nil {
		return nil
	}
	return &tob.Unit{Id: tbUnit.Id, Name: tbUnit.Name}
}

func MakeDatastoreGroup(group *tob.Group, order, level int) *TbLearningGroup {
	if group == nil {
		return nil
	}
	return &TbLearningGroup{
		Id:   group.Name, // Just to make a subset of TbLearningUnit
		Name: group.Name,

		Order:    order,
		Level:    level,
		NodeType: tob.NODE_GROUP,
	}
}

func FromDatastoreGroup(tbGroup *TbLearningGroup) *tob.Group {
	if tbGroup == nil {
		return nil
	}
	return &tob.Group{Name: tbGroup.Name}
}

func MakeDatastoreModule(mod *tob.Module, order int) *TbLearningModule {
	return &TbLearningModule{
		Id:         mod.Id,
		Name:       mod.Name,
		Complexity: mod.Complexity,

		Order: order,
	}
}
