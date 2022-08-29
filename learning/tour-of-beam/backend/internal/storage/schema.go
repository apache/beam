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
	tob "beam.apache.org/learning/tour-of-beam/backend/internal"
	"cloud.google.com/go/datastore"
)

// ToB Datastore schema
// - Content tree has a root entity in tb_learning_path,
//   descendant entities in tb_learning_module/group/unit have it as a common ancestor
// - learning path consists of modules, modules consist of groups and units
// - Ordering is established by "order" property
// - To limit ancestor queries by only first-level descendants, "level" property is used

const (
	PgNamespace = "Playground"

	TbLearningPathKind   = "tb_learning_path"
	TbLearningModuleKind = "tb_learning_module"
	TbLearningNodeKind   = "tb_learning_node"

	PgSnippetsKind = "pg_snippets"
	PgSdksKind     = "pg_sdks"
)

// tb_learning_path.
type TbLearningPath struct {
	Key  *datastore.Key `datastore:"__key__"`
	Name string         `datastore:"name"`
}

// tb_learning_module.
type TbLearningModule struct {
	Key        *datastore.Key `datastore:"__key__"`
	Id         string         `datastore:"id"`
	Name       string         `datastore:"name"`
	Complexity string         `datastore:"complexity"`

	// internal, only db
	Order int `datastore:"order"`
}

// tb_learning_node.group.
type TbLearningGroup struct {
	Name string `datastore:"name"`
}

// tb_learning_node.unit
// Learning Unit content.
type TbLearningUnit struct {
	Id          string   `datastore:"id"`
	Name        string   `datastore:"name"`
	Description string   `datastore:"description,noindex"`
	Hints       []string `datastore:"hints,noindex"`

	TaskSnippetId     string `datastore:"taskSnippetId"`
	SolutionSnippetId string `datastore:"solutionSnippetId"`
}

// tb_learning_node
// Container for learning tree nodes, which are either groups or units.
type TbLearningNode struct {
	Type tob.NodeType `datastore:"type"`
	// common fields, duplicate same fields from the nested entities
	// (needed to allow projection when getting the content tree)
	Id   string `datastore:"id"`
	Name string `datastore:"name"`

	// type-specific nested info
	Unit  *TbLearningUnit  `datastore:"unit,noindex"`
	Group *TbLearningGroup `datastore:"group,noindex"`

	// internal datastore-only fields
	// key: <sdk>_<id>
	Key   *datastore.Key `datastore:"__key__"`
	Order int            `datastore:"order"`
	Level int            `datastore:"level"`
}

type PgSnippets struct {
	Key    *datastore.Key `datastore:"__key__"`
	Origin string         `datastore:"origin"`
	Sdk    *datastore.Key `datastore:"sdk"`
}
