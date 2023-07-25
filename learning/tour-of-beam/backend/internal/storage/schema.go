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
	"os"
	"time"

	tob "beam.apache.org/learning/tour-of-beam/backend/internal"
	"cloud.google.com/go/datastore"
)

var (
	PgNamespace string
)

func init() {
	ns, ok := os.LookupEnv("DATASTORE_NAMESPACE")
	//empty namespace corresponds to Default Datastore namespace and is perfectly fine
	PgNamespace = ns
	if !ok { // default to Playground
		PgNamespace = "Playground"
	}
}

// ToB Datastore schema
// - Content tree has a root entity in tb_learning_path,
//   descendant entities in tb_learning_module/group/unit have it as a common ancestor
// - learning path consists of modules, modules consist of groups and units
// - Ordering is established by "order" property
// - To limit ancestor queries by only first-level descendants, "level" property is used

const (
	TbLearningPathKind   = "tb_learning_path"
	TbLearningModuleKind = "tb_learning_module"
	TbLearningNodeKind   = "tb_learning_node"
	TbUserKind           = "tb_user"
	TbUserProgressKind   = "tb_user_progress"

	PgSnippetsKind = "pg_snippets"
	PgSdksKind     = "pg_sdks"

	OriginTbExamples = "TB_EXAMPLES"
)

// tb_learning_path.
type TbLearningPath struct {
	Key   *datastore.Key `datastore:"__key__"`
	Title string         `datastore:"title"`
}

// tb_learning_module.
type TbLearningModule struct {
	Key        *datastore.Key `datastore:"__key__"`
	Id         string         `datastore:"id"`
	Title      string         `datastore:"title"`
	Complexity string         `datastore:"complexity"`

	// internal, only db
	Order int `datastore:"order"`
}

// tb_learning_node.group.
type TbLearningGroup struct {
	Id    string `datastore:"id"`
	Title string `datastore:"title"`
}

// tb_learning_node.unit
// Learning Unit content.
type TbLearningUnit struct {
	Id          string   `datastore:"id"`
	Title       string   `datastore:"title"`
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
	Id    string `datastore:"id"`
	Title string `datastore:"title"`

	// type-specific nested info
	Unit  *TbLearningUnit  `datastore:"unit,noindex"`
	Group *TbLearningGroup `datastore:"group,noindex"`

	// internal datastore-only fields
	// key: <sdk>_<id>
	Key   *datastore.Key `datastore:"__key__"`
	Order int            `datastore:"order"`
	Level int            `datastore:"level"`
}

type TbUser struct {
	Key         *datastore.Key `datastore:"__key__"`
	UID         string         `datastore:"uid"`
	LastVisitAt time.Time      `datastore:"lastVisitAt"`
}

type TbUnitProgress struct {
	Key *datastore.Key `datastore:"__key__"`
	Sdk *datastore.Key `datastore:"sdk"`

	UnitID         string `datastore:"unitId"`
	IsCompleted    bool   `datastore:"isCompleted"`
	SnippetId      string `datastore:"snippetId"`
	PersistenceKey string `datastore:"persistenceKey,noindex"`
}

type PgSnippets struct {
	Key    *datastore.Key `datastore:"__key__"`
	Origin string         `datastore:"origin"`
	Sdk    *datastore.Key `datastore:"sdk"`
}
