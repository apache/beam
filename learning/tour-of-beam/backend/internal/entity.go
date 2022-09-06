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

package internal

type Unit struct {
	Id   string `json:"unitId"`
	Name string `json:"name"`

	// optional
	Description       string   `json:"description,omitempty"`
	Hints             []string `json:"hints,omitempty"`
	TaskSnippetId     string   `json:"taskSnippetId,omitempty"`
	SolutionSnippetId string   `json:"solutionSnippetId,omitempty"`
	TaskName          string   `json:"-"`
	SolutionName      string   `json:"-"`

	// optional, user-specific
	UserSnippetId string `json:"userSnippetId,omitempty"`
	IsCompleted   bool   `json:"isCompleted,omitempty"`
}

type NodeType int

const (
	NODE_UNDEFINED NodeType = iota
	NODE_UNIT
	NODE_GROUP
)

type Group struct {
	Name  string `json:"name"`
	Nodes []Node `json:"nodes"`
}

type Node struct {
	Type  NodeType `json:"type"`
	Group *Group   `json:"group,omitempty"`
	Unit  *Unit    `json:"unit,omitempty"`
}

type Module struct {
	Id         string `json:"moduleId"`
	Name       string `json:"name"`
	Complexity string `json:"complexity"`
	Nodes      []Node `json:"nodes"`
}

type ContentTree struct {
	Sdk     Sdk      `json:"sdk"`
	Modules []Module `json:"modules"`
}

type CodeMessage struct {
	Code    string `json:"code"`
	Message string `json:"message,omitempty"`
}
