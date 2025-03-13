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

import (
	"errors"
)

var (
	ErrNoUnit = errors.New("unit not found")
	ErrNoUser = errors.New("user not found")
)

type SdkItem struct {
	Id    string `json:"id"`
	Title string `json:"title"`
}

type SdkList struct {
	Sdks []SdkItem `json:"sdks"`
}

type Unit struct {
	Id    string `json:"id"`
	Title string `json:"title"`

	// optional
	Description       string   `json:"description,omitempty"`
	Hints             []string `json:"hints,omitempty"`
	TaskSnippetId     string   `json:"taskSnippetId,omitempty"`
	SolutionSnippetId string   `json:"solutionSnippetId,omitempty"`

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
	Id    string `json:"id"`
	Title string `json:"title"`
	Nodes []Node `json:"nodes"`
}

type Node struct {
	Type  NodeType `json:"type"`
	Group *Group   `json:"group,omitempty"`
	Unit  *Unit    `json:"unit,omitempty"`
}

type Module struct {
	Id         string `json:"id"`
	Title      string `json:"title"`
	Complexity string `json:"complexity"`
	Nodes      []Node `json:"nodes"`
}

type ContentTree struct {
	Sdk     Sdk      `json:"sdkId"`
	Modules []Module `json:"modules"`
}

type CodeMessage struct {
	Code    string `json:"code"`
	Message string `json:"message,omitempty"`
}

type UnitProgress struct {
	Id            string `json:"id"`
	IsCompleted   bool   `json:"isCompleted"`
	UserSnippetId string `json:"userSnippetId,omitempty"`
}
type SdkProgress struct {
	Units []UnitProgress `json:"units"`
}

type UserCodeFile struct {
	Name    string `json:"name"`
	Content string `json:"content"`
	IsMain  bool   `json:"isMain,omitempty"`
}
type UserCodeRequest struct {
	Files           []UserCodeFile `json:"files"`
	PipelineOptions string         `json:"pipelineOptions"`
}
