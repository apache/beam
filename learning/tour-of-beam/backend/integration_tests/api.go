// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

// Reduced version of API, for integration tests and documentation
// * No hidden fields
// * Internal enumerations: sdk, node.type to string params

type sdkListResponse struct {
	Names []string
}

type Unit struct {
	Id   string `json:"unitId"`
	Name string `json:"name"`

	// optional
	Description       string   `json:"description,omitempty"`
	Hints             []string `json:"hints,omitempty"`
	TaskSnippetId     string   `json:"taskSnippetId,omitempty"`
	SolutionSnippetId string   `json:"solutionSnippetId,omitempty"`

	// optional, user-specific
	UserSnippetId string `json:"userSnippetId,omitempty"`
	IsCompleted   bool   `json:"isCompleted,omitempty"`
}

type Group struct {
	Name  string `json:"name"`
	Nodes []Node `json:"nodes"`
}

type Node struct {
	Type  string `json:"type"`
	Group *Group `json:"group,omitempty"`
	Unit  *Unit  `json:"unit,omitempty"`
}

type Module struct {
	Id         string `json:"moduleId"`
	Name       string `json:"name"`
	Complexity string `json:"complexity"`
	Nodes      []Node `json:"nodes"`
}

type ContentTree struct {
	Sdk     string   `json:"sdk"`
	Modules []Module `json:"modules"`
}

type ErrorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message,omitempty"`
}
