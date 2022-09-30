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

package fs_content

import (
	"path/filepath"
	"testing"

	tob "beam.apache.org/learning/tour-of-beam/backend/internal"
	"github.com/stretchr/testify/assert"
)

func genUnitNode(id string) tob.Node {
	return tob.Node{Type: tob.NODE_UNIT, Unit: &tob.Unit{
		Id: id, Title: "Challenge Name",
		Description: "## Challenge description\n\nawesome description\n",
		Hints: []string{
			"## Hint 1\n\nhint 1",
			"## Hint 2\n\nhint 2",
		},
	}}
}

func TestSample(t *testing.T) {
	trees, err := CollectLearningTree(filepath.Join("..", "..", "samples", "learning-content"))
	assert.Nil(t, err)
	assert.Equal(t, 2, len(trees))
	assert.Equal(t, tob.ContentTree{
		Sdk: tob.SDK_JAVA,
		Modules: []tob.Module{
			{
				Id: "module1", Title: "Module One", Complexity: "BASIC",
				Nodes: []tob.Node{
					{Type: tob.NODE_UNIT, Unit: &tob.Unit{Id: "example1", Title: "Example Unit Name"}},
					genUnitNode("challenge1"),
				},
			},
			{
				Id: "module2", Title: "Module Two", Complexity: "MEDIUM",
				Nodes: []tob.Node{
					{Type: tob.NODE_UNIT, Unit: &tob.Unit{Id: "example21", Title: "Example Unit Name"}},
					genUnitNode("challenge21"),
				},
			},
		},
	}, trees[0])
	assert.Equal(t, tob.ContentTree{
		Sdk: tob.SDK_PYTHON,
		Modules: []tob.Module{
			{
				Id: "module1", Title: "Module One", Complexity: "BASIC",
				Nodes: []tob.Node{
					{Type: tob.NODE_UNIT, Unit: &tob.Unit{Id: "intro-unit", Title: "Intro Unit Name"}},
					{
						Type: tob.NODE_GROUP, Group: &tob.Group{
							Title: "The Group", Nodes: []tob.Node{
								{Type: tob.NODE_UNIT, Unit: &tob.Unit{Id: "example1", Title: "Example Unit Name"}},
								genUnitNode("challenge1"),
							},
						},
					},
				},
			},
		},
	}, trees[1])
}
