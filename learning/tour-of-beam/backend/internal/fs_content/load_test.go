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
	"fmt"
	"path/filepath"
	"testing"

	tob "beam.apache.org/learning/tour-of-beam/backend/internal"
	"github.com/stretchr/testify/assert"
)

func getTaskNode(id string, sdk tob.Sdk) tob.Node {
	return tob.Node{Type: tob.NODE_UNIT, Unit: &tob.Unit{
		Id: id, Title: "Challenge Name",
		Description: "## Challenge description\n\nawesome description\n",
		Hints: []string{
			"## Hint 1\n\nhint 1",
			"## Hint 2\n\nhint 2",
		},
		TaskSnippetId:     fmt.Sprintf("TB_EXAMPLES_%s_ChallengeTask", sdk.StorageID()),
		SolutionSnippetId: fmt.Sprintf("TB_EXAMPLES_%s_ChallengeSolution", sdk.StorageID()),
	}}
}

func getExampleNode(id string, sdk tob.Sdk) tob.Node {
	return tob.Node{Type: tob.NODE_UNIT, Unit: &tob.Unit{
		Id:            id,
		Title:         "Example Unit Name",
		TaskSnippetId: fmt.Sprintf("TB_EXAMPLES_%s_ExampleName", sdk.StorageID()),
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
					getExampleNode("example1", tob.SDK_JAVA),
					getTaskNode("challenge1", tob.SDK_JAVA),
				},
			},
			{
				Id: "module2", Title: "Module Two", Complexity: "MEDIUM",
				Nodes: []tob.Node{
					getExampleNode("example21", tob.SDK_JAVA),
					getTaskNode("challenge21", tob.SDK_JAVA),
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
					getExampleNode("intro-unit", tob.SDK_PYTHON),
					{
						Type: tob.NODE_GROUP, Group: &tob.Group{
							Id:    "group1",
							Title: "The Group",
							Nodes: []tob.Node{
								getExampleNode("example1", tob.SDK_PYTHON),
								getTaskNode("challenge1", tob.SDK_PYTHON),
							},
						},
					},
				},
			},
		},
	}, trees[1])
}
