package fs_content

import (
	"path/filepath"
	"testing"

	tob "beam.apache.org/learning/tour-of-beam/backend/internal"
	"github.com/stretchr/testify/assert"
)

func genUnitNode(id string) tob.Node {
	return tob.Node{Type: tob.NODE_UNIT, Unit: &tob.Unit{
		Id: id, Name: "Challenge Name",
		Description: "## Challenge description\n\nbla bla bla",
		Hints: []string{
			"## Hint 1\n\napply yourself :)",
			"## Hint 2\n\napply more",
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
			{Id: "module1", Name: "Module One", Complexity: "BASIC",
				Nodes: []tob.Node{
					{Type: tob.NODE_UNIT, Unit: &tob.Unit{Id: "example1", Name: "Example Unit Name"}},
					genUnitNode("challenge1"),
				},
			},
			{Id: "module2", Name: "Module Two", Complexity: "MEDIUM",
				Nodes: []tob.Node{
					{Type: tob.NODE_UNIT, Unit: &tob.Unit{Id: "example21", Name: "Example Unit Name"}},
					genUnitNode("challenge21"),
				},
			},
		},
	}, trees[0])
	assert.Equal(t, tob.ContentTree{
		Sdk: tob.SDK_PYTHON,
		Modules: []tob.Module{
			{Id: "module1", Name: "Module One", Complexity: "BASIC",
				Nodes: []tob.Node{
					{Type: tob.NODE_GROUP, Group: &tob.Group{
						Name: "The Group", Nodes: []tob.Node{
							{Type: tob.NODE_UNIT, Unit: &tob.Unit{Id: "example1", Name: "Example Unit Name"}},
							genUnitNode("challenge1"),
						},
					},
					},
				}}}}, trees[1])
}
