package fs_content

import (
	"path/filepath"
	"testing"

	tob "beam.apache.org/learning/tour-of-beam/backend/internal"
	"github.com/stretchr/testify/assert"
)

func TestSample(t *testing.T) {
	trees, err := CollectLearningTree(filepath.Join("..", "..", "samples", "learning-content"))
	assert.Nil(t, err)
	assert.Equal(t, 2, len(trees))
	assert.Equal(t, tob.ContentTree{
		Sdk: tob.SDK_JAVA,
		Modules: []tob.Module{
			{Id: "module1", Name: "Module One", Complexity: "BASIC",
				Nodes: []tob.Node{
					{Type: tob.NODE_UNIT, Unit: tob.Unit{Id: "example1", Name: "Example Unit Name"}},
					{Type: tob.NODE_UNIT, Unit: tob.Unit{
						Id: "challenge1", Name: "Challenge Name",
						Description: []byte("## Challenge description\n\nbla bla bla"),
						Hints: [][]byte{
							[]byte("## Hint 1\n\napply yourself :)"),
							[]byte("## Hint 2\n\napply more"),
						},
					}},
				},
			},
			{Id: "module2", Name: "Module Two", Complexity: "MEDIUM",
				Nodes: []tob.Node{
					{Type: tob.NODE_UNIT, Unit: tob.Unit{Id: "example21", Name: "Example Unit Name"}},
					{Type: tob.NODE_UNIT, Unit: tob.Unit{
						Id: "challenge21", Name: "Challenge Name",
						Description: []byte("## Challenge description\n\nbla bla bla"),
						Hints: [][]byte{
							[]byte("## Hint 1\n\napply yourself :)"),
							[]byte("## Hint 2\n\napply more"),
						},
					}},
				},
			},
		},
	}, trees[0])
	assert.Equal(t, tob.ContentTree{
		Sdk: tob.SDK_PYTHON,
		Modules: []tob.Module{
			{Id: "module1", Name: "Module One", Complexity: "BASIC",
				Nodes: []tob.Node{
					{Type: tob.NODE_GROUP, Group: tob.Group{
						Name: "The Group", Nodes: []tob.Node{
							{Type: tob.NODE_UNIT, Unit: tob.Unit{Id: "example1", Name: "Example Unit Name"}},
							{Type: tob.NODE_UNIT, Unit: tob.Unit{
								Id: "challenge1", Name: "Challenge Name",
								Description: []byte("## Challenge description\n\nbla bla bla"),
								Hints: [][]byte{
									[]byte("## Hint 1\n\napply yourself :)"),
									[]byte("## Hint 2\n\napply more"),
								},
							}},
						},
					},
					},
				}}}}, trees[1])
}
