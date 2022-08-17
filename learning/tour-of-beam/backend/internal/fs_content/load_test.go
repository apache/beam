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
	assert.Equal(t, []tob.ContentTree{
		{Sdk: tob.SDK_JAVA, Modules: []tob.Module{
			{Id: "java#introduction", Name: "Introduction"},
		}},
		{Sdk: tob.SDK_PYTHON, Modules: []tob.Module{
			{Id: "python#module 1", Name: "Module One", Units: []tob.UnitContent{
				{Unit: tob.Unit{Id: "python#module 1#unit-challenge", Name: "Challenge Name"}, Complexity: "BASIC"},
				{Unit: tob.Unit{Id: "python#module 1#unit-example", Name: "Example Unit Name"}},
			}},
		}},
	}, trees)
}
