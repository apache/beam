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
		{Sdk: tob.SDK_PYTHON, Modules: []tob.Module{
			{Id: "python#module 1", Name: "Module One", Complexity: "BASIC",
				Units: []tob.UnitContent{
					{Id: "python#module 1#unit-challenge", Name: "Challenge Name"},
					{Id: "python#module 1#unit-example", Name: "Example Unit Name"},
				},
			},
		}},
	}, trees)
}
