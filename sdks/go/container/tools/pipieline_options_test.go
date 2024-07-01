package tools

import (
	"os"
	"testing"
)

func TestMakePipelineOptionsFileAndEnvVar(t *testing.T) {
	MakePipelineOptionsFileAndEnvVar("")
	os.Remove("pipeline_options.json")
}
