package fileio

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx/schema"
	"reflect"
	"testing"
)

func TestWriteConfiguration_CrossLanguagePayload(t *testing.T) {
	configuration := WriteConfiguration{
		Format:         "json",
		FilenamePrefix: "gs://bucket/object",
	}
	ss, err := schema.FromType(reflect.TypeOf(WriteConfiguration{}))
	if err != nil {
		t.Fatal(err)
	}
	beam.CrossLanguage()
}
