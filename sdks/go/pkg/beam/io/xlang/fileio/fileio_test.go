package fileio

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx/schema"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"reflect"
	"testing"
)

func TestWriteConfiguration_CrossLanguagePayload(t *testing.T) {
	configuration := WriteConfiguration{
		Format:         "json",
		FilenamePrefix: "gs://bucket/object",
	}
	ss, err := schema.FromType(reflect.TypeOf(WriteConfiguration{}).Elem())
	if err != nil {
		panic(err)
	}
	schemaTransformPayload := pipeline_v1.SchemaTransformPayload{
		Identifier:          identifier,
		ConfigurationSchema: ss,
		ConfigurationRow:    beam.CrossLanguagePayload(configuration),
	}
	if err := beam.CrossLanguagePayload(&schemaTransformPayload); err == nil {
		return
	}
	t.Error(err)
}
