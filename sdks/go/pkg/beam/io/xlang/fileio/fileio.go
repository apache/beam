package fileio

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx/schema"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"os"
	"reflect"
)

const (
	inputRowTupleTag  = "input"
	outputRowTupleTag = "output"
	//expansionUri      = "beam:transform:org.apache.beam:file_write:v1"
	identifier   = "beam:schematransform:org.apache.beam:file_write:v1"
	expansionUri = "beam:expansion:payload:schematransform:v1"
)

func Write(s beam.Scope, expansionAddress string, configuration WriteConfiguration, input beam.PCollection) beam.PCollection {
	ss, err := schema.FromType(reflect.TypeOf(WriteConfiguration{}))
	if err != nil {
		panic(err)
	}
	schemaTransformPayload := &pipeline_v1.SchemaTransformPayload{
		Identifier:          identifier,
		ConfigurationSchema: ss,
		ConfigurationRow:    beam.CrossLanguagePayload(configuration),
	}
	_ = beam.CrossLanguagePayload(schemaTransformPayload)

	os.Exit(0)
	//
	//namedInput := map[string]beam.PCollection{
	//	inputRowTupleTag: input,
	//}
	//
	//outputTypes := map[string]typex.FullType{
	//	outputRowTupleTag: typex.New(reflectx.String),
	//}
	//
	//output := beam.CrossLanguage(s.Scope(expansionUri), expansionUri, pl, expansionAddress, namedInput, outputTypes)
	//return output[outputRowTupleTag]
	return beam.Create(s, "hi")
}

// WriteConfiguration configures a struct-based DoFn that writes to a file or object system.
// WriteConfiguration is based on the FileWriteSchemaTransformConfiguration.
// See https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/fileschematransform/FileWriteSchemaTransformConfiguration.html
type WriteConfiguration struct {
	Format               string        `beam:"format"`
	FilenamePrefix       string        `beam:"filenamePrefix"`
	Compression          string        `beam:"compression"`
	NumShards            int32         `beam:"numShards"`
	ShardNameTemplate    string        `beam:"shardNameTemplate"`
	FilenameSuffix       string        `beam:"filenameSuffix"`
	CsvConfiguration     *CsvWrite     `beam:"csvConfiguration"`
	ParquetConfiguration *ParquetWrite `beam:"parquetConfiguration"`
	XmlConfiguration     *XmlWrite     `beam:"xmlConfiguration"`
}

// CsvWrite configures details for writing CSV formatted data to a file or object system.
// CsvWrite is based on the FileWriteSchemaTransformConfiguration.CsvConfiguration.
// See https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/fileschematransform/FileWriteSchemaTransformConfiguration.CsvConfiguration.html
type CsvWrite struct {
	PredefinedCsvFormat string `beam:"predefinedCsvFormat"`
}

type ParquetWrite struct {
	CompressionCodecName string `beam:"compressionCodecName"`
	RowGroupSize         int32  `beam:"rowGroupSize"`
}

type XmlWrite struct {
	RootElement string `beam:"rootElement"`
	Charset     string `beam:"charset"`
}
