package fileio

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
)

const (
	inputRowTupleTag  = "input"
	outputRowTupleTag = "output"
	expansionUri      = "beam:schematransform:org.apache.beam:file_write:v1"
)

func Write(s beam.Scope, expansionAddress string, configuration *WriteConfiguration, input beam.PCollection) beam.PCollection {
	pl := beam.CrossLanguagePayload(configuration)
	namedInput := map[string]beam.PCollection{
		inputRowTupleTag: input,
	}
	outputTypes := map[string]typex.FullType{
		outputRowTupleTag: typex.New(reflectx.String),
	}
	output := beam.CrossLanguage(s.Scope(expansionUri), expansionUri, pl, expansionAddress, namedInput, outputTypes)
	return output[outputRowTupleTag]
}

// WriteConfiguration configures a struct-based DoFn that writes to a file or object system.
// WriteConfiguration is based on the FileWriteSchemaTransformConfiguration.
// See https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/fileschematransform/FileWriteSchemaTransformConfiguration.html
type WriteConfiguration struct {
	Compression          string
	FilenamePrefix       string
	FilenameSuffix       string
	Format               string
	NumShards            int
	ShardNameTemplate    string
	CsvConfiguration     *CsvWrite
	ParquetConfiguration *ParquetWrite
	XmlConfiguration     *XmlWrite
}

// CsvWrite configures details for writing CSV formatted data to a file or object system.
// CsvWrite is based on the FileWriteSchemaTransformConfiguration.CsvConfiguration.
// See https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/fileschematransform/FileWriteSchemaTransformConfiguration.CsvConfiguration.html
type CsvWrite struct {
}

type ParquetWrite struct{}

type XmlWrite struct{}
