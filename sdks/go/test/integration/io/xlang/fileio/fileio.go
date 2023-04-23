package fileio

import (
	"encoding"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	xfileio "github.com/apache/beam/sdks/v2/go/pkg/beam/io/xlang/fileio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"
)

func WritePipeline(expansionAddress string, configuration *xfileio.WriteConfiguration, input []interface{}, want []encoding.BinaryMarshaler) *beam.Pipeline {
	p, s := beam.NewPipelineWithRoot()
	inputCol := beam.Create(s.Scope("input"), input...)
	filesWritten := xfileio.Write(s, expansionAddress, configuration, inputCol)

	debug.Printf(s, "file written: %s", filesWritten)

	return p
}
