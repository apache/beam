package fileio

import (
	"encoding"
	"flag"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/xlang/fileio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"testing"
)

var expansionAddress string // Populate with expansion address labelled "fileio".

func checkFlags(t *testing.T) {
	if expansionAddress == "" {
		t.Skip("No DebeziumIo expansion address provided.")
	}
}

type primitiveTypesContaining struct {
	AString  string
	AnInt32  int
	AnInt64  int64
	AFloat32 float32
	AFloat64 float64
	ABoolean bool
}

func TestFileIO_PrimitiveTypesContaining(t *testing.T) {
	write := WritePipeline(expansionAddress, &fileio.WriteConfiguration{
		FilenamePrefix: "gs://acb654db-6269-43f6-92b9-07dbc9066baa/output",
		Format:         "json",
		NumShards:      1,
	}, []interface{}{
		&primitiveTypesContaining{
			AString:  "a",
			AnInt32:  1,
			AnInt64:  1001,
			AFloat32: 1.1,
			AFloat64: 1000.1,
			ABoolean: true,
		},
	}, []encoding.BinaryMarshaler{})
	ptest.RunAndValidate(t, write)
}

func TestMain(m *testing.M) {
	flag.Parse()
	beam.Init()

	expansionAddress = "localhost:8097"

	//services := integration.NewExpansionServices()
	//defer func() { services.Shutdown() }()
	//addr, err := services.GetAddr("debeziumio")
	//if err != nil {
	//	log.Printf("skipping missing expansion service: %v", err)
	//} else {
	//	expansionAddr = addr
	//}

	ptest.MainRet(m)
}
