package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	fileiox "github.com/apache/beam/sdks/v2/go/pkg/beam/io/xlang/fileio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"
)

var (
	expansionAddress = "localhost:8080"
)

type someDataModel struct {
	AString  string
	AnInt32  int
	AnInt64  int64
	AFloat32 float32
	AFloat64 float64
	ABoolean bool
}

func main() {
	flag.Parse()
	beam.Init()
	ctx := context.Background()
	if err := run(ctx); err != nil {
		log.Fatal(ctx, err)
	}
}

func run(ctx context.Context) error {
	elements := []interface{}{
		&someDataModel{
			AString:  "a",
			AnInt32:  1,
			AnInt64:  100000001,
			AFloat32: 1.01,
			AFloat64: 1.0000001,
			ABoolean: true,
		},
	}
	p, s := beam.NewPipelineWithRoot()

	input := beam.Create(s.Scope("input"), elements...)

	filePathsWritten := fileiox.Write(s, expansionAddress, &fileiox.WriteConfiguration{
		FilenamePrefix: "gs://acb654db-6269-43f6-92b9-07dbc9066baa/output",
		Format:         "json",
		NumShards:      1,
	}, input)

	debug.Print(s, filePathsWritten)

	if err := beamx.Run(ctx, p); err != nil {
		return fmt.Errorf("fatal error executing job: %w", err)
	}
	return nil
}
