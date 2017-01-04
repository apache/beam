package main

// See: https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/cookbook/DeDupExample.java

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"beam"
	"count"
	"dataflow"
	"textio"
)

// Options used purely at pipeline construction-time can just be flags.
var (
	input  = flag.String("input", "gs://apache-beam-samples/shakespeare/*", "Files to read.")
	output = flag.String("output", "gs://foo/dedup", "Prefix of output.")
)

func main() {
	p := &beam.Pipeline{}

	*output = os.ExpandEnv(*output)

	// (1) build pipeline, using assert-style error handling.

	lines := must(textio.Read(p, *input))
	dedupped := must(count.Dedup(p, lines))
	must(nil, textio.Write(p, *output, dedupped))

	// (2) execute it on Dataflow

	if err := dataflow.Execute(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
	log.Print("Success!")
}

// TODO: these kinds of helpers could be in beam.

func must(col *beam.PCollection, err error) *beam.PCollection {
	if err != nil {
		panic(fmt.Sprintf("Failed to constuct pipeline: %v", err))
	}
	return col
}
