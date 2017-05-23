// Package dot is a Beam runner that "runs" a pipeline by producing a DOT
// graph of the execution plan.
package dot

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io/ioutil"

	"github.com/apache/beam/sdks/go/pkg/beam"
	dotlib "github.com/apache/beam/sdks/go/pkg/beam/util/dot"
)

// Code for making DOT graphs of the Graph data structure

var dotFile = flag.String("dot_file", "", "DOT output file to create")

// Execute produces a DOT representation of the pipeline.
func Execute(ctx context.Context, p *beam.Pipeline) error {
	if *dotFile == "" {
		return errors.New("must supply dot_file argument")
	}

	edges, nodes, err := p.Build()
	if err != nil {
		return errors.New("can't get data to render")
	}

	var buf bytes.Buffer
	if err := dotlib.Render(edges, nodes, &buf); err != nil {
		return err
	}
	if err := ioutil.WriteFile(*dotFile, buf.Bytes(), 0644); err != nil {
		return err
	}
	return nil
}
