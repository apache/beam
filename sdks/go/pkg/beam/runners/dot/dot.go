// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package dot is a Beam runner that "runs" a pipeline by producing a DOT
// graph of the execution plan.
package dot

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"sort"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/pipelinex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
)

var errNoComponents = errors.New("pipeline has no components")

func init() {
	beam.RegisterRunner("dot", Execute)
}

// Code for making DOT graphs of the Graph data structure

var dotFile = flag.String("dot_file", "", "DOT output file to create")

// Execute produces a DOT representation of the pipeline.
func Execute(ctx context.Context, p *beam.Pipeline) (beam.PipelineResult, error) {
	if *dotFile == "" {
		return nil, errors.New("must supply dot_file argument")
	}

	edges, _, err := p.Build()
	if err != nil {
		return nil, errors.New("can't get data to render")
	}

	pipeline, err := graphx.Marshal(edges, &graphx.Options{})
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	buf.WriteString("digraph G {\n")

	components := pipeline.GetComponents()
	if components == nil {
		return nil, errNoComponents
	}

	transforms := components.GetTransforms()

	// Build reverse input index: PCollectionID -> []TransformID
	consumers := make(map[string][]string)
	for tid, t := range transforms {
		// Skip composite transforms
		if len(t.GetSubtransforms()) != 0 {
			continue
		}

		for _, pcollID := range t.GetInputs() {
			consumers[pcollID] = append(consumers[pcollID], tid)
		}
	}

	// Ensure deterministic ordering of consumer lists
	for pcollID := range consumers {
		sort.Strings(consumers[pcollID])
	}

	// Topologically sort transforms for deterministic emission.
	// We use the same ordering utility as Prism to ensure stable and execution-consistent graph traversal.
	roots := pipeline.GetRootTransformIds()
	ordered := pipelinex.TopologicalSort(transforms, roots)

	// Generate edges
	for _, tid := range ordered {
		t := transforms[tid]
		// Skip composite transforms
		if len(t.GetSubtransforms()) != 0 {
			continue
		}

		from := t.GetUniqueName()

		for _, pcollID := range t.GetOutputs() {
			for _, consumerID := range consumers[pcollID] {

				consumer, ok := transforms[consumerID]
				if !ok {
					continue // Defensively skip if the consumer transform is missing
				}

				to := consumer.GetUniqueName()
				fmt.Fprintf(&buf, "\"%s\" -> \"%s\";\n", from, to)
			}
		}
	}

	buf.WriteString("}\n")

	return nil, os.WriteFile(*dotFile, buf.Bytes(), 0644)
}
