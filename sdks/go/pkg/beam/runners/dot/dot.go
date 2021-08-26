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
	"io/ioutil"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	dotlib "github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/dot"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
)

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

	edges, nodes, err := p.Build()
	if err != nil {
		return nil, errors.New("can't get data to render")
	}

	var buf bytes.Buffer
	if err := dotlib.Render(edges, nodes, &buf); err != nil {
		return nil, err
	}
	return nil, ioutil.WriteFile(*dotFile, buf.Bytes(), 0644)
}
