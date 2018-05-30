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

// forest is an example that shows that pipeline construction is normal Go
// code -- the pipeline "forest" is created recursively and uses a global
// variable -- and that a pipeline may contain non-connected parts.
//
// The pipeline generated has the shape of a forest where the output of each
// singleton leaf is flattened together over several rounds. This is most
// clearly seen via a visual representation of the pipeline, such as the one
// produced by the 'dot' runner.
//
// Running the pipeline logs "1", "2", "3", etc for each leaf in the forest.
// Note that different runners may produces different or non-deterministic
// orders.
package main

import (
	"context"
	"flag"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/go/pkg/beam/x/debug"
)

var (
	n     = flag.Int("count", 2, "Number of trees")
	depth = flag.Int("depth", 3, "Depth of each tree")
)

func tree(s beam.Scope, depth int) beam.PCollection {
	if depth <= 0 {
		return leaf(s)
	}
	a := tree(s, depth-1)
	b := tree(s, depth-1)
	c := tree(s, depth-2)
	return beam.Flatten(s, a, b, c)
}

var count = 0

func leaf(s beam.Scope) beam.PCollection {
	count++
	return beam.Create(s, count) // singleton PCollection<int>
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()

	log.Info(ctx, "Running forest")

	// Build a forest of processing nodes with flatten "branches".
	p := beam.NewPipeline()
	s := p.Root()
	for i := 0; i < *n; i++ {
		t := tree(s, *depth)
		debug.Print(s, t)
	}

	if err := beamx.Run(ctx, p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
