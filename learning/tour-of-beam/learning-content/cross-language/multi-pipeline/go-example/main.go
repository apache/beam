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

// beam-playground:
//   name: multi-pipeline
//   description: Multi pipeline example.
//   multifile: false
//   context_line: 34
//   categories:
//     - Quickstart
//   complexity: ADVANCED
//   tags:
//     - hellobeam

package main

import (
	"context"
	"flag"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/external"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
	"strings"
)

func main() {
	// Parse command line arguments.
	flag.Parse()
	beam.Init()

	// Define the pipeline.
	pipeline := beam.NewPipeline()
	scope := pipeline.Root()

	// Read from input text file.
	input := textio.Read(scope, "input.txt")

	// Apply an external Python transform.
	pythonTransformURN := "beam:transforms:python_transform:v1"
	expansionServiceURL := "localhost:12345"
	pythonTransformOutput := external.Expand(scope, pythonTransformURN, input, expansionServiceURL)

	// Continue with Go transforms.
	processData := beam.ParDo(scope, func(element string) string {
		return "Go: " + strings.ToUpper(element)
	}, pythonTransformOutput)

	// Write the results to an output text file.
	textio.Write(scope, "output.txt", processData)

	// Run the pipeline.
	ctx := context.Background()
	err := beamx.Run(ctx, pipeline)
	if err != nil {
		panic(err)
	}
}
