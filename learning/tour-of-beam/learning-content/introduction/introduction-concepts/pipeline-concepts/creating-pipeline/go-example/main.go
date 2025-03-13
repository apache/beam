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
//   name: creating-pipeline
//   description: Creating pipeline example.
//   multifile: false
//   context_line: 39
//   categories:
//     - Quickstart
//   complexity: BASIC
//   tags:
//     - hellobeam

package main

import (
	"context"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"
)

func main() {
	ctx := context.Background()
	beam.Init()

	p, s := beam.NewPipelineWithRoot()

	hello := helloBeam(s)
	debug.Print(s, hello)

	err := beamx.Run(ctx, p)
	if err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}

func helloBeam(s beam.Scope) beam.PCollection {
	return beam.Create(s, "Hello Beam")
}
