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
//   name: setting-pipeline
//   description: Setting pipeline example.
//   multifile: false
//   context_line: 34
//   categories:
//     - Quickstart

package main

import (
  "context"
  "flag"
  "github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
  "github.com/apache/beam/sdks/v2/go/pkg/beam"
  "github.com/apache/beam/sdks/v2/go/pkg/beam/log"
  "github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
  "github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"
)

var (
  // By default, this example reads from a public dataset containing the text of
  // King Lear. Set this option to choose a different input file or glob.
  input = flag.String("input", "gs://apache-beam-samples/shakespeare/kinglear.txt", "File(s) to read.")

  // Set this required option to specify where to write the output.
  output = flag.String("output", "", "Output file (required).")
)

func main() {
  // If beamx or Go flags are used, flags must be parsed first.
  flag.Parse()


  // Input validation is done as usual. Note that it must be after Init().
  if *output == "" {
    log.Fatal("No output provided")
  }

  p := beam.NewPipeline()
  s := p.Root()

  // Read from option input file
  lines := textio.Read(s, *input)

  // Write to option output file
  textio.Write(s, *output, formatted)

  if err := beamx.Run(context.Background(), p); err != nil {
    log.Fatalf("Failed to execute job: %v", err)
  }
}
