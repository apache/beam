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
//   name: text-io-gcs-write
//   description: TextIO GCS write example.
//   multifile: false
//   context_line: 30
//   categories:
//     - Quickstart
//   complexity: MEDIUM
//   tags:
//     - hellobeam


package main

import (
  "context"
  "github.com/apache/beam/sdks/v2/go/pkg/beam"
  _ "github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
  "github.com/apache/beam/sdks/v2/go/pkg/beam/log"
  "github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
  "regexp"
  "github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"
  _ "github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/gcs"
  _ "github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/local"
)
var wordRE = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)

var locationGCS = "gs://mybucket/myfile.txt"

func main() {
  beam.Init()

  p := beam.NewPipeline()
  s := p.Root()

  input := beam.Create(s, "Hello write from playground","First example")

  // It may be unsupported. Since gcs requires credentials
  // textio.Write(s, locationGCS, input)

  debug.Printf(s,"Recorded data:", input)

  err := beamx.Run(context.Background(), p)
    if err != nil {
        log.Exitf(context.Background(), "Failed to execute job: %v", err)
    }
}
