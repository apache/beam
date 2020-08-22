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

// multi exemplifies using a cross-language transform with multiple inputs and
// outputs from a test expansion service.
//
// Prerequisites to run wordcount:
// –> [Required] Job needs to be submitted to a portable runner (--runner=universal)
// –> [Required] Endpoint of job service needs to be passed (--endpoint=<ip:port>)
// –> [Required] Endpoint of expansion service needs to be passed (--expansion_addr=<ip:port>)
// –> [Optional] Environment type can be LOOPBACK. Defaults to DOCKER. (--environment_type=LOOPBACK|DOCKER)
package main

import (
	"context"
	"flag"
	"log"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"

	// Imports to enable correct filesystem access and runner setup in LOOPBACK mode
	_ "github.com/apache/beam/sdks/go/pkg/beam/io/filesystem/gcs"
	_ "github.com/apache/beam/sdks/go/pkg/beam/io/filesystem/local"
	_ "github.com/apache/beam/sdks/go/pkg/beam/runners/universal"
)

var (
	expansionAddr = flag.String("expansion_addr", "", "Address of Expansion Service")
)

func main() {
	flag.Parse()
	beam.Init()

	if *expansionAddr == "" {
		log.Fatal("No expansion address provided")
	}

	p := beam.NewPipeline()
	s := p.Root()

	main1 := beam.CreateList(s, []string{"a", "bb"})
	main2 := beam.CreateList(s, []string{"x", "yy", "zzz"})
	side := beam.CreateList(s, []string{"s"})
	namedInputs := map[string]beam.PCollection{"main1": main1, "main2": main2, "side": side}

	// Using the cross-language transform
	outputType := typex.New(reflectx.String)
	namedOutputs := map[string]typex.FullType{"main": outputType, "side": outputType}
	multi := beam.CrossLanguage(s, "beam:transforms:xlang:test:multi", nil, *expansionAddr, namedInputs, namedOutputs)

	passert.Equals(s, multi["main"], "as", "bbs", "xs", "yys", "zzzs")
	passert.Equals(s, multi["side"], "ss")

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
