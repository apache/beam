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

// group_by exemplifies using a cross-language group by key transform from a test expansion service.
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
	"fmt"
	"log"
	"reflect"
	"sort"

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

// formatFn is a DoFn that formats a word and its count as a string.
func formatFn(w string, c []int) string {
	sort.Ints(c)
	return fmt.Sprintf("%v:%v", w, c)
}

// KV used to represent KV PCollection values
type KV struct {
	X string
	Y int64
}

func getKV(kv KV, emit func(string, int64)) {
	emit(kv.X, kv.Y)
}

func collectValues(key string, iter func(*int64) bool) (string, []int) {
	var count int64
	var values []int
	for iter(&count) {
		values = append(values, int(count))
	}
	return key, values
}

func init() {
	beam.RegisterType(reflect.TypeOf((*KV)(nil)).Elem())
	beam.RegisterFunction(formatFn)
	beam.RegisterFunction(getKV)
	beam.RegisterFunction(collectValues)
}

func main() {
	flag.Parse()
	beam.Init()

	if *expansionAddr == "" {
		log.Fatal("No expansion address provided")
	}

	p := beam.NewPipeline()
	s := p.Root()

	// Using the cross-language transform
	kvs := beam.Create(s, KV{X: "0", Y: 1}, KV{X: "0", Y: 2}, KV{X: "1", Y: 3})
	ins := beam.ParDo(s, getKV, kvs)
	outputType := typex.NewCoGBK(typex.New(reflectx.String), typex.New(reflectx.Int64))
	outs := beam.CrossLanguageWithSingleInputOutput(s, "beam:transforms:xlang:test:gbk", nil, *expansionAddr, ins, outputType)

	vals := beam.ParDo(s, collectValues, outs)
	formatted := beam.ParDo(s, formatFn, vals)
	passert.Equals(s, formatted, "0:[1 2]", "1:[3]")

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
