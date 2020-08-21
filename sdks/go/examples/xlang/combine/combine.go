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

package main

import (
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/passert"

	"context"
	"flag"
	"log"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"

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
func formatFn(w string, c int64) string {
	return fmt.Sprintf("%s:%v", w, c)
}

type KV struct {
	X string
	Y int64
}

func getKV(kv KV, emit func(string, int64)) {
	emit(kv.X, kv.Y)
}

func sumCounts(key string, iter func(*int64) bool) (string, int64) {
	var count, sum int64
	for iter(&count) {
		sum += count
	}
	return key, sum
}

func init() {
	beam.RegisterType(reflect.TypeOf((*KV)(nil)).Elem())
	beam.RegisterFunction(formatFn)
	beam.RegisterFunction(getKV)
	beam.RegisterFunction(sumCounts)

}

func main() {
	flag.Parse()
	beam.Init()

	if *expansionAddr == "" {
		log.Fatal("No expansion address provided")
	}

	p := beam.NewPipeline()
	s := p.Root()

	// Using Cross-language Count from Python's test expansion service

	x := KV{X: "a", Y: 1}
	y := KV{X: "a", Y: 2}
	z := KV{X: "b", Y: 3}
	l := beam.Create(s, x, y, z)
	ins := beam.ParDo(s, getKV, l)

	outputType := typex.NewKV(typex.New(reflectx.String), typex.New(reflectx.Int64))

	c := beam.CrossLanguageWithSingleInputOutput(s, "beam:transforms:xlang:test:compk", nil, *expansionAddr, ins, outputType)

	formatted := beam.ParDo(s, formatFn, c)

	passert.Equals(s, formatted, "a:3", "b:3")

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
