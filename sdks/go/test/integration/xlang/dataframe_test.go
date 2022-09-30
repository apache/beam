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

package xlang

import (
	"flag"
	"log"
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/xlang/python/dataframe"
	"github.com/apache/beam/sdks/v2/go/test/integration"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*TestRow)(nil)).Elem())
}

type TestRow struct {
	A int64 `beam:"a"`
	B int32 `beam:"b"`
}

func TestDataframe(t *testing.T) {
	flag.Parse()
	beam.Init()

	services := integration.NewExpansionServices()
	defer func() { services.Shutdown() }()
	addr, err := services.GetAddr("python_transform")
	if err != nil {
		log.Printf("skipping missing expansion service: %v", err)
	} else {
		expansionAddr = addr
	}

	integration.CheckFilters(t)
	checkFlags(t)

	row0 := TestRow{A: int64(100), B: int32(1)}
	row1 := TestRow{A: int64(100), B: int32(2)}
	row2 := TestRow{A: int64(100), B: int32(3)}
	row3 := TestRow{A: int64(200), B: int32(4)}

	p, s := beam.NewPipelineWithRoot()

	input := beam.Create(s, row0, row1, row3)
	outCol := dataframe.Transform(s, "lambda df: df.groupby('a').sum()", input, reflect.TypeOf((*TestRow)(nil)).Elem(), dataframe.WithExpansionAddr(expansionAddr), dataframe.WithIndexes())

	passert.Equals(s, outCol, row2, row3)
	ptest.RunAndValidate(t, p)
}
