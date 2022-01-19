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

package primitives

import (
	"context"
	"fmt"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

func init() {
	beam.RegisterFunction(IncrementFn)
}

func IncrementFn(w beam.Window, value int, emit func(int)) {
	inv := (w.(window.IntervalWindow))
	fmt.Print(inv)
	emit(value + 1)
}

func main() {
	beam.Init()

	p := beam.NewPipeline()
	s := p.Root()
	input := beam.CreateList(s, []int{10, 20, 30})
	beam.ParDo(s, IncrementFn, input)

	if err := beamx.Run(context.Background(), p); err != nil {
		fmt.Print(err)
	}
}
