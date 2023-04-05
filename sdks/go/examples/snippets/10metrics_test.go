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

package snippets

import (
	"context"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
)

func TestMetricsPipeline(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	input := beam.Create(s, 1, 2, 3, 4, 5, 6, 7)
	addMetricDoFnToPipeline(s, input)
	ms, err := executePipelineAndGetMetrics(context.Background(), p)
	if err != nil {
		t.Errorf("error executing pipeline: %v", err)
	}
	if got, want := len(ms.Counters()), 1; got != want {
		t.Errorf("got %v counters, want %v: %+v", got, want, ms.Counters())
	}
	c := ms.Counters()[0]
	if got, want := c.Committed, int64(7); got != want {
		t.Errorf("Attempted Counter: got %v, want %v: %+v", got, want, c)
	}
}
