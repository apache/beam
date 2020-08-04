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

package passert

import (
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/ptest"
)

func TestCount_Good(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	col := beam.Create(s, "a", "b", "c", "d", "e")
	count := 5

	Count(s, col, "TestCount_Good", count)
	if err := ptest.Run(p); err != nil {
		t.Errorf("Pipeline failed: %v", err)
	}
}

func TestCount_Bad(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	col := beam.Create(s, "a", "b", "c", "d", "e")
	count := 10

	Count(s, col, "TestCount_Bad", count)
	if err := ptest.Run(p); err == nil {
		t.Errorf("pipeline SUCCEEDED but should have failed")
	}
}
