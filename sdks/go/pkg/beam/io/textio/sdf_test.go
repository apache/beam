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

package textio

import (
	"context"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/direct"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
)

// TestReadSdf tests that readSdf successfully reads a test text file, and
// outputs the correct number of lines for it, even for an exceedingly long
// line.
func TestReadSdf(t *testing.T) {
	f := "../../../../data/textio_test.txt"
	p, s := beam.NewPipelineWithRoot()
	lines := ReadSdf(s, f)
	passert.Count(s, lines, "NumLines", 1)

	if _, err := beam.Run(context.Background(), "direct", p); err != nil {
		t.Fatalf("Failed to execute job: %v", err)
	}
}
