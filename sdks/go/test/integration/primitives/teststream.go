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
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/teststream"
)

// TestStreamSequence tests the TestStream primitive by inserting string elements
// then advancing the watermark past the point where they were inserted.
func TestStreamSingleSequence() *beam.Pipeline {
	p, s := beam.NewPipelineWithRoot()
	con := teststream.NewConfig()
	con.AddElements(100, "a", "b", "c")
	con.AdvanceWatermark(110)

	col := teststream.Create(s, con)

	passert.Count(s, col, "teststream strings", 3)

	return p
}

// TestStreamTwoSequences tests the TestStream primitive by inserting two sets of
// string elements that arrive on-time into the TestStream
func TestStreamTwoSequences() *beam.Pipeline {
	p, s := beam.NewPipelineWithRoot()
	con := teststream.NewConfig()
	con.AddElements(100, "a", "b", "c")
	con.AdvanceWatermark(110)
	con.AddElements(120, "d", "e", "f")
	con.AdvanceWatermark(130)

	col := teststream.Create(s, con)

	passert.Count(s, col, "teststream strings", 6)
	return p
}
