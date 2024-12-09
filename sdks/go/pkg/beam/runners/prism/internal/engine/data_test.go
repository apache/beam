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

package engine

import (
	"encoding/binary"
	"math"
	"testing"
)

func TestCompareTimestampSuffixes(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		loI := int64(math.MinInt64)
		hiI := int64(math.MaxInt64)

		loB := binary.BigEndian.AppendUint64(nil, uint64(loI))
		hiB := binary.BigEndian.AppendUint64(nil, uint64(hiI))

		if compareTimestampSuffixes(loB, hiB) != (loI < hiI) {
			t.Errorf("lo vs Hi%v < %v: bytes %v vs %v, %v %v", loI, hiI, loB, hiB, loI < hiI, compareTimestampSuffixes(loB, hiB))
		}
	})
}
