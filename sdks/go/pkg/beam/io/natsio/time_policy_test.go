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

package natsio

import (
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
)

func Test_timePolicy_TimestampFn(t *testing.T) {
	t.Run("processingTime", func(t *testing.T) {
		pubTime := time.Date(2020, 1, 2, 3, 4, 5, 6e6, time.UTC)

		t1 := mtime.Now()
		got := processingTimePolicy.TimestampFn()(pubTime)
		t2 := mtime.Now()

		if got < t1 || got > t2 {
			t.Errorf("timestamp = %v, want between %v and %v", got, t1, t2)
		}
	})

	t.Run("publishingTime", func(t *testing.T) {
		pubTime := time.Date(2020, 1, 2, 3, 4, 5, 6e6, time.UTC)

		if got, want := publishingTimePolicy.TimestampFn()(pubTime), mtime.FromTime(pubTime); got != want {
			t.Errorf("timestamp = %v, want %v", got, want)
		}
	})
}
