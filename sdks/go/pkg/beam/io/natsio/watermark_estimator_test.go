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
)

func Test_watermarkEstimator_CurrentWatermark(t *testing.T) {
	ms := int64(1577934245000)
	we := &watermarkEstimator{
		state: ms,
	}
	if got, want := we.CurrentWatermark(), time.UnixMilli(ms); got != want {
		t.Errorf("CurrentWatermark() = %v, want %v", got, want)
	}
}

func Test_watermarkEstimator_ObserveTimestamp(t *testing.T) {
	t1 := time.Date(2020, 1, 2, 3, 4, 5, 6e6, time.UTC)
	t2 := time.Date(2020, 1, 2, 3, 4, 5, 7e6, time.UTC)

	tests := []struct {
		name  string
		state int64
		t     time.Time
		want  int64
	}{
		{
			name:  "Update watermark when the time is greater than the current state",
			state: t1.UnixMilli(),
			t:     t2,
			want:  t2.UnixMilli(),
		},
		{
			name:  "Keep existing watermark when the time is not greater than the current state",
			state: t2.UnixMilli(),
			t:     t1,
			want:  t2.UnixMilli(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			we := &watermarkEstimator{
				state: tt.state,
			}
			we.ObserveTimestamp(tt.t)
			if got, want := we.state, tt.want; got != want {
				t.Errorf("state = %v, want %v", got, want)
			}
		})
	}
}
