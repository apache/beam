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

package batch

import (
	"testing"
	"time"
)

func TestParams_validate(t *testing.T) {
	cases := []struct {
		name    string
		p       Params
		wantErr bool
	}{
		{"zero_limits", Params{}, true},
		{"negative_size", Params{BatchSize: -1}, true},
		{"negative_bytes", Params{BatchSizeBytes: -1}, true},
		{"negative_duration", Params{BatchSize: 10, MaxBufferingDuration: -time.Second}, true},
		{"count_only", Params{BatchSize: 10}, false},
		{"bytes_only", Params{BatchSizeBytes: 1024}, false},
		{"both_and_duration", Params{BatchSize: 10, BatchSizeBytes: 1024, MaxBufferingDuration: time.Second}, false},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			err := c.p.validate()
			gotErr := err != nil
			if gotErr != c.wantErr {
				t.Errorf("validate() err = %v, wantErr = %v", err, c.wantErr)
			}
		})
	}
}
