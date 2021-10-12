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

package cache

import (
	"github.com/google/uuid"
	"reflect"
	"testing"
	"time"
)

func TestNewCache(t *testing.T) {
	type args struct {
		cacheType string
	}
	tests := []struct {
		name string
		args args
		want Cache
	}{
		{
			name: "New Cache",
			args: args{cacheType: "TEST_VALUE"},
			want: &LocalCache{
				cleanupInterval:     cleanupInterval,
				items:               make(map[uuid.UUID]map[SubKey]interface{}),
				pipelinesExpiration: make(map[uuid.UUID]time.Time),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewCache(tt.args.cacheType); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewCache() = %v, want %v", got, tt.want)
			}
		})
	}
}
