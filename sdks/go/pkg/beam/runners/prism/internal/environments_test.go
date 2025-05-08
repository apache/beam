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

package internal

import (
	"testing"

	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/urns"
)

func TestSelectAnyOf(t *testing.T) {
	tests := []struct {
		name, want string
		wantTag    string
		envs       []*pipepb.Environment
	}{
		{name: "singleDefault", want: urns.EnvDefault, envs: []*pipepb.Environment{{Urn: urns.EnvDefault}}},
		{name: "singleDocker", want: urns.EnvDocker, envs: []*pipepb.Environment{{Urn: urns.EnvDocker}}},
		{name: "singleProcess", want: urns.EnvProcess, envs: []*pipepb.Environment{{Urn: urns.EnvProcess}}},
		{name: "singleExternal", want: urns.EnvExternal, envs: []*pipepb.Environment{{Urn: urns.EnvExternal}}},
		{name: "multiplePickExternal_1", want: urns.EnvExternal, envs: []*pipepb.Environment{{Urn: urns.EnvExternal}, {Urn: urns.EnvDocker}, {Urn: urns.EnvProcess}}},
		{name: "multiplePickExternal_2", want: urns.EnvExternal, envs: []*pipepb.Environment{{Urn: urns.EnvDocker}, {Urn: urns.EnvProcess}, {Urn: urns.EnvExternal}}},
		{name: "multiplePickProcess", want: urns.EnvProcess, envs: []*pipepb.Environment{{Urn: urns.EnvDocker}, {Urn: urns.EnvProcess}}},
		{name: "multiplePickDocker", want: urns.EnvDocker, envs: []*pipepb.Environment{{Urn: urns.EnvDefault}, {Urn: urns.EnvDocker}}},
		{name: "multiplePickFirstExternal", want: urns.EnvExternal, wantTag: "first", envs: []*pipepb.Environment{{Urn: urns.EnvExternal, Payload: []byte("first")}, {Urn: urns.EnvExternal, Payload: []byte("second")}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			selected := selectAnyOfEnv(&pipepb.AnyOfEnvironmentPayload{Environments: test.envs})
			if selected.GetUrn() != test.want {
				t.Errorf("expected %v, got %v", test.want, selected.GetUrn())
			}
			if got, want := string(selected.GetPayload()), test.wantTag; got != want {
				t.Errorf("expected payload with tag %v, got %v", want, got)
			}
		})
	}

}
