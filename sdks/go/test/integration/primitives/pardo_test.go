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
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/v2/go/test/integration"
)

func TestParDoMultiOutput(t *testing.T) {
	integration.CheckFilters(t)
	ptest.RunAndValidate(t, ParDoMultiOutput())
}

func TestParDoSideInput(t *testing.T) {
	integration.CheckFilters(t)
	ptest.RunAndValidate(t, ParDoSideInput())
}

func TestParDoKVSideInput(t *testing.T) {
	integration.CheckFilters(t)
	ptest.RunAndValidate(t, ParDoKVSideInput())
}

func TestParDoMultiMapSideInput(t *testing.T) {
	integration.CheckFilters(t)
	ptest.RunAndValidate(t, ParDoMultiMapSideInput())
}

func TestParDoPipelineOptions(t *testing.T) {
	integration.CheckFilters(t)
	ptest.RunAndValidate(t, ParDoPipelineOptions())
}

func TestParDoBundleFinalizer(t *testing.T) {
	integration.CheckFilters(t)
	for _, tt := range []struct {
		name       string
		pipelineFn func() *beam.Pipeline
		want       int32
	}{
		{
			name:       "InProcessElement",
			pipelineFn: ParDoProcessElementBundleFinalizer,
			want:       BundleFinalizerProcess,
		},
		{
			name:       "InFinishBundle",
			pipelineFn: ParDoFinishBundleFinalizer,
			want:       BundleFinalizerFinish,
		},
		{
			name:       "InStartProcessFinishBundle",
			pipelineFn: ParDoFinalizerInAll,
			want:       BundleFinalizerStart + BundleFinalizerProcess + BundleFinalizerFinish,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			CountInvokeBundleFinalizer.Store(0)
			p := tt.pipelineFn()
			_, err := ptest.RunWithMetrics(p)
			if err != nil {
				t.Fatalf("Failed to execute job: %v", err)
			}
			if got := CountInvokeBundleFinalizer.Load(); got != tt.want {
				t.Errorf("BundleFinalization RegisterCallback not invoked as expected via proxy counts, got: %v, want: %v", got, tt.want)
			}
		})
	}
}
