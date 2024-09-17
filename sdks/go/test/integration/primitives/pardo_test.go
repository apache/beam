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
	"fmt"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
	"sync/atomic"
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

var countInvokeBundleFinalizer atomic.Int32

func init() {
	beam.RegisterFunction(incrInvokeBF)
	beam.RegisterFunction(retError)
}

func TestParDoBundleFinalizer(t *testing.T) {
	integration.CheckFilters(t)
	for _, tt := range []struct {
		name       string
		fn         func() error
		pipelineFn func(beam.EncodedFunc) *beam.Pipeline
		want       int32
		wantErr    bool
	}{
		{
			name:       "InProcessElement",
			fn:         incrInvokeBF,
			pipelineFn: ParDoProcessElementBundleFinalizer,
			want:       1,
		},
		{
			name:       "InProcessElement_withErr",
			fn:         retError,
			pipelineFn: ParDoProcessElementBundleFinalizer,
			wantErr:    true,
		},
		{
			name:       "InFinishBundle",
			fn:         incrInvokeBF,
			pipelineFn: ParDoFinishBundleFinalizer,
			want:       1,
		},
		{
			name:       "InFinishBundle_withError",
			fn:         retError,
			pipelineFn: ParDoFinishBundleFinalizer,
			wantErr:    true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			countInvokeBundleFinalizer.Store(0)
			enc := beam.EncodedFunc{
				Fn: reflectx.MakeFunc(tt.fn),
			}
			p := tt.pipelineFn(enc)
			_, err := ptest.RunWithMetrics(p)
			if err == nil && tt.wantErr {
				t.Errorf("error nil from pipeline Job, wantErr: %v", tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if err != nil {
				t.Fatalf("Failed to execute job: %v", err)
			}
			if got := countInvokeBundleFinalizer.Load(); got != tt.want {
				t.Errorf("BundleFinalization RegisterCallback not invoked as expected via proxy counts, got: %v, want: %v", got, tt.want)
			}
		})
	}
}

func TestParDoBundleFinalizerInAll(t *testing.T) {
	t.Skip()
	var want int32 = 7
	countInvokeBundleFinalizer.Store(0)
	startFn := func() {
		countInvokeBundleFinalizer.Add(1)
	}
	startEnc := beam.EncodedFunc{Fn: reflectx.MakeFunc(startFn)}
	processFn := func() {
		countInvokeBundleFinalizer.Add(2)
	}
	processEnc := beam.EncodedFunc{Fn: reflectx.MakeFunc(processFn)}
	finishFn := func() {
		countInvokeBundleFinalizer.Add(4)
	}
	finishEnc := beam.EncodedFunc{Fn: reflectx.MakeFunc(finishFn)}

	p := ParDoFinalizerInAll(startEnc, processEnc, finishEnc)
	ptest.RunAndValidate(t, p)

	if got := countInvokeBundleFinalizer.Load(); got != want {
		t.Errorf("BundleFinalization RegisterCallback not invoked as expected via proxy counts, got: %v, want: %v", got, want)
	}
}

func incrInvokeBF() error {
	countInvokeBundleFinalizer.Add(1)
	return nil
}

func retError() error {
	return fmt.Errorf("error")
}
