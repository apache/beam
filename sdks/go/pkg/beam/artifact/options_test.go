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

package artifact

import (
	"context"
	"testing"

	structpb "google.golang.org/protobuf/types/known/structpb"
)

func TestPipelineOptionsContext(t *testing.T) {
	ctx := context.Background()
	if PipelineOptions(ctx) != nil {
		t.Errorf("empty context should have nil pipeline options")
	}

	options, _ := structpb.NewStruct(map[string]interface{}{
		"options": map[string]interface{}{
			"experiments": []interface{}{"exp1"},
		},
	})
	ctxWithOpts := WithPipelineOptions(ctx, options)
	got := PipelineOptions(ctxWithOpts)
	if got == nil {
		t.Errorf("context should have pipeline options")
	}
}

func TestGetExperiments_Nil(t *testing.T) {
	if got := GetExperiments(nil); got != nil {
		t.Errorf("GetExperiments(nil) = %v, want nil", got)
	}
}

func TestGetExperiments_Legacy(t *testing.T) {
	options, _ := structpb.NewStruct(map[string]interface{}{
		"options": map[string]interface{}{
			"experiments": []interface{}{"exp1", "exp2"},
		},
	})
	exps := GetExperiments(options)
	if len(exps) != 2 || exps[0] != "exp1" || exps[1] != "exp2" {
		t.Errorf("GetExperiments() = %v, want [exp1 exp2]", exps)
	}
}

func TestGetExperiments_URN(t *testing.T) {
	urnOptions, _ := structpb.NewStruct(map[string]interface{}{
		"beam:option:experiments:v1": []interface{}{"expA", "expB"},
	})
	expsURN := GetExperiments(urnOptions)
	if len(expsURN) != 2 || expsURN[0] != "expA" || expsURN[1] != "expB" {
		t.Errorf("GetExperiments() = %v, want [expA expB]", expsURN)
	}
}

func TestHasExperiment(t *testing.T) {
	options, _ := structpb.NewStruct(map[string]interface{}{
		"options": map[string]interface{}{
			"experiments": []interface{}{"exp1", "exp2"},
		},
	})

	if !HasExperiment(options, "exp1") {
		t.Errorf("HasExperiment(exp1) = false, want true")
	}
	if HasExperiment(options, "exp3") {
		t.Errorf("HasExperiment(exp3) = true, want false")
	}
}
