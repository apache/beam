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

func TestPipelineOptionsAndExperiments(t *testing.T) {
	ctx := context.Background()
	if PipelineOptions(ctx) != nil {
		t.Errorf("empty context should have nil pipeline options")
	}
	if len(GetExperiments(PipelineOptions(ctx))) != 0 {
		t.Errorf("empty context should have no experiments")
	}

	options, _ := structpb.NewStruct(map[string]interface{}{
		"options": map[string]interface{}{
			"experiments": []interface{}{"exp1", "exp2"},
		},
	})
	ctx2 := WithPipelineOptions(ctx, options)
	if PipelineOptions(ctx2) == nil {
		t.Errorf("context should have pipeline options")
	}
	exps := GetExperiments(options)
	if len(exps) != 2 || exps[0] != "exp1" || exps[1] != "exp2" {
		t.Errorf("GetExperiments() = %v, want [exp1 exp2]", exps)
	}
	if !HasExperiment(options, "exp1") {
		t.Errorf("HasExperiment(exp1) = false, want true")
	}
	if HasExperiment(options, "exp3") {
		t.Errorf("HasExperiment(exp3) = true, want false")
	}

	// Test URN Style
	urnOptions, _ := structpb.NewStruct(map[string]interface{}{
		"beam:option:experiments:v1": []interface{}{"expA", "expB"},
	})
	ctx3 := WithPipelineOptions(ctx, urnOptions)
	if PipelineOptions(ctx3) == nil {
		t.Errorf("context should have pipeline options")
	}
	expsURN := GetExperiments(urnOptions)
	if len(expsURN) != 2 || expsURN[0] != "expA" || expsURN[1] != "expB" {
		t.Errorf("GetExperiments() = %v, want [expA expB]", expsURN)
	}
	if !HasExperiment(urnOptions, "expB") {
		t.Errorf("HasExperiment(expB) = false, want true")
	}
}
