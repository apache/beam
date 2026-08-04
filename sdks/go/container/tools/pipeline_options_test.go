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

package tools

import (
	"encoding/json"
	"os"
	"testing"

	structpb "google.golang.org/protobuf/types/known/structpb"
)

func parseOptionsForTest(t *testing.T, options string) *PipelineOptions {
	if options == "" {
		options = "{}"
	}
	var raw map[string]interface{}
	if err := json.Unmarshal([]byte(options), &raw); err != nil {
		t.Fatalf("failed to unmarshal JSON for test: %v", err)
	}
	st, err := structpb.NewStruct(raw)
	if err != nil {
		t.Fatalf("failed to create structpb for test: %v", err)
	}
	po, err := ParseOptionsFromProto(st, "go_options")
	if err != nil {
		t.Fatalf("ParseOptionsFromProto failed: %v", err)
	}
	return po
}

func TestMakePipelineOptionsFileAndEnvVar(t *testing.T) {
	tests := []struct {
		name          string
		inputOptions  string
		expectedError string
	}{
		{
			"empty options",
			"{}",
			"",
		},
		{
			"valid options",
			"{\"abc\": 123}",
			"",
		},
		{
			"invalid options",
			"{4}",
			"options string is not JSON formatted {4}",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Cleanup(os.Clearenv)
			err := MakePipelineOptionsFileAndEnvVar(test.inputOptions)
			if err != nil {
				if got, want := err.Error(), test.expectedError; got != want {
					t.Errorf("got error: %v, want error: %v", got, want)
				}
			}
		})
	}
	os.Remove("pipeline_options.json")
}

func TestPipelineOptions(t *testing.T) {
	tests := []struct {
		name         string
		inputOptions string
		validate     func(t *testing.T, po *PipelineOptions)
	}{
		{
			"nested options",
			`{
				"options": {
					"profiler_agent": "memray",
					"profile_upload_interval_sec": 10,
					"profiler_stop_after_crash": true,
					"experiments": ["beam_fn_api", "use_build_isolation"]
				}
			}`,
			func(t *testing.T, po *PipelineOptions) {
				if got, err := po.GetString("profiler_agent"); err != nil || got != "memray" {
					t.Errorf("GetString(profiler_agent) = (%q, %v), want (\"memray\", nil)", got, err)
				}
				if got, err := po.GetInt("profile_upload_interval_sec"); err != nil || got != 10 {
					t.Errorf("GetInt(profile_upload_interval_sec) = (%d, %v), want (10, nil)", got, err)
				}
				if got, err := po.GetBool("profiler_stop_after_crash"); err != nil || got != true {
					t.Errorf("GetBool(profiler_stop_after_crash) = (%t, %v), want (true, nil)", got, err)
				}
				experiments, err := po.GetStringSlice("experiments")
				if err != nil || len(experiments) != 2 || experiments[0] != "beam_fn_api" || experiments[1] != "use_build_isolation" {
					t.Errorf("GetStringSlice(experiments) = (%v, %v), want ([beam_fn_api, use_build_isolation], nil)", experiments, err)
				}
			},
		},
		{
			"flat URN options with string values",
			`{
				"beam:option:profiler_agent:v1": "memray",
				"beam:option:profile_upload_interval_sec:v1": "10",
				"beam:option:profiler_stop_after_crash:v1": "true",
				"beam:option:experiments:v1": ["beam_fn_api"]
			}`,
			func(t *testing.T, po *PipelineOptions) {
				if got, err := po.GetString("profiler_agent"); err != nil || got != "memray" {
					t.Errorf("GetString(profiler_agent) = (%q, %v), want (\"memray\", nil)", got, err)
				}
				if got, err := po.GetInt("profile_upload_interval_sec"); err != nil || got != 10 {
					t.Errorf("GetInt(profile_upload_interval_sec) = (%d, %v), want (10, nil)", got, err)
				}
				if got, err := po.GetBool("profiler_stop_after_crash"); err != nil || got != true {
					t.Errorf("GetBool(profiler_stop_after_crash) = (%t, %v), want (true, nil)", got, err)
				}
				experiments, err := po.GetStringSlice("experiments")
				if err != nil || len(experiments) != 1 || experiments[0] != "beam_fn_api" {
					t.Errorf("GetStringSlice(experiments) = (%v, %v), want ([beam_fn_api], nil)", experiments, err)
				}
			},
		},
		{
			"comma-separated string options parsed as slice (Go SDK style)",
			`{
				"options": {
					"experiments": "exp1,exp2,exp3",
					"dataflow_service_options": "opt1"
				}
			}`,
			func(t *testing.T, po *PipelineOptions) {
				experiments, err := po.GetStringSlice("experiments")
				if err != nil || len(experiments) != 3 || experiments[0] != "exp1" || experiments[1] != "exp2" || experiments[2] != "exp3" {
					t.Errorf("GetStringSlice(experiments) = (%v, %v), want ([exp1, exp2, exp3], nil)", experiments, err)
				}
				if !po.HasExperiment("exp1") || !po.HasExperiment("exp2") || !po.HasExperiment("exp3") {
					t.Errorf("expected experiments exp1, exp2, and exp3 to be present, experiments map: %+v", po.experiments)
				}
				serviceOpts, err := po.GetStringSlice("dataflow_service_options")
				if err != nil || len(serviceOpts) != 1 || serviceOpts[0] != "opt1" {
					t.Errorf("GetStringSlice(dataflow_service_options) = (%v, %v), want ([opt1], nil)", serviceOpts, err)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			po := parseOptionsForTest(t, test.inputOptions)
			test.validate(t, po)
		})
	}
}

func TestPipelineOptions_Errors(t *testing.T) {
	t.Run("malformed integer", func(t *testing.T) {
		po := parseOptionsForTest(t, `{"options": {"profile_upload_interval_sec": "invalid"}}`)
		_, err := po.GetInt("profile_upload_interval_sec")
		if err == nil {
			t.Errorf("expected error, got nil")
		}
	})

	t.Run("malformed bool", func(t *testing.T) {
		po := parseOptionsForTest(t, `{"options": {"profiler_stop_after_crash": "maybe"}}`)
		_, err := po.GetBool("profiler_stop_after_crash")
		if err == nil {
			t.Errorf("expected error, got nil")
		}
	})

	t.Run("type mismatch int expected got bool", func(t *testing.T) {
		po := parseOptionsForTest(t, `{"options": {"profile_upload_interval_sec": true}}`)
		_, err := po.GetInt("profile_upload_interval_sec")
		if err == nil {
			t.Errorf("expected error, got nil")
		}
	})

	t.Run("missing key returns error", func(t *testing.T) {
		po := parseOptionsForTest(t, `{}`)
		_, errInt := po.GetInt("profile_upload_interval_sec")
		_, errBool := po.GetBool("profiler_stop_after_crash")
		if errInt == nil || errBool == nil {
			t.Errorf("expected error for missing keys, got: intErr=%v, boolErr=%v", errInt, errBool)
		}
	})

	t.Run("HasOption", func(t *testing.T) {
		po := parseOptionsForTest(t, `{"options": {"profile_upload_interval_sec": 10}}`)
		if !po.HasOption("profile_upload_interval_sec") {
			t.Errorf("HasOption(profile_upload_interval_sec) = false, want true")
		}
		if po.HasOption("profiler_stop_after_crash") {
			t.Errorf("HasOption(profiler_stop_after_crash) = true, want false")
		}
	})

	t.Run("HasExperiment", func(t *testing.T) {
		po := parseOptionsForTest(t, `{"options": {"experiments": ["exp1", "exp2=val2"]}}`)
		if !po.HasExperiment("exp1") {
			t.Errorf("HasExperiment(exp1) = false, want true")
		}
		if !po.HasExperiment("exp2") {
			t.Errorf("HasExperiment(exp2) = false, want true")
		}
		if po.HasExperiment("exp3") {
			t.Errorf("HasExperiment(exp3) = true, want false")
		}
	})

	t.Run("LookupExperiment", func(t *testing.T) {
		po := parseOptionsForTest(t, `{"options": {"experiments": ["exp1", "exp2=val2", "exp3=val3=val4"]}}`)
		
		val, ok := po.LookupExperiment("exp1")
		if !ok || val != "" {
			t.Errorf("LookupExperiment(exp1) = (%q, %t), want (\"\", true)", val, ok)
		}

		val, ok = po.LookupExperiment("exp2")
		if !ok || val != "val2" {
			t.Errorf("LookupExperiment(exp2) = (%q, %t), want (\"val2\", true)", val, ok)
		}

		val, ok = po.LookupExperiment("exp3")
		if !ok || val != "val3=val4" {
			t.Errorf("LookupExperiment(exp3) = (%q, %t), want (\"val3=val4\", true)", val, ok)
		}

		val, ok = po.LookupExperiment("exp4")
		if ok || val != "" {
			t.Errorf("LookupExperiment(exp4) = (%q, %t), want (\"\", false)", val, ok)
		}
	})



	t.Run("ParseOptionsFromProto", func(t *testing.T) {
		optionsStruct, err := structpb.NewStruct(map[string]interface{}{
			"options": map[string]interface{}{
				"profiler_agent": "memray",
			},
			"beam:option:experiments:v1": []interface{}{"expA", "expB"},
			"beam:option:go_options:v1": map[string]interface{}{
				"options": map[string]interface{}{
					"dataflow_service_options": "enable_google_cloud_profiler=custom_profiler",
				},
			},
		})
		if err != nil {
			t.Fatalf("failed to create proto Struct: %v", err)
		}

		po, err := ParseOptionsFromProto(optionsStruct, "go_options")
		if err != nil {
			t.Fatalf("ParseOptionsFromProto failed: %v", err)
		}
		if got, err := po.GetString("profiler_agent"); err != nil || got != "memray" {
			t.Errorf("GetString(profiler_agent) = (%q, %v), want (\"memray\", nil)", got, err)
		}
		if !po.HasExperiment("expA") || !po.HasExperiment("expB") {
			t.Errorf("expected experiments expA and expB to be present, options: %+v", po.options)
		}
		if got, err := po.GetString("dataflow_service_options"); err != nil || got != "enable_google_cloud_profiler=custom_profiler" {
			t.Errorf("GetString(dataflow_service_options) = (%q, %v), want (\"enable_google_cloud_profiler=custom_profiler\", nil)", got, err)
		}
		goOpts, ok := po.options["go_options"].(map[string]any)
		if !ok {
			t.Errorf("expected go_options map to be present, options: %+v", po.options)
		}
		nestedOpts, ok := goOpts["options"].(map[string]any)
		if !ok {
			t.Errorf("expected nested options map inside go_options, got: %+v", goOpts)
		}
		if got := nestedOpts["dataflow_service_options"]; got != "enable_google_cloud_profiler=custom_profiler" {
			t.Errorf("got dataflow_service_options = %v, want enable_google_cloud_profiler=custom_profiler", got)
		}
	})
}
