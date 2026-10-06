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
	"fmt"
	"os"
	"strconv"
	"strings"

	structpb "google.golang.org/protobuf/types/known/structpb"
)

// MakePipelineOptionsFileAndEnvVar writes the pipeline options to a file.
// Assumes the options string is JSON formatted.
//
// Stores the file name in question in PIPELINE_OPTIONS_FILE for access by the SDK.
func MakePipelineOptionsFileAndEnvVar(options string) error {
	fn := "pipeline_options.json"
	f, err := os.Create(fn)
	if err != nil {
		return fmt.Errorf("unable to create %v: %w", fn, err)
	}
	defer f.Close()
	var js map[string]interface{}
	if json.Unmarshal([]byte(options), &js) != nil {
		return fmt.Errorf("options string is not JSON formatted %v", options)
	}
	if _, err := f.WriteString(options); err != nil {
		return fmt.Errorf("error writing %v: %w", f.Name(), err)
	}
	os.Setenv("PIPELINE_OPTIONS_FILE", f.Name())
	return nil
}

// PipelineOptions represents parsed pipeline options as a normalized map.
type PipelineOptions struct {
	options     map[string]any
	experiments map[string]string
}

// ParseOptionsFromProto creates normalized PipelineOptions directly from a protobuf Struct.
func ParseOptionsFromProto(opt *structpb.Struct, sdkNamespace string) *PipelineOptions {
	if opt == nil {
		return &PipelineOptions{options: make(map[string]any), experiments: make(map[string]string)}
	}
	raw := opt.AsMap()
	flat := make(map[string]any)

	// 1. Extract nested options if present (Dataflow runner uses this structure)
	if optsVal, ok := raw["options"]; ok {
		if optsMap, ok := optsVal.(map[string]any); ok {
			for k, v := range optsMap {
				flat[k] = v
			}
		}
	}

	// 2. Extract standard URN keys (Portable runners use this structure)
	for k, v := range raw {
		if k == "options" || k == "display_data" {
			continue
		}
		if strings.HasPrefix(k, "beam:option:") && strings.HasSuffix(k, ":v1") {
			name := strings.TrimPrefix(k, "beam:option:")
			name = strings.TrimSuffix(name, ":v1")
			flat[name] = v
		}
	}

	// 3. Promote specified SDK namespace options (Highest precedence, may overwrite earlier entries).
	// Beam Go SDK uses this structure.
	if sdkNamespace != "" {
		sdkURN := fmt.Sprintf("beam:option:%s:v1", sdkNamespace)
		if sdkVal, ok := raw[sdkURN]; ok {
			if urnMap, ok := sdkVal.(map[string]any); ok {
				if nestedOpts, ok := urnMap["options"].(map[string]any); ok {
					for nk, nv := range nestedOpts {
						flat[nk] = nv
					}
				}
			}
		}
	}

	po := &PipelineOptions{
		options:     flat,
		experiments: make(map[string]string),
	}
	if exps, err := po.GetStringSlice("experiments"); err == nil {
		po.experiments = parseExperiments(exps)
	}
	return po
}

func parseExperiments(slice []string) map[string]string {
	res := make(map[string]string)
	for _, item := range slice {
		if strings.Contains(item, "=") {
			parts := strings.SplitN(item, "=", 2)
			res[parts[0]] = parts[1]
		} else {
			res[item] = ""
		}
	}
	return res
}

// HasOption returns true if the option is defined and not nil.
func (po *PipelineOptions) HasOption(name string) bool {
	val, ok := po.options[name]
	return ok && val != nil
}

// GetString returns the value of an option as a string.
// As a convenience and to maintain compatibility with Go SDK's flags serialization style,
// if the option is stored as a string slice/array, GetString will conjoin the elements
// into a single comma-separated string (e.g. ["opt1", "opt2"] -> "opt1,opt2").
func (po *PipelineOptions) GetString(name string) (string, error) {
	val, ok := po.options[name]
	if !ok || val == nil {
		return "", fmt.Errorf("option %q not defined", name)
	}
	if str, ok := val.(string); ok {
		return str, nil
	}
	if slice, ok := val.([]any); ok {
		var parts []string
		for _, item := range slice {
			if str, ok := item.(string); ok {
				parts = append(parts, str)
			} else {
				return "", fmt.Errorf("option %q: expected string slice element, got type %T", name, item)
			}
		}
		return strings.Join(parts, ","), nil
	}
	return "", fmt.Errorf("option %q: expected string, got type %T", name, val)
}

// GetStringSlice returns the value of an option as a string slice.
// As a convenience and to maintain compatibility with Go SDK's flags serialization style,
// if the option is stored as a single comma-separated string (such as experiments
// or dataflow_service_options), GetStringSlice will parse it by splitting the string
// by comma (e.g. "opt1,opt2" -> ["opt1", "opt2"]).
func (po *PipelineOptions) GetStringSlice(name string) ([]string, error) {
	val, ok := po.options[name]
	if !ok || val == nil {
		return nil, fmt.Errorf("option %q not defined", name)
	}
	if slice, ok := val.([]any); ok {
		var res []string
		for _, item := range slice {
			if str, ok := item.(string); ok {
				res = append(res, str)
			} else {
				return nil, fmt.Errorf("option %q: expected string slice element, got type %T", name, item)
			}
		}
		return res, nil
	}
	if str, ok := val.(string); ok {
		// Go SDK models multi-value list flags (like experiments or dataflow_service_options)
		// as comma-separated string values.
		if str == "" {
			return nil, nil
		}
		return strings.Split(str, ","), nil
	}
	return nil, fmt.Errorf("option %q: expected string slice, got type %T", name, val)
}

// GetInt returns the value of an option as an integer.
func (po *PipelineOptions) GetInt(name string) (int, error) {
	val, ok := po.options[name]
	if !ok || val == nil {
		return 0, fmt.Errorf("option %q not defined", name)
	}
	switch v := val.(type) {
	case float64:
		return int(v), nil
	case string:
		res, err := strconv.Atoi(v)
		if err == nil {
			return res, nil
		}
		return 0, fmt.Errorf("option %q: failed to parse %q as int: %w", name, v, err)
	default:
		return 0, fmt.Errorf("option %q: expected int (represented as number or string), got type %T", name, val)
	}
}

// GetBool returns the value of an option as a boolean.
func (po *PipelineOptions) GetBool(name string) (bool, error) {
	val, ok := po.options[name]
	if !ok || val == nil {
		return false, fmt.Errorf("option %q not defined", name)
	}
	switch v := val.(type) {
	case bool:
		return v, nil
	case string:
		res, err := strconv.ParseBool(v)
		if err != nil {
			return false, fmt.Errorf("option %q: failed to parse %q as bool: %w", name, v, err)
		}
		return res, nil
	case float64:
		return v != 0, nil
	default:
		return false, fmt.Errorf("option %q: expected bool, got type %T", name, val)
	}
}

// GetFloat64 returns the value of an option as a float64.
func (po *PipelineOptions) GetFloat64(name string) (float64, error) {
	val, ok := po.options[name]
	if !ok || val == nil {
		return 0, fmt.Errorf("option %q not defined", name)
	}
	switch v := val.(type) {
	case float64:
		return v, nil
	case string:
		res, err := strconv.ParseFloat(v, 64)
		if err == nil {
			return res, nil
		}
		return 0, fmt.Errorf("option %q: failed to parse %q as float64: %w", name, v, err)
	default:
		return 0, fmt.Errorf("option %q: expected float64 (represented as number or string), got type %T", name, val)
	}
}

// LookupExperiment returns the value of an experiment option if present.
// - If the experiment is present but has no value (e.g., --experiments=foo), it returns "", true.
// - If the experiment is present as a key-value pair (e.g., --experiments=foo=bar), it returns "bar", true.
// - If the experiment is not present, it returns "", false.
func (po *PipelineOptions) LookupExperiment(key string) (string, bool) {
	val, ok := po.experiments[key]
	return val, ok
}

// HasExperiment returns true if the specified experiment is present in the options (either as a flag or key-value pair).
func (po *PipelineOptions) HasExperiment(name string) bool {
	_, ok := po.LookupExperiment(name)
	return ok
}
