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

type PipelineOptionsData struct {
	Options     LegacyOptionsData `json:"options"`
	Experiments []string          `json:"beam:option:experiments:v1"`
}

type LegacyOptionsData struct {
	Experiments []string `json:"experiments"`
}

// GetExperiments extracts a string array from the options string (in JSON format)
//
// The json string of pipeline options can be in two formats.
//
// Legacy format:
//
//	{
//		"display_data": [
//			{...},
//		],
//		"options": {
//			...
//			"experiments": [
//				...
//			],
//		}
//	}
//
// URN format:
//
//	{
//		"beam:option:experiments:v1": [
//			...
//		]
//	}
func GetExperiments(options string) []string {
	var opts PipelineOptionsData
	err := json.Unmarshal([]byte(options), &opts)
	if err != nil {
		return nil
	}

	// Check the legacy experiments first
	if len(opts.Options.Experiments) > 0 {
		return opts.Options.Experiments
	}

	if len(opts.Experiments) > 0 {
		return opts.Experiments
	}

	return nil
}
