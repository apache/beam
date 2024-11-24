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
