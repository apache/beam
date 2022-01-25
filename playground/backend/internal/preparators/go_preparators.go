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

package preparators

import (
	"errors"
	"os/exec"
	"path/filepath"
)

const (
	nameBinGo = "go"
	fmtArgs   = "fmt"
)

// GetGoPreparators returns reparation methods that should be applied to Go code
func GetGoPreparators(filePath string) *[]Preparator {
	preparatorArgs := make([]interface{}, 1)
	preparatorArgs[0] = filePath
	formatCodePreparator := Preparator{Prepare: formatCode, Args: preparatorArgs}
	return &[]Preparator{formatCodePreparator}
}

// formatCode formats go code
func formatCode(args ...interface{}) error {
	filePath := args[0].(string)
	cmd := exec.Command(nameBinGo, fmtArgs, filepath.Base(filePath))
	cmd.Dir = filepath.Dir(filePath)
	stdout, err := cmd.CombinedOutput()
	if err != nil {
		return errors.New(string(stdout))
	}
	return nil
}
