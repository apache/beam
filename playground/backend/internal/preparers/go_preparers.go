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

package preparers

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const (
	goName  = "go"
	fmtArgs = "fmt"
	sep     = "."
)

// GoPreparersBuilder facet of PreparersBuilder
type GoPreparersBuilder struct {
	PreparersBuilder
}

// GoPreparers chains to type *PreparersBuilder and returns a *GoPreparersBuilder
func (builder *PreparersBuilder) GoPreparers() *GoPreparersBuilder {
	return &GoPreparersBuilder{*builder}
}

// WithCodeFormatter adds code formatter preparer
func (builder *GoPreparersBuilder) WithCodeFormatter() *GoPreparersBuilder {
	formatCodePreparer := Preparer{
		Prepare: formatCode,
		Args:    []interface{}{builder.filePath},
	}
	builder.AddPreparer(formatCodePreparer)
	return builder
}

// WithFileNameChanger adds preparer to change file name
func (builder *GoPreparersBuilder) WithFileNameChanger() *GoPreparersBuilder {
	changeTestFileName := Preparer{
		Prepare: changeGoTestFileName,
		Args:    []interface{}{builder.filePath},
	}
	builder.AddPreparer(changeTestFileName)
	return builder
}

// GetGoPreparers returns reparation methods that should be applied to Go code
func GetGoPreparers(builder *PreparersBuilder, isUnitTest bool) {
	builder.
		GoPreparers().
		WithCodeFormatter()
	if isUnitTest {
		builder.GoPreparers().WithFileNameChanger()
	}
}

// formatCode formats go code
func formatCode(args ...interface{}) error {
	filePath := args[0].(string)
	cmd := exec.Command(goName, fmtArgs, filepath.Base(filePath))
	cmd.Dir = filepath.Dir(filePath)
	stdout, err := cmd.CombinedOutput()
	if err != nil {
		return errors.New(string(stdout))
	}
	return nil
}

func changeGoTestFileName(args ...interface{}) error {
	filePath := args[0].(string)
	testFileName := fmt.Sprintf("%s_test.%s", strings.Split(filePath, sep)[0], goName)
	err := os.Rename(filePath, testFileName)
	if err != nil {
		return err
	}
	return nil
}
