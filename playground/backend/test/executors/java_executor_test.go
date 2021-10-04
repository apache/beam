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

package executors

import (
	"beam.apache.org/playground/backend/pkg/executors"
	"beam.apache.org/playground/backend/pkg/fs_tool"
	"strconv"
	"testing"
)

var (
	javaExecutor *executors.JavaExecutor
	javaFS       *fs_tool.JavaFileSystemService
	fileName     string
)

const (
	javaCode   = "class HelloWorld {\n    public static void main(String[] args) {\n        System.out.println(\"Hello World!\");\n    }\n}"
	pipelineId = 1
)

func TestMain(m *testing.M) {
	err := setup()
	defer teardown()
	if err != nil {
		panic("Error in tests setup")
	}
	m.Run()
}

func setup() error {
	javaFS = &fs_tool.JavaFileSystemService{}
	err := javaFS.PrepareFolders()
	if err != nil {
		return err
	}
	javaExecutor = executors.NewJavaExecutor(javaFS)
	return nil
}

func teardown() {
	_, err := javaFS.DeletePreparedFolders()
	if err != nil {
		return
	}
}

func TestValidateJavaFile(t *testing.T) {
	_, _ = javaFS.CreateExecutableFile(javaCode, pipelineId)
	fileName = javaFS.GetJavaExecutableFileName(pipelineId)
	expected := true
	validated, err := javaExecutor.Validate(fileName)
	if expected != validated || err != nil {
		t.Fatalf(`TestValidateJavaFile: %q, %v doesn't match for %#q, nil`, strconv.FormatBool(validated), err,
			strconv.FormatBool(expected))
	}
}

func TestCompileJavaFile(t *testing.T) {
	fileName = javaFS.GetJavaExecutableFileName(pipelineId)
	err := javaExecutor.Compile(fileName)
	if err != nil {
		t.Fatalf("TestCompileJavaFile: Unexpexted error at compiliation: %s ", err.Error())
	}
}

func TestRunJavaFile(t *testing.T) {
	className := "HelloWorld"
	expected := "Hello World!\n"
	out, err := javaExecutor.Run(className)
	if expected != out || err != nil {
		t.Fatalf(`TestRunJavaFile: '%q, %v' doesn't match for '%#q', nil`, out, err, expected)
	}
}
