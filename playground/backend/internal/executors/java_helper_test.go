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
	pb "beam.apache.org/playground/backend/internal/api"
	"beam.apache.org/playground/backend/internal/fs_tool"
	"github.com/google/uuid"
	"testing"
)

var (
	javaExecutor *Executor
	pipelineId   = uuid.New()
)

const (
	javaCode = "class HelloWorld {\n    public static void main(String[] args) {\n        System.out.println(\"Hello World!\");\n    }\n}"
)

func TestMain(m *testing.M) {
	javaFS := setup()
	defer teardown(javaFS)
	m.Run()
}

func setup() *fs_tool.LifeCycle {
	javaFS, _ := fs_tool.NewLifeCycle(pb.Sdk_SDK_JAVA, pipelineId)
	_ = javaFS.CreateFolders()
	_, _ = javaFS.CreateExecutableFile(javaCode)
	javaExecutor = NewJavaExecutor(javaFS, GetJavaValidators())
	return javaFS
}

func teardown(javaFS *fs_tool.LifeCycle) {
	err := javaFS.DeleteFolders()
	if err != nil {
		return
	}
}

func TestValidateJavaFile(t *testing.T) {
	err := javaExecutor.Validate()
	if err != nil {
		t.Fatalf(`TestValidateJavaFile error: %v `, err)
	}
}

func TestCompileJavaFile(t *testing.T) {
	err := javaExecutor.Compile()
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
