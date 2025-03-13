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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"

	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/db/entity"
	"beam.apache.org/playground/backend/internal/fs_tool"
)

func Test_replace(t *testing.T) {
	codeWithPublicClass := "package org.apache.beam.sdk.transforms; \n public class Class {\n    public static void main(String[] args) {\n        System.out.println(\"Hello World!\");\n    }\n}"
	codeWithoutPublicClass := "package org.apache.beam.sdk.transforms; \n class Class {\n    public static void main(String[] args) {\n        System.out.println(\"Hello World!\");\n    }\n}"
	codeWithImportedPackage := "import org.apache.beam.sdk.transforms.*; \n class Class {\n    public static void main(String[] args) {\n        System.out.println(\"Hello World!\");\n    }\n}"

	path, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	lc, _ := fs_tool.NewLifeCycle(pb.Sdk_SDK_JAVA, uuid.New(), filepath.Join(path, "temp"))
	_ = lc.CreateFolders()
	defer os.RemoveAll(filepath.Join(path, "temp"))
	sources := []entity.FileEntity{{Name: "main.java", Content: codeWithPublicClass, IsMain: true}}
	_ = lc.CreateSourceCodeFiles(sources)

	type args struct {
		args []interface{}
	}
	tests := []struct {
		name     string
		args     args
		wantCode string
		wantErr  bool
	}{
		{
			name:    "File doesn't exist",
			args:    args{[]interface{}{"someFile.java", classWithPublicModifierPattern, classWithoutPublicModifierPattern}},
			wantErr: true,
		},
		{
			// Test that file with public class loses 'public' modifier
			name:     "File with public class",
			args:     args{[]interface{}{lc.Paths.AbsoluteSourceFilePath, classWithPublicModifierPattern, classWithoutPublicModifierPattern}},
			wantCode: codeWithoutPublicClass,
			wantErr:  false,
		},
		{
			// Test that file with defined package changes to import dependencies from this package
			name:     "File with package",
			args:     args{[]interface{}{lc.Paths.AbsoluteSourceFilePath, packagePattern, importStringPattern}},
			wantCode: codeWithImportedPackage,
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := replace(tt.args.args...); (err != nil) != tt.wantErr {
				t.Errorf("removePublicClassModifier() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				data, err := os.ReadFile(tt.args.args[0].(string))
				if err != nil {
					t.Errorf("removePublicClassModifier() unexpected error = %v", err)
				}
				if !strings.EqualFold(string(data), tt.wantCode) {
					t.Errorf("removePublicClassModifier() code = {%v}, wantCode {%v}", string(data), tt.wantCode)
				}
			}
		})
	}
}

func TestGetJavaPreparers(t *testing.T) {
	type args struct {
		filePath      string
		prepareParams map[string]string
		isUnitTest    bool
		isKata        bool
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "Test number of preparers for code",
			args: args{"MOCK_FILEPATH", make(map[string]string), false, false},
			want: 3,
		},
		{
			name: "Test number of preparers for unit test",
			args: args{"MOCK_FILEPATH", make(map[string]string), true, false},
			want: 2,
		},
		{
			name: "Test number of preparers for kata",
			args: args{"MOCK_FILEPATH", make(map[string]string), false, true},
			want: 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewPreparersBuilder(tt.args.filePath, tt.args.prepareParams)
			GetJavaPreparers(builder, tt.args.isUnitTest, tt.args.isKata)
			if got := builder.Build().GetPreparers(); len(*got) != tt.want {
				t.Errorf("GetJavaPreparation() returns %v Preparers, want %v", len(*got), tt.want)
			}
		})
	}
}

func Test_findPipelineObjectName(t *testing.T) {
	code := "package org.apache.beam.examples;\n\n/*\n * Licensed to the Apache Software Foundation (ASF) under one\n * or more contributor license agreements.  See the NOTICE file\n * distributed with this work for additional information\n * regarding copyright ownership.  The ASF licenses this file\n * to you under the Apache License, Version 2.0 (the\n * \"License\"); you may not use this file except in compliance\n * with the License.  You may obtain a copy of the License at\n *\n *     http://www.apache.org/licenses/LICENSE-2.0\n *\n * Unless required by applicable law or agreed to in writing, software\n * distributed under the License is distributed on an \"AS IS\" BASIS,\n * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n * See the License for the specific language governing permissions and\n * limitations under the License.\n */\n\n// beam-playground:\n//   name: Branching\n//   description: Task from katas to branch out the numbers to two different transforms, one transform\n//     is multiplying each number by 5 and the other transform is multiplying each number by 10.\n//   multifile: false\n//   categories:\n//     - Branching\n//     - Core Transforms\n\nimport static org.apache.beam.sdk.values.TypeDescriptors.integers;\n\nimport org.apache.beam.sdk.Pipeline;\nimport org.apache.beam.sdk.options.PipelineOptions;\nimport org.apache.beam.sdk.options.PipelineOptionsFactory;\nimport org.apache.beam.sdk.transforms.Create;\nimport org.apache.beam.sdk.transforms.MapElements;\nimport org.apache.beam.sdk.values.PCollection;\nimport org.apache.beam.sdk.util.construction.renderer.construction.PipelineDotRenderer;\n\npublic class Task {\n\n\n\n  public static void main(String[] args) {\n    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();\n    Pipeline pipeline = Pipeline.create(options);\n\n    PCollection<Integer> numbers =\n        pipeline.apply(Create.of(1, 2, 3, 4, 5));\n\n    PCollection<Integer> mult5Results = applyMultiply5Transform(numbers);\n    PCollection<Integer> mult10Results = applyMultiply10Transform(numbers);\n\n    mult5Results.apply(\"Log multiply 5\", Log.ofElements(\"Multiplied by 5: \"));\n    mult10Results.apply(\"Log multiply 10\", Log.ofElements(\"Multiplied by 10: \"));\n\n    String dotString = PipelineDotRenderer.toDotString(pipeline);\n    System.out.println(dotString);\n    pipeline.run();\n\n  }\n\n  static PCollection<Integer> applyMultiply5Transform(PCollection<Integer> input) {\n    return input.apply(\"Multiply by 5\", MapElements.into(integers()).via(num -> num * 5));\n  }\n\n  static PCollection<Integer> applyMultiply10Transform(PCollection<Integer> input) {\n    return input.apply(\"Multiply by 10\", MapElements.into(integers()).via(num -> num * 10));\n  }\n\n}\n"
	lc := createTempFileWithCode(code)
	codeWithoutPipeline := "package org.apache.beam.examples;\n\n/*\n * Licensed to the Apache Software Foundation (ASF) under one\n * or more contributor license agreements.  See the NOTICE file\n * distributed with this work for additional information\n * regarding copyright ownership.  The ASF licenses this file\n * to you under the Apache License, Version 2.0 (the\n * \"License\"); you may not use this file except in compliance\n * with the License.  You may obtain a copy of the License at\n *\n *     http://www.apache.org/licenses/LICENSE-2.0\n *\n * Unless required by applicable law or agreed to in writing, software\n * distributed under the License is distributed on an \"AS IS\" BASIS,\n * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n * See the License for the specific language governing permissions and\n * limitations under the License.\n */\n\n// beam-playground:\n//   name: Branching\n//   description: Task from katas to branch out the numbers to two different transforms, one transform\n//     is multiplying each number by 5 and the other transform is multiplying each number by 10.\n//   multifile: false\n//   categories:\n//     - Branching\n//     - Core Transforms\n\nimport static org.apache.beam.sdk.values.TypeDescriptors.integers;\n\nimport org.apache.beam.sdk.Pipeline;\nimport org.apache.beam.sdk.options.PipelineOptions;\nimport org.apache.beam.sdk.options.PipelineOptionsFactory;\nimport org.apache.beam.sdk.transforms.Create;\nimport org.apache.beam.sdk.transforms.MapElements;\nimport org.apache.beam.sdk.values.PCollection;\nimport org.apache.beam.sdk.util.construction.renderer.construction.PipelineDotRenderer;\n\npublic class Task {\n\n\n\n  public static void main(String[] args) {\n    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();\n     PCollection<Integer> numbers =\n        pipeline.apply(Create.of(1, 2, 3, 4, 5));\n\n    PCollection<Integer> mult5Results = applyMultiply5Transform(numbers);\n    PCollection<Integer> mult10Results = applyMultiply10Transform(numbers);\n\n    mult5Results.apply(\"Log multiply 5\", Log.ofElements(\"Multiplied by 5: \"));\n    mult10Results.apply(\"Log multiply 10\", Log.ofElements(\"Multiplied by 10: \"));\n\n    String dotString = PipelineDotRenderer.toDotString(pipeline);\n    System.out.println(dotString);\n    pipeline.run();\n\n  }\n\n  static PCollection<Integer> applyMultiply5Transform(PCollection<Integer> input) {\n    return input.apply(\"Multiply by 5\", MapElements.into(integers()).via(num -> num * 5));\n  }\n\n  static PCollection<Integer> applyMultiply10Transform(PCollection<Integer> input) {\n    return input.apply(\"Multiply by 10\", MapElements.into(integers()).via(num -> num * 10));\n  }\n\n}\n"
	lcWithoutPipeline := createTempFileWithCode(codeWithoutPipeline)
	path, _ := os.Getwd()
	defer os.RemoveAll(filepath.Join(path, "temp"))

	type args struct {
		filepath string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{name: "pipeline name found", args: args{filepath: lc.AbsoluteSourceFilePath}, wantErr: false, want: "pipeline"},
		{name: "pipeline name not found", args: args{filepath: lcWithoutPipeline.AbsoluteSourceFilePath}, wantErr: false, want: ""},
		{name: "file not found", args: args{filepath: "someFile"}, wantErr: true, want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := findPipelineObjectName(tt.args.filepath)
			if (err != nil) != tt.wantErr {
				t.Errorf("findPipelineObjectName() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("findPipelineObjectName() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func createTempFileWithCode(code string) fs_tool.LifeCyclePaths {
	path, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	lc, _ := fs_tool.NewLifeCycle(pb.Sdk_SDK_JAVA, uuid.New(), filepath.Join(path, "temp"))
	_ = lc.CreateFolders()

	sources := []entity.FileEntity{{Name: "main.java", Content: code, IsMain: true}}
	_ = lc.CreateSourceCodeFiles(sources)
	return lc.Paths
}
