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

package validators

import (
	"testing"
)

const (
	javaUnitTestFilePath = "unitTestCode.java"
	javaKataFilePath     = "kataCode.java"
	javaCodePath         = "code.java"
	javaUnitTestCode     = "@RunWith(JUnit4.class)\npublic class DeduplicateTest {\n\n  @Rule public TestPipeline p = TestPipeline.create();\n\n  @Test\n  @Category({NeedsRunner.class, UsesTestStream.class})\n  public void testInDifferentWindows() {}}"
	javaKataCode         = "package org.apache.beam.learning.katas.commontransforms.aggregation.max;\n\nimport org.apache.beam.learning.katas.util.Log;\nimport org.apache.beam.sdk.Pipeline;\nimport org.apache.beam.sdk.options.PipelineOptions;\nimport org.apache.beam.sdk.options.PipelineOptionsFactory;\nimport org.apache.beam.sdk.transforms.Create;\nimport org.apache.beam.sdk.transforms.Max;\nimport org.apache.beam.sdk.values.PCollection;\n\npublic class Task {\n\n  public static void main(String[] args) {\n    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();\n    Pipeline pipeline = Pipeline.create(options);\n\n    PCollection<Integer> numbers = pipeline.apply(Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));\n\n    PCollection<Integer> output = applyTransform(numbers);\n\n    output.apply(Log.ofElements());\n\n    pipeline.run();\n  }\n\n  static PCollection<Integer> applyTransform(PCollection<Integer> input) {\n    return input.apply(Max.integersGlobally());\n  }\n\n}"
	javaCode             = "package org.apache.beam.sdk.transforms; \n public class Class {\n    public static void main(String[] args) {\n        System.out.println(\"Hello World!\");\n    }\n}"
)

func TestCheckIsUnitTestJava(t *testing.T) {
	testValidatorArgs := getValidatorsArgs(javaUnitTestFilePath, javaUnitTestPattern)
	validatorArgs := getValidatorsArgs(javaCodePath, javaUnitTestPattern)

	type args struct {
		args []interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{
			// Test if code is unit test code
			name: "Is unit test",
			args: args{
				testValidatorArgs,
			},
			want:    true,
			wantErr: false,
		},
		{
			// Test if code is not unit test code
			name: "Is not unit test",
			args: args{
				validatorArgs,
			},
			want:    false,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := checkIsUnitTestJava(tt.args.args...)
			if (err != nil) != tt.wantErr {
				t.Errorf("checkIsUnitTestJava error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("checkIsUnitTestJava got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCheckIsKataJava(t *testing.T) {
	testValidatorArgs := getValidatorsArgs(javaKataFilePath, javaKatasPattern)
	validatorArgs := getValidatorsArgs(javaCodePath, javaKatasPattern)
	type args struct {
		args []interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{
			// Test if code is kata
			name: "Is kata",
			args: args{
				testValidatorArgs,
			},
			want:    true,
			wantErr: false,
		},
		{
			// Test if code is not kata
			name: "Is not kata",
			args: args{
				validatorArgs,
			},
			want:    false,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := checkIsKataJava(tt.args.args...)
			if (err != nil) != tt.wantErr {
				t.Errorf("checkIsKataJava() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("checkIsKataJava() got = %v, want %v", got, tt.want)
			}
		})
	}
}

// getValidatorsArgs returns array of received arguments for validators
func getValidatorsArgs(args ...interface{}) []interface{} {
	preparedArgs := make([]interface{}, 3)
	preparedArgs[0] = args[0]
	preparedArgs[2] = args[1]
	return preparedArgs
}
