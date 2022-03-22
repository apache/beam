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
	goUnitTestFilePath = "unitTestCode.go"
	goCodePath         = "code.go"
	goUnitTestCode     = "func TestDedup(t *testing.T) {\n\ttests := []struct {\n\t\tdups []interface{}\n\t\texp  []interface{}\n}}"
	goCode             = "func main() {\n\t// beam.Init() is an initialization hook that must be called on startup.\n\tbeam.Init()\n\n\t// Create the Pipeline object and root scope.\n\tp := beam.NewPipeline()\n\ts := p.Root()\n}"
)

func TestCheckIsUnitTestGo(t *testing.T) {
	testValidatorArgs := make([]interface{}, 1)
	testValidatorArgs[0] = goUnitTestFilePath

	validatorArgs := make([]interface{}, 1)
	validatorArgs[0] = goCodePath

	argsWithoutRealFile := make([]interface{}, 1)
	argsWithoutRealFile[0] = "fileNotExists.go"

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
			name: "Check that file is a go unit test",
			args: args{
				testValidatorArgs,
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "Check that file is not a go unit test",
			args: args{
				validatorArgs,
			},
			want:    false,
			wantErr: false,
		},
		{
			name:    "Error if file not exists",
			args:    args{args: argsWithoutRealFile},
			want:    false,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CheckIsUnitTestGo(tt.args.args...)
			if (err != nil) != tt.wantErr {
				t.Errorf("CheckIsUnitTestGo() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("CheckIsUnitTestGo() got = %v, want %v", got, tt.want)
			}
		})
	}
}
