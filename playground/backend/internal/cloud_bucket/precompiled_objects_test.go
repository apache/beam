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

package cloud_bucket

import (
	pb "beam.apache.org/playground/backend/internal/api/v1"
	"context"
	"reflect"
	"testing"
)

const (
	precompiledObjectPath = "SDK_JAVA/MinimalWordCount"
	targetSdk             = pb.Sdk_SDK_UNSPECIFIED
)

var bucket *CloudStorage
var ctx context.Context

func init() {
	bucket = New()
	ctx = context.Background()
}

func Test_getFullFilePath(t *testing.T) {
	type args struct {
		examplePath string
		extension   string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			// Try to get the full path to the code of the precompiled example
			// by the path to its directory on Cloud Storage:
			// (SDK_JAVA/HelloWorld, java) -> SDK_JAVA/HelloWorld/HelloWorld.java
			name: "Test getFullFilePath()",
			args: args{
				examplePath: "SDK_JAVA/HelloWorld",
				extension:   "java",
			},
			want: "SDK_JAVA/HelloWorld/HelloWorld.java",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getFullFilePath(tt.args.examplePath, tt.args.extension); got != tt.want {
				t.Errorf("getFullFilePath() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getSdkName(t *testing.T) {
	type args struct {
		path string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			// Try to get the name of the SDK from the path
			name: "Test getSdkName",
			args: args{path: "SDK_JAVA/HelloWorld"},
			want: "SDK_JAVA",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getSdkName(tt.args.path); got != tt.want {
				t.Errorf("getSdkName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_isDir(t *testing.T) {
	type args struct {
		path string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Test isDir if it is a directory",
			args: args{path: "SDK_JAVA/HelloWorld/"},
			want: true,
		},
		{
			name: "Test isDir if it is a file",
			args: args{path: "SDK_JAVA/HelloWorld/HelloWorld.java"},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isDir(tt.args.path); got != tt.want {
				t.Errorf("isDir() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_isPathToPrecompiledObjectFile(t *testing.T) {
	type args struct {
		path string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Test if path is valid",
			args: args{path: "SDK_JAVA/HelloWorld/HelloWorld.java"},
			want: true,
		},
		{
			name: "Test if path is not valid",
			args: args{path: "SDK_JAVA/HelloWorld/"},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isPathToPrecompiledObjectFile(tt.args.path); got != tt.want {
				t.Errorf("isPathToPrecompiledObjectFile() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_appendPrecompiledObject(t *testing.T) {
	type args struct {
		objectInfo      ObjectInfo
		sdkToCategories *SdkToCategories
		pathToObject    string
		categoryName    string
	}
	tests := []struct {
		name string
		args args
		want *SdkToCategories
	}{
		{
			name: "Test append new objects",
			args: args{
				objectInfo: ObjectInfo{
					Name:        "",
					CloudPath:   "",
					Description: "",
					Type:        0,
					Categories:  []string{"Common"},
				},
				sdkToCategories: &SdkToCategories{},
				pathToObject:    "SDK_JAVA/HelloWorld",
				categoryName:    "Common",
			},
			want: &SdkToCategories{"SDK_JAVA": CategoryToPrecompiledObjects{"Common": PrecompiledObjects{ObjectInfo{
				Name:        "HelloWorld",
				CloudPath:   "SDK_JAVA/HelloWorld",
				Description: "",
				Type:        0,
				Categories:  []string{"Common"},
			}}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			appendPrecompiledObject(tt.args.objectInfo, tt.args.sdkToCategories, tt.args.pathToObject, tt.args.categoryName)
			got := tt.args.sdkToCategories
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("appendPrecompiledObject() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getFileExtensionBySdk(t *testing.T) {
	type args struct {
		precompiledObjectPath string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			// Try to get an extension of a file by the sdk at file path:
			// SDK_JAVA/HelloWorld -> java
			name:    "Test getFileExtensionBySdk() valid sdk",
			args:    args{precompiledObjectPath: "SDK_JAVA/HelloWorld"},
			want:    "java",
			wantErr: false,
		},
		{
			// Try to get an error if sdk is not a valid one:
			// INVALID_SDK/HelloWorld -> ""
			name:    "Test getFileExtensionBySdk() invalid sdk",
			args:    args{precompiledObjectPath: "INVALID_SDK/HelloWorld"},
			want:    "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getFileExtensionBySdk(tt.args.precompiledObjectPath)
			if (err != nil) != tt.wantErr {
				t.Errorf("getFileExtensionBySdk() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("getFileExtensionBySdk() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Benchmark_GetPrecompiledObjects(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = bucket.GetPrecompiledObjects(ctx, targetSdk, "")
	}
}

func Benchmark_GetPrecompiledObjectOutput(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = bucket.GetPrecompiledObjectOutput(ctx, precompiledObjectPath)
	}
}

func Benchmark_GetPrecompiledObjectCode(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = bucket.GetPrecompiledObjectCode(ctx, precompiledObjectPath)
	}
}
