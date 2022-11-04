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

package datastore

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/constants"
	"beam.apache.org/playground/backend/internal/db/entity"
	"beam.apache.org/playground/backend/internal/db/mapper"
	"beam.apache.org/playground/backend/internal/tests/test_cleaner"
	"beam.apache.org/playground/backend/internal/utils"
)

var datastoreDb *Datastore
var ctx context.Context

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	teardown()
	os.Exit(code)
}

func setup() {
	datastoreEmulatorHost := os.Getenv(constants.EmulatorHostKey)
	if datastoreEmulatorHost == "" {
		if err := os.Setenv(constants.EmulatorHostKey, constants.EmulatorHostValue); err != nil {
			panic(err)
		}
	}
	ctx = context.Background()
	ctx = context.WithValue(ctx, constants.DatastoreNamespaceKey, "datastore")
	var err error
	datastoreDb, err = New(ctx, mapper.NewPrecompiledObjectMapper(), constants.EmulatorProjectId)
	if err != nil {
		panic(err)
	}
}

func teardown() {
	if err := datastoreDb.Client.Close(); err != nil {
		panic(err)
	}
}

func TestDatastore_PutSnippet(t *testing.T) {
	type args struct {
		ctx  context.Context
		id   string
		snip *entity.Snippet
	}
	tests := []struct {
		name      string
		args      args
		wantErr   bool
		cleanData func()
	}{
		{
			name: "PutSnippet() in the usual case",
			args: args{ctx: ctx, id: "MOCK_ID", snip: &entity.Snippet{
				IDMeta: &entity.IDMeta{
					Salt:     "MOCK_SALT",
					IdLength: 11,
				},
				Snippet: &entity.SnippetEntity{
					Sdk:           utils.GetSdkKey(ctx, pb.Sdk_SDK_GO.String()),
					PipeOpts:      "MOCK_OPTIONS",
					Origin:        constants.UserSnippetOrigin,
					NumberOfFiles: 1,
				},
				Files: []*entity.FileEntity{{
					Name:    "MOCK_NAME",
					Content: "MOCK_CONTENT",
					IsMain:  false,
				}},
			}},
			wantErr: false,
			cleanData: func() {
				test_cleaner.CleanFiles(ctx, t, "MOCK_ID", 1)
				test_cleaner.CleanSnippet(ctx, t, "MOCK_ID")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := datastoreDb.PutSnippet(tt.args.ctx, tt.args.id, tt.args.snip)
			if err != nil {
				t.Error("PutSnippet() method failed")
			}
			tt.cleanData()
		})
	}
}

func TestDatastore_GetSnippet(t *testing.T) {
	nowDate := time.Now()
	type args struct {
		ctx context.Context
		id  string
	}
	tests := []struct {
		name      string
		prepare   func()
		args      args
		wantErr   bool
		cleanData func()
	}{
		{
			name:      "GetSnippet() with id that is no in the database",
			prepare:   func() {},
			args:      args{ctx: ctx, id: "MOCK_ID"},
			wantErr:   true,
			cleanData: func() {},
		},
		{
			name: "GetSnippet() in the usual case",
			prepare: func() {
				_ = datastoreDb.PutSnippet(ctx, "MOCK_ID", &entity.Snippet{
					IDMeta: &entity.IDMeta{
						Salt:     "MOCK_SALT",
						IdLength: 11,
					},
					Snippet: &entity.SnippetEntity{
						Sdk:           utils.GetSdkKey(ctx, pb.Sdk_SDK_GO.String()),
						PipeOpts:      "MOCK_OPTIONS",
						Created:       nowDate,
						Origin:        constants.UserSnippetOrigin,
						NumberOfFiles: 1,
					},
					Files: []*entity.FileEntity{{
						Name:    "MOCK_NAME",
						Content: "MOCK_CONTENT",
						IsMain:  false,
					}},
				})
			},
			args:    args{ctx: ctx, id: "MOCK_ID"},
			wantErr: false,
			cleanData: func() {
				test_cleaner.CleanFiles(ctx, t, "MOCK_ID", 1)
				test_cleaner.CleanSnippet(ctx, t, "MOCK_ID")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepare()
			snip, err := datastoreDb.GetSnippet(tt.args.ctx, tt.args.id)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetSnippet() error = %v, wantErr %v", err, tt.wantErr)
			}

			if err == nil {
				if snip.Sdk.Name != pb.Sdk_SDK_GO.String() ||
					snip.PipeOpts != "MOCK_OPTIONS" ||
					snip.Origin != constants.UserSnippetOrigin ||
					snip.OwnerId != "" {
					t.Error("GetSnippet() unexpected result")
				}
			}
			tt.cleanData()
		})
	}
}

func TestDatastore_PutSDKs(t *testing.T) {
	type args struct {
		ctx  context.Context
		sdks []*entity.SDKEntity
	}
	sdks := getSDKs()
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "PutSDKs() in the usual case",
			args: args{
				ctx:  ctx,
				sdks: sdks,
			},
			wantErr: false,
		},
		{
			name: "PutSDKs() when input data is nil",
			args: args{
				ctx:  ctx,
				sdks: nil,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := datastoreDb.PutSDKs(tt.args.ctx, tt.args.sdks)
			if err != nil {
				t.Error("PutSDKs() method failed")
			}
		})
	}
}

func TestDatastore_PutSchemaVersion(t *testing.T) {
	type args struct {
		ctx    context.Context
		id     string
		schema *entity.SchemaEntity
	}
	tests := []struct {
		name      string
		args      args
		wantErr   bool
		cleanData func()
	}{
		{
			name: "PutSchemaVersion() in the usual case",
			args: args{
				ctx:    ctx,
				id:     "MOCK_ID",
				schema: &entity.SchemaEntity{Descr: "MOCK_DESCRIPTION"},
			},
			wantErr: false,
			cleanData: func() {
				test_cleaner.CleanSchemaVersion(ctx, t, "MOCK_ID")
			},
		},
		{
			name: "PutSchemaVersion() when input data is nil",
			args: args{
				ctx:    ctx,
				id:     "MOCK_ID",
				schema: nil,
			},
			wantErr:   false,
			cleanData: func() {},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := datastoreDb.PutSchemaVersion(tt.args.ctx, tt.args.id, tt.args.schema)
			if err != nil {
				t.Error("PutSchemaVersion() method failed")
			}
			tt.cleanData()
		})
	}
}

func TestDatastore_GetFiles(t *testing.T) {
	type args struct {
		ctx           context.Context
		snipId        string
		numberOfFiles int
	}
	tests := []struct {
		name      string
		prepare   func()
		args      args
		wantErr   bool
		cleanData func()
	}{
		{
			name:      "GetFiles() with snippet id that is no in the database",
			prepare:   func() {},
			args:      args{ctx: ctx, snipId: "MOCK_ID", numberOfFiles: 1},
			wantErr:   true,
			cleanData: func() {},
		},
		{
			name:    "GetFiles() in the usual case",
			prepare: func() { saveSnippet("MOCK_ID", pb.Sdk_SDK_GO.String()) },
			args:    args{ctx: ctx, snipId: "MOCK_ID", numberOfFiles: 1},
			wantErr: false,
			cleanData: func() {
				test_cleaner.CleanFiles(ctx, t, "MOCK_ID", 1)
				test_cleaner.CleanSnippet(ctx, t, "MOCK_ID")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepare()
			files, err := datastoreDb.GetFiles(tt.args.ctx, tt.args.snipId, tt.args.numberOfFiles)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetFiles() error = %v, wantErr %v", err, tt.wantErr)
			}
			if files != nil {
				if len(files) != 1 ||
					files[0].Content != "MOCK_CONTENT" ||
					files[0].IsMain != true {
					t.Error("GetFiles() unexpected result")
				}
				tt.cleanData()
			}
		})
	}
}

func TestDatastore_GetSDKs(t *testing.T) {
	type args struct {
		ctx context.Context
	}
	sdks := getSDKs()
	tests := []struct {
		name    string
		prepare func()
		args    args
		wantErr bool
	}{
		{
			name:    "GetSDKs() in the usual case",
			prepare: func() { _ = datastoreDb.PutSDKs(ctx, sdks) },
			args:    args{ctx: ctx},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepare()
			sdkEntities, err := datastoreDb.GetSDKs(tt.args.ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetSDKs() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil {
				if len(sdkEntities) != 4 {
					t.Error("GetSDK unexpected result, should be four entities")
				}
			}
		})
	}
}

func TestDatastore_GetCatalog(t *testing.T) {
	type args struct {
		ctx        context.Context
		sdkCatalog []*entity.SDKEntity
	}
	tests := []struct {
		name      string
		prepare   func()
		args      args
		wantErr   bool
		cleanData func()
	}{
		{
			name: "Getting catalog in the usual case",
			prepare: func() {
				exampleId := utils.GetIDWithDelimiter(pb.Sdk_SDK_JAVA.String(), "MOCK_EXAMPLE")
				saveExample("MOCK_EXAMPLE", pb.Sdk_SDK_JAVA.String())
				saveSnippet(exampleId, pb.Sdk_SDK_JAVA.String())
				savePCObjs(exampleId)
			},
			args: args{
				ctx: ctx,
				sdkCatalog: func() []*entity.SDKEntity {
					var sdks []*entity.SDKEntity
					for sdkName := range pb.Sdk_value {
						sdks = append(sdks, &entity.SDKEntity{
							Name:           sdkName,
							DefaultExample: "MOCK_DEFAULT_EXAMPLE",
						})
					}
					return sdks
				}(),
			},
			wantErr: false,
			cleanData: func() {
				exampleId := utils.GetIDWithDelimiter(pb.Sdk_SDK_JAVA.String(), "MOCK_EXAMPLE")
				test_cleaner.CleanPCObjs(ctx, t, exampleId)
				test_cleaner.CleanFiles(ctx, t, exampleId, 1)
				test_cleaner.CleanSnippet(ctx, t, exampleId)
				test_cleaner.CleanExample(ctx, t, exampleId)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepare()
			catalog, err := datastoreDb.GetCatalog(tt.args.ctx, tt.args.sdkCatalog)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetCatalog() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil {
				if catalog[0].GetSdk() != pb.Sdk_SDK_JAVA {
					t.Error("GetCatalog() unexpected result: wrong sdk")
				}
				actualCatName := catalog[0].GetCategories()[0].GetCategoryName()
				actualPCObj := catalog[0].GetCategories()[0].GetPrecompiledObjects()[0]
				if actualCatName != "MOCK_CATEGORY" {
					t.Error("GetCatalog() unexpected result: wrong category")
				}
				if actualPCObj.DefaultExample != false ||
					actualPCObj.Multifile != false ||
					actualPCObj.Name != "MOCK_EXAMPLE" ||
					actualPCObj.Type.String() != "PRECOMPILED_OBJECT_TYPE_EXAMPLE" ||
					actualPCObj.CloudPath != "SDK_JAVA/PRECOMPILED_OBJECT_TYPE_EXAMPLE/MOCK_EXAMPLE" ||
					actualPCObj.PipelineOptions != "MOCK_OPTIONS" ||
					actualPCObj.Description != "MOCK_DESCR" ||
					actualPCObj.Link != "MOCK_PATH" ||
					actualPCObj.ContextLine != 32 ||
					actualPCObj.Complexity != pb.Complexity_COMPLEXITY_MEDIUM {
					t.Error("GetCatalog() unexpected result: wrong precompiled obj")
				}
				tt.cleanData()
			}
		})
	}
}

func TestDatastore_GetDefaultExamples(t *testing.T) {
	type args struct {
		ctx  context.Context
		sdks []*entity.SDKEntity
	}
	tests := []struct {
		name    string
		prepare func()
		args    args
		wantErr bool
		clean   func()
	}{
		{
			name: "Getting default examples in the usual case",
			prepare: func() {
				for sdk := range pb.Sdk_value {
					exampleId := utils.GetIDWithDelimiter(sdk, "MOCK_DEFAULT_EXAMPLE")
					saveExample("MOCK_DEFAULT_EXAMPLE", sdk)
					saveSnippet(exampleId, sdk)
					savePCObjs(exampleId)
				}
			},
			args: args{
				ctx:  ctx,
				sdks: getSDKs(),
			},
			wantErr: false,
			clean: func() {
				for sdk := range pb.Sdk_value {
					exampleId := utils.GetIDWithDelimiter(sdk, "MOCK_DEFAULT_EXAMPLE")
					test_cleaner.CleanPCObjs(ctx, t, exampleId)
					test_cleaner.CleanFiles(ctx, t, exampleId, 1)
					test_cleaner.CleanSnippet(ctx, t, exampleId)
					test_cleaner.CleanExample(ctx, t, exampleId)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepare()
			defaultExamples, err := datastoreDb.GetDefaultExamples(tt.args.ctx, tt.args.sdks)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetDefaultExamples() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil {
				if len(defaultExamples) < 4 {
					t.Errorf("GetDefaultExamples() unexpected result: the length of example should be at least 4")
				}
				for _, example := range defaultExamples {
					if example.DefaultExample != true ||
						example.Name != "MOCK_DEFAULT_EXAMPLE" ||
						example.ContextLine != 32 ||
						example.Multifile != false ||
						example.Type.String() != "PRECOMPILED_OBJECT_TYPE_EXAMPLE" ||
						example.PipelineOptions != "MOCK_OPTIONS" ||
						example.Description != "MOCK_DESCR" ||
						example.Link != "MOCK_PATH" ||
						example.Complexity != pb.Complexity_COMPLEXITY_MEDIUM {
						t.Errorf("GetDefaultExamples() unexpected result: wrong precompiled obj")
					}
				}
				tt.clean()
			}
		})
	}
}

func TestDatastore_GetExample(t *testing.T) {
	type args struct {
		ctx       context.Context
		exampleId string
		sdks      []*entity.SDKEntity
	}
	tests := []struct {
		name    string
		prepare func()
		args    args
		wantErr bool
		clean   func()
	}{
		{
			name: "Getting an example in the usual case",
			prepare: func() {
				exampleId := utils.GetIDWithDelimiter(pb.Sdk_SDK_JAVA.String(), "MOCK_EXAMPLE")
				saveExample("MOCK_EXAMPLE", pb.Sdk_SDK_JAVA.String())
				saveSnippet(exampleId, pb.Sdk_SDK_JAVA.String())
				savePCObjs(exampleId)
			},
			args: args{
				ctx:       ctx,
				exampleId: utils.GetIDWithDelimiter(pb.Sdk_SDK_JAVA.String(), "MOCK_EXAMPLE"),
				sdks:      getSDKs(),
			},
			wantErr: false,
			clean: func() {
				exampleId := utils.GetIDWithDelimiter(pb.Sdk_SDK_JAVA.String(), "MOCK_EXAMPLE")
				test_cleaner.CleanPCObjs(ctx, t, exampleId)
				test_cleaner.CleanFiles(ctx, t, exampleId, 1)
				test_cleaner.CleanSnippet(ctx, t, exampleId)
				test_cleaner.CleanExample(ctx, t, exampleId)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepare()
			example, err := datastoreDb.GetExample(tt.args.ctx, tt.args.exampleId, tt.args.sdks)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetExample() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil {
				if example.DefaultExample != false ||
					example.Name != "MOCK_EXAMPLE" ||
					example.Multifile != false ||
					example.ContextLine != 32 ||
					example.Description != "MOCK_DESCR" ||
					example.Type.String() != "PRECOMPILED_OBJECT_TYPE_EXAMPLE" ||
					example.Link != "MOCK_PATH" ||
					example.PipelineOptions != "MOCK_OPTIONS" ||
					example.CloudPath != "SDK_JAVA/PRECOMPILED_OBJECT_TYPE_EXAMPLE/MOCK_EXAMPLE" ||
					example.Complexity != pb.Complexity_COMPLEXITY_MEDIUM {
					t.Errorf("GetExample() unexpected result: wrong precompiled obj")
				}
				tt.clean()
			}
		})
	}
}

func TestDatastore_GetExampleCode(t *testing.T) {
	type args struct {
		ctx       context.Context
		exampleId string
	}
	tests := []struct {
		name    string
		prepare func()
		args    args
		wantErr bool
		clean   func()
	}{
		{
			name: "Getting an example code in the usual case",
			prepare: func() {
				exampleId := utils.GetIDWithDelimiter(pb.Sdk_SDK_JAVA.String(), "MOCK_EXAMPLE")
				saveExample("MOCK_EXAMPLE", pb.Sdk_SDK_JAVA.String())
				saveSnippet(exampleId, pb.Sdk_SDK_JAVA.String())
				savePCObjs(exampleId)
			},
			args: args{
				ctx:       ctx,
				exampleId: utils.GetIDWithDelimiter(pb.Sdk_SDK_JAVA.String(), "MOCK_EXAMPLE"),
			},
			wantErr: false,
			clean: func() {
				exampleId := utils.GetIDWithDelimiter(pb.Sdk_SDK_JAVA.String(), "MOCK_EXAMPLE")
				test_cleaner.CleanPCObjs(ctx, t, exampleId)
				test_cleaner.CleanFiles(ctx, t, exampleId, 1)
				test_cleaner.CleanSnippet(ctx, t, exampleId)
				test_cleaner.CleanExample(ctx, t, exampleId)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepare()
			code, err := datastoreDb.GetExampleCode(tt.args.ctx, tt.args.exampleId)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetExampleCode() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil {
				if code != "MOCK_CONTENT" {
					t.Errorf("GetExampleCode() unexpected result: wrong precompiled obj")
				}
				tt.clean()
			}
		})
	}
}

func TestDatastore_GetExampleOutput(t *testing.T) {
	type args struct {
		ctx       context.Context
		exampleId string
	}
	tests := []struct {
		name    string
		prepare func()
		args    args
		wantErr bool
		clean   func()
	}{
		{
			name: "Getting an example output in the usual case",
			prepare: func() {
				exampleId := utils.GetIDWithDelimiter(pb.Sdk_SDK_JAVA.String(), "MOCK_EXAMPLE")
				saveExample("MOCK_EXAMPLE", pb.Sdk_SDK_JAVA.String())
				saveSnippet(exampleId, pb.Sdk_SDK_JAVA.String())
				savePCObjs(exampleId)
			},
			args: args{
				ctx:       ctx,
				exampleId: utils.GetIDWithDelimiter(pb.Sdk_SDK_JAVA.String(), "MOCK_EXAMPLE"),
			},
			wantErr: false,
			clean: func() {
				exampleId := utils.GetIDWithDelimiter(pb.Sdk_SDK_JAVA.String(), "MOCK_EXAMPLE")
				test_cleaner.CleanPCObjs(ctx, t, exampleId)
				test_cleaner.CleanFiles(ctx, t, exampleId, 1)
				test_cleaner.CleanSnippet(ctx, t, exampleId)
				test_cleaner.CleanExample(ctx, t, exampleId)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepare()
			output, err := datastoreDb.GetExampleOutput(tt.args.ctx, tt.args.exampleId)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetExampleOutput() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil {
				if output != "MOCK_CONTENT_OUTPUT" {
					t.Errorf("GetExampleOutput() unexpected result: wrong precompiled obj")
				}
				tt.clean()
			}
		})
	}
}

func TestDatastore_GetExampleLogs(t *testing.T) {
	type args struct {
		ctx       context.Context
		exampleId string
	}
	tests := []struct {
		name    string
		prepare func()
		args    args
		wantErr bool
		clean   func()
	}{
		{
			name: "Getting an example logs in the usual case",
			prepare: func() {
				exampleId := utils.GetIDWithDelimiter(pb.Sdk_SDK_JAVA.String(), "MOCK_EXAMPLE")
				saveExample("MOCK_EXAMPLE", pb.Sdk_SDK_JAVA.String())
				saveSnippet(exampleId, pb.Sdk_SDK_JAVA.String())
				savePCObjs(exampleId)
			},
			args: args{
				ctx:       ctx,
				exampleId: utils.GetIDWithDelimiter(pb.Sdk_SDK_JAVA.String(), "MOCK_EXAMPLE"),
			},
			wantErr: false,
			clean: func() {
				exampleId := utils.GetIDWithDelimiter(pb.Sdk_SDK_JAVA.String(), "MOCK_EXAMPLE")
				test_cleaner.CleanPCObjs(ctx, t, exampleId)
				test_cleaner.CleanFiles(ctx, t, exampleId, 1)
				test_cleaner.CleanSnippet(ctx, t, exampleId)
				test_cleaner.CleanExample(ctx, t, exampleId)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepare()
			output, err := datastoreDb.GetExampleLogs(tt.args.ctx, tt.args.exampleId)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetExampleLogs() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil {
				if output != "MOCK_CONTENT_LOG" {
					t.Errorf("GetExampleLogs() unexpected result: wrong precompiled obj")
				}
				tt.clean()
			}
		})
	}
}

func TestDatastore_GetExampleGraph(t *testing.T) {
	type args struct {
		ctx       context.Context
		exampleId string
	}
	tests := []struct {
		name    string
		prepare func()
		args    args
		wantErr bool
		clean   func()
	}{
		{
			name: "Getting an example graph in the usual case",
			prepare: func() {
				exampleId := utils.GetIDWithDelimiter(pb.Sdk_SDK_JAVA.String(), "MOCK_EXAMPLE")
				saveExample("MOCK_EXAMPLE", pb.Sdk_SDK_JAVA.String())
				saveSnippet(exampleId, pb.Sdk_SDK_JAVA.String())
				savePCObjs(exampleId)
			},
			args: args{
				ctx:       ctx,
				exampleId: utils.GetIDWithDelimiter(pb.Sdk_SDK_JAVA.String(), "MOCK_EXAMPLE"),
			},
			wantErr: false,
			clean: func() {
				exampleId := utils.GetIDWithDelimiter(pb.Sdk_SDK_JAVA.String(), "MOCK_EXAMPLE")
				test_cleaner.CleanPCObjs(ctx, t, exampleId)
				test_cleaner.CleanFiles(ctx, t, exampleId, 1)
				test_cleaner.CleanSnippet(ctx, t, exampleId)
				test_cleaner.CleanExample(ctx, t, exampleId)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepare()
			output, err := datastoreDb.GetExampleGraph(tt.args.ctx, tt.args.exampleId)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetExampleGraph() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil {
				if output != "MOCK_CONTENT_GRAPH" {
					t.Errorf("GetExampleGraph() unexpected result: wrong precompiled obj")
				}
				tt.clean()
			}
		})
	}
}

func TestDatastore_DeleteUnusedSnippets(t *testing.T) {
	type args struct {
		ctx     context.Context
		dayDiff int32
	}
	now := time.Now()
	tests := []struct {
		name    string
		args    args
		prepare func()
		wantErr bool
	}{
		{
			name: "DeleteUnusedSnippets() with different cases",
			args: args{ctx: ctx, dayDiff: 10},
			prepare: func() {
				//last visit date is now - 7 days
				putSnippet("MOCK_ID0", constants.UserSnippetOrigin, now.Add(-time.Hour*24*7), 2)
				//last visit date is now - 10 days
				putSnippet("MOCK_ID1", constants.UserSnippetOrigin, now.Add(-time.Hour*24*10), 4)
				//last visit date is now - 15 days
				putSnippet("MOCK_ID2", constants.UserSnippetOrigin, now.Add(-time.Hour*24*15), 8)
				//last visit date is now
				putSnippet("MOCK_ID3", constants.UserSnippetOrigin, now, 1)
				//last visit date is now + 2 days
				putSnippet("MOCK_ID4", constants.UserSnippetOrigin, now.Add(time.Hour*24*2), 2)
				//last visit date is now + 10 days
				putSnippet("MOCK_ID5", constants.UserSnippetOrigin, now.Add(time.Hour*24*10), 2)
				//last visit date is now - 18 days
				putSnippet("MOCK_ID6", constants.UserSnippetOrigin, now.Add(-time.Hour*24*18), 3)
				//last visit date is now - 18 days and origin != PG_USER
				putSnippet("MOCK_ID7", constants.ExampleOrigin, now.Add(-time.Hour*24*18), 2)
				//last visit date is now - 9 days
				putSnippet("MOCK_ID8", constants.UserSnippetOrigin, now.Add(-time.Hour*24*9), 2)
				//last visit date is now - 11 days
				putSnippet("MOCK_ID9", constants.UserSnippetOrigin, now.Add(-time.Hour*24*11), 2)
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepare()
			err := datastoreDb.DeleteUnusedSnippets(tt.args.ctx, tt.args.dayDiff)
			if (err != nil) != tt.wantErr {
				t.Errorf("DeleteUnusedSnippets() error = %v, wantErr %v", err, tt.wantErr)
			}

			if err == nil {
				_, err = datastoreDb.GetSnippet(tt.args.ctx, "MOCK_ID0")
				_, err = datastoreDb.GetFiles(tt.args.ctx, "MOCK_ID0", 2)
				if err != nil {
					t.Errorf("DeleteUnusedSnippets() this snippet shouldn't be deleted, err: %s", err)
				}
				_, err = datastoreDb.GetSnippet(tt.args.ctx, "MOCK_ID1")
				_, err = datastoreDb.GetFiles(tt.args.ctx, "MOCK_ID1", 4)
				if err == nil {
					t.Errorf("DeleteUnusedSnippets() this snippet should be deleted, err: %s", err)
				}
				_, err = datastoreDb.GetSnippet(tt.args.ctx, "MOCK_ID2")
				_, err = datastoreDb.GetFiles(tt.args.ctx, "MOCK_ID2", 8)
				if err == nil {
					t.Errorf("DeleteUnusedSnippets() this snippet should be deleted, err: %s", err)
				}
				_, err = datastoreDb.GetSnippet(tt.args.ctx, "MOCK_ID3")
				_, err = datastoreDb.GetFiles(tt.args.ctx, "MOCK_ID3", 1)
				if err != nil {
					t.Errorf("DeleteUnusedSnippets() this snippet shouldn't be deleted, err: %s", err)
				}
				_, err = datastoreDb.GetSnippet(tt.args.ctx, "MOCK_ID4")
				_, err = datastoreDb.GetFiles(tt.args.ctx, "MOCK_ID4", 2)
				if err != nil {
					t.Errorf("DeleteUnusedSnippets() this snippet shouldn't be deleted, err: %s", err)
				}
				_, err = datastoreDb.GetSnippet(tt.args.ctx, "MOCK_ID5")
				_, err = datastoreDb.GetFiles(tt.args.ctx, "MOCK_ID5", 2)
				if err != nil {
					t.Errorf("DeleteUnusedSnippets() this snippet shouldn't be deleted, err: %s", err)
				}
				_, err = datastoreDb.GetSnippet(tt.args.ctx, "MOCK_ID6")
				_, err = datastoreDb.GetFiles(tt.args.ctx, "MOCK_ID6", 3)
				if err == nil {
					t.Errorf("DeleteUnusedSnippets() this snippet should be deleted, err: %s", err)
				}
				_, err = datastoreDb.GetSnippet(tt.args.ctx, "MOCK_ID7")
				_, err = datastoreDb.GetFiles(tt.args.ctx, "MOCK_ID7", 2)
				if err != nil {
					t.Errorf("DeleteUnusedSnippets() this snippet shouldn't be deleted, err: %s", err)
				}
				_, err = datastoreDb.GetSnippet(tt.args.ctx, "MOCK_ID8")
				_, err = datastoreDb.GetFiles(tt.args.ctx, "MOCK_ID8", 2)
				if err != nil {
					t.Errorf("DeleteUnusedSnippets() this snippet shouldn't be deleted, err: %s", err)
				}
				_, err = datastoreDb.GetSnippet(tt.args.ctx, "MOCK_ID9")
				_, err = datastoreDb.GetFiles(tt.args.ctx, "MOCK_ID9", 2)
				if err == nil {
					t.Errorf("DeleteUnusedSnippets() this snippet should be deleted, err: %s", err)
				}
			}

		})
	}
}

func TestNew(t *testing.T) {
	type args struct {
		ctx       context.Context
		projectId string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "Initialize datastore database",
			args:    args{ctx: ctx, projectId: constants.EmulatorProjectId},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(ctx, mapper.NewPrecompiledObjectMapper(), constants.EmulatorProjectId)
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func saveExample(name, sdk string) {
	_, _ = datastoreDb.Client.Put(ctx, utils.GetExampleKey(ctx, sdk, name), &entity.ExampleEntity{
		Name:   name,
		Sdk:    utils.GetSdkKey(ctx, sdk),
		Descr:  "MOCK_DESCR",
		Cats:   []string{"MOCK_CATEGORY"},
		Path:   "MOCK_PATH",
		Type:   pb.PrecompiledObjectType_PRECOMPILED_OBJECT_TYPE_EXAMPLE.String(),
		Origin: constants.ExampleOrigin,
		SchVer: utils.GetSchemaVerKey(ctx, "MOCK_VERSION"),
	})
}

func saveSnippet(snipId, sdk string) {
	_ = datastoreDb.PutSnippet(ctx, snipId, &entity.Snippet{
		IDMeta: &entity.IDMeta{
			Salt:     "MOCK_SALT",
			IdLength: 11,
		},
		Snippet: &entity.SnippetEntity{
			Sdk:           utils.GetSdkKey(ctx, sdk),
			PipeOpts:      "MOCK_OPTIONS",
			Origin:        constants.ExampleOrigin,
			NumberOfFiles: 1,
			Complexity:    pb.Complexity_COMPLEXITY_MEDIUM.String(),
		},
		Files: []*entity.FileEntity{{
			Name:     "MOCK_NAME",
			Content:  "MOCK_CONTENT",
			CntxLine: 32,
			IsMain:   true,
		}},
	})
}

func savePCObjs(exampleId string) {
	pcTypes := []string{constants.PCOutputType, constants.PCLogType, constants.PCGraphType}
	for _, pcType := range pcTypes {
		_, _ = datastoreDb.Client.Put(
			ctx,
			utils.GetPCObjectKey(ctx, exampleId, pcType),
			&entity.PrecompiledObjectEntity{Content: "MOCK_CONTENT_" + pcType})
	}
}

func getSDKs() []*entity.SDKEntity {
	var sdkEntities []*entity.SDKEntity
	for _, sdk := range pb.Sdk_name {
		if sdk == pb.Sdk_SDK_UNSPECIFIED.String() {
			continue
		}
		sdkEntities = append(sdkEntities, &entity.SDKEntity{
			Name:           sdk,
			DefaultExample: "MOCK_DEFAULT_EXAMPLE",
		})
	}
	return sdkEntities
}

func putSnippet(id, origin string, lVisited time.Time, numberOfFiles int) {
	var files []*entity.FileEntity
	for i := 0; i < numberOfFiles; i++ {
		file := &entity.FileEntity{
			Name:    fmt.Sprintf("%s_%d", "MOCK_NAME", i),
			Content: fmt.Sprintf("%s_%d", "MOCK_CONTENT", i),
		}
		files = append(files, file)
	}
	_ = datastoreDb.PutSnippet(ctx, id, &entity.Snippet{
		IDMeta: &entity.IDMeta{Salt: "MOCK_SALT", IdLength: 11},
		Snippet: &entity.SnippetEntity{
			Sdk:           utils.GetSdkKey(ctx, pb.Sdk_SDK_GO.String()),
			PipeOpts:      "MOCK_OPTIONS",
			LVisited:      lVisited,
			Origin:        origin,
			NumberOfFiles: numberOfFiles,
		},
		Files: files,
	})
}
