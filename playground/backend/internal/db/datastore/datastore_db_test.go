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
	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/db/entity"
	"beam.apache.org/playground/backend/internal/utils"
	"cloud.google.com/go/datastore"
	"context"
	"os"
	"testing"
	"time"
)

const (
	datastoreEmulatorHostKey   = "DATASTORE_EMULATOR_HOST"
	datastoreEmulatorHostValue = "127.0.0.1:8888"
	datastoreEmulatorProjectId = "test"
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
	datastoreEmulatorHost := os.Getenv(datastoreEmulatorHostKey)
	if datastoreEmulatorHost == "" {
		if err := os.Setenv(datastoreEmulatorHostKey, datastoreEmulatorHostValue); err != nil {
			panic(err)
		}
	}
	ctx = context.Background()
	var err error
	datastoreDb, err = New(ctx, datastoreEmulatorProjectId)
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
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "PutSnippet() in the usual case",
			args: args{ctx: ctx, id: "MOCK_ID", snip: &entity.Snippet{
				IDMeta: &entity.IDMeta{
					Salt:     "MOCK_SALT",
					IdLength: 11,
				},
				Snippet: &entity.SnippetEntity{
					Sdk:           utils.GetNameKey(SdkKind, "SDK_GO", Namespace, nil),
					PipeOpts:      "MOCK_OPTIONS",
					Origin:        "PG_USER",
					OwnerId:       "",
					NumberOfFiles: 1,
				},
				Files: []*entity.FileEntity{{
					Name:    "MOCK_NAME",
					Content: "MOCK_CONTENT",
					IsMain:  false,
				}},
			}},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := datastoreDb.PutSnippet(tt.args.ctx, tt.args.id, tt.args.snip)
			if err != nil {
				t.Error("PutSnippet() method failed")
			}
		})
	}

	cleanData(t, FileKind, "MOCK_ID_0", nil)
	cleanData(t, SnippetKind, "MOCK_ID", nil)
}

func TestDatastore_GetSnippet(t *testing.T) {
	nowDate := time.Now()
	type args struct {
		ctx context.Context
		id  string
	}
	tests := []struct {
		name    string
		prepare func()
		args    args
		wantErr bool
	}{
		{
			name:    "GetSnippet() with id that is no in the database",
			prepare: func() {},
			args:    args{ctx: ctx, id: "MOCK_ID"},
			wantErr: true,
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
						Sdk:           utils.GetNameKey(SdkKind, "SDK_GO", Namespace, nil),
						PipeOpts:      "MOCK_OPTIONS",
						Created:       nowDate,
						Origin:        "PG_USER",
						OwnerId:       "",
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
				if snip.Sdk.Name != "SDK_GO" ||
					snip.PipeOpts != "MOCK_OPTIONS" ||
					snip.Origin != "PG_USER" ||
					snip.OwnerId != "" {
					t.Error("GetSnippet() unexpected result")
				}
			}
		})
	}

	cleanData(t, FileKind, "MOCK_ID_0", nil)
	cleanData(t, SnippetKind, "MOCK_ID", nil)
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

	for _, sdk := range sdks {
		cleanData(t, SdkKind, sdk.Name, nil)
	}
}

func TestDatastore_PutSchemaVersion(t *testing.T) {
	type args struct {
		ctx    context.Context
		id     string
		schema *entity.SchemaEntity
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "PutSchemaVersion() in the usual case",
			args: args{
				ctx:    ctx,
				id:     "MOCK_ID",
				schema: &entity.SchemaEntity{Descr: "MOCK_DESCRIPTION"},
			},
			wantErr: false,
		},
		{
			name: "PutSchemaVersion() when input data is nil",
			args: args{
				ctx:    ctx,
				id:     "MOCK_ID",
				schema: nil,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := datastoreDb.PutSchemaVersion(tt.args.ctx, tt.args.id, tt.args.schema)
			if err != nil {
				t.Error("PutSchemaVersion() method failed")
			}
		})
	}

	cleanData(t, SchemaKind, "MOCK_ID", nil)
}

func TestDatastore_GetFiles(t *testing.T) {
	type args struct {
		ctx           context.Context
		snipId        string
		numberOfFiles int
	}
	tests := []struct {
		name    string
		prepare func()
		args    args
		wantErr bool
	}{
		{
			name:    "GetFiles() with snippet id that is no in the database",
			prepare: func() {},
			args:    args{ctx: ctx, snipId: "MOCK_ID", numberOfFiles: 1},
			wantErr: true,
		},
		{
			name: "GetFiles() in the usual case",
			prepare: func() {
				_ = datastoreDb.PutSnippet(ctx, "MOCK_ID", &entity.Snippet{
					IDMeta: &entity.IDMeta{
						Salt:     "MOCK_SALT",
						IdLength: 11,
					},
					Snippet: &entity.SnippetEntity{
						Sdk:           utils.GetNameKey(SdkKind, "SDK_GO", Namespace, nil),
						PipeOpts:      "MOCK_OPTIONS",
						Origin:        "PG_USER",
						OwnerId:       "",
						NumberOfFiles: 1,
					},
					Files: []*entity.FileEntity{{
						Name:    "MOCK_NAME",
						Content: "MOCK_CONTENT",
						IsMain:  false,
					}},
				})
			},
			args:    args{ctx: ctx, snipId: "MOCK_ID", numberOfFiles: 1},
			wantErr: false,
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
					files[0].IsMain != false {
					t.Error("GetFiles() unexpected result")
				}
				cleanData(t, FileKind, "MOCK_ID_0", nil)
				cleanData(t, SnippetKind, "MOCK_ID", nil)
			}
		})
	}
}

func TestDatastore_GetSDK(t *testing.T) {
	type args struct {
		ctx context.Context
		id  string
	}
	sdks := getSDKs()
	tests := []struct {
		name    string
		prepare func()
		args    args
		wantErr bool
	}{
		{
			name: "GetSDK() in the usual case",
			prepare: func() {
				_ = datastoreDb.PutSDKs(ctx, sdks)
			},
			args:    args{ctx: ctx, id: pb.Sdk_SDK_GO.String()},
			wantErr: false,
		},
		{
			name:    "GetSDK() when sdk is missing",
			prepare: func() {},
			args:    args{ctx: ctx, id: pb.Sdk_SDK_GO.String()},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepare()
			sdkEntity, err := datastoreDb.GetSDK(tt.args.ctx, tt.args.id)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetSDK() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil {
				if sdkEntity.DefaultExample != "MOCK_EXAMPLE" {
					t.Error("GetSDK() unexpected result")
				}
				for _, sdk := range sdks {
					cleanData(t, SdkKind, sdk.Name, nil)
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
			args:    args{ctx: ctx, projectId: datastoreEmulatorProjectId},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(ctx, datastoreEmulatorProjectId)
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func cleanData(t *testing.T, kind, id string, parentId *datastore.Key) {
	key := datastore.NameKey(kind, id, nil)
	if parentId != nil {
		key.Parent = parentId
	}
	key.Namespace = Namespace
	if err := datastoreDb.Client.Delete(ctx, key); err != nil {
		t.Errorf("Error during data cleaning after the test, err: %s", err.Error())
	}
}

func getSDKs() []*entity.SDKEntity {
	var sdkEntities []*entity.SDKEntity
	for _, sdk := range pb.Sdk_name {
		sdkEntities = append(sdkEntities, &entity.SDKEntity{
			Name:           sdk,
			DefaultExample: "MOCK_EXAMPLE",
		})
	}
	return sdkEntities
}
