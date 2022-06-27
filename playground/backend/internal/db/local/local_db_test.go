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

package local_db

import (
	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/db/entity"
	"beam.apache.org/playground/backend/internal/utils"
	"context"
	"fmt"
	"testing"
	"time"
)

var ctx context.Context
var localDb *LocalDB

func init() {
	ctx = context.Background()
	var err error
	localDb, err = New()
	if err != nil {
		fmt.Printf("Error during local database running, err: %s", err.Error())
	}
}

func TestLocalDB_PutSnippet(t *testing.T) {
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
			args: args{
				ctx: ctx,
				id:  "MOCK_ID",
				snip: &entity.Snippet{
					Snippet: &entity.SnippetEntity{
						Sdk:      utils.GetNameKey("MOCK_KIND", "SDK_GO", "MOCK_NAMESPACE", nil),
						PipeOpts: "MOCK_OPTIONS",
						Origin:   entity.PG_USER,
						OwnerId:  "",
					},
					Files: []*entity.FileEntity{{
						Content: "MOCK_CONTENT",
						IsMain:  false,
					}},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := localDb.PutSnippet(tt.args.ctx, tt.args.id, tt.args.snip)
			if (err != nil) != tt.wantErr {
				t.Errorf("PutSnippet() error = %v, wantErr %v", err, tt.wantErr)
			}
			delete(localDb.items, "MOCK_ID")
		})
	}
}

func TestLocalDB_GetSnippet(t *testing.T) {
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
			name:    "GetSnippet() when K is not in the database",
			prepare: func() {},
			args: args{
				ctx: ctx,
				id:  "MOCK_ID",
			},
			wantErr: true,
		},
		{
			name: "GetSnippet() in the usual case",
			prepare: func() {
				_ = localDb.PutSnippet(ctx, "MOCK_ID", &entity.Snippet{
					Snippet: &entity.SnippetEntity{
						Sdk:      utils.GetNameKey("MOCK_KIND", "SDK_GO", "MOCK_NAMESPACE", nil),
						PipeOpts: "MOCK_OPTIONS",
						Created:  nowDate,
						Origin:   entity.PG_USER,
						OwnerId:  "",
					},
					Files: []*entity.FileEntity{{
						Content: "MOCK_CONTENT",
						IsMain:  false,
					}},
				})
			},
			args: args{
				ctx: ctx,
				id:  "MOCK_ID",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepare()
			snip, err := localDb.GetSnippet(tt.args.ctx, tt.args.id)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetSnippet() error = %v, wantErr %v", err, tt.wantErr)
			}

			if err == nil {
				if snip.Sdk.Name != "SDK_GO" ||
					snip.PipeOpts != "MOCK_OPTIONS" ||
					snip.Origin != entity.PG_USER ||
					snip.OwnerId != "" {
					t.Error("GetSnippet() unexpected result")
				}
				delete(localDb.items, "MOCK_ID")
			}
		})
	}
}

func TestLocalDB_PutSDKs(t *testing.T) {
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
			err := localDb.PutSDKs(tt.args.ctx, tt.args.sdks)
			if err != nil {
				t.Error("PutSDKs() method failed")
			}
			for _, sdk := range sdks {
				delete(localDb.items, sdk.Name)
			}
		})
	}
}

func TestLocalDB_PutSchemaVersion(t *testing.T) {
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
			err := localDb.PutSchemaVersion(tt.args.ctx, tt.args.id, tt.args.schema)
			if err != nil {
				t.Error("PutSchemaVersion() method failed")
			}
			delete(localDb.items, "MOCK_ID")
		})
	}
}

func TestLocalDB_GetCodes(t *testing.T) {
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
				_ = localDb.PutSnippet(ctx, "MOCK_ID", &entity.Snippet{
					IDMeta: &entity.IDMeta{
						Salt:     "MOCK_SALT",
						IdLength: 11,
					},
					Snippet: &entity.SnippetEntity{
						Sdk:      utils.GetNameKey("MOCK_KIND", "SDK_GO", "MOCK_NAMESPACE", nil),
						PipeOpts: "MOCK_OPTIONS",
						Origin:   entity.PG_USER,
						OwnerId:  "",
					},
					Files: []*entity.FileEntity{{
						Content: "MOCK_CONTENT",
						IsMain:  false,
					}},
				})
			},
			args:    args{ctx: ctx, snipId: "MOCK_ID"},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepare()
			codes, err := localDb.GetFiles(tt.args.ctx, tt.args.snipId, tt.args.numberOfFiles)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetFiles() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil {
				if len(codes) != 1 ||
					codes[0].Content != "MOCK_CONTENT" ||
					codes[0].IsMain != false {
					t.Error("GetFiles() unexpected result")
					delete(localDb.items, "MOCK_ID")
				}
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
				_ = localDb.PutSDKs(ctx, sdks)
			},
			args:    args{ctx: ctx, id: "SDK_GO"},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepare()
			sdkEntity, err := localDb.GetSDK(tt.args.ctx, tt.args.id)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetSDK() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil {
				if sdkEntity.DefaultExample != "MOCK_EXAMPLE" {
					t.Error("GetSDK() unexpected result")
				}
				for _, sdk := range sdks {
					delete(localDb.items, sdk.Name)
				}
			}
		})
	}
}

func TestNew(t *testing.T) {
	tests := []struct {
		name    string
		wantErr bool
	}{
		{
			name:    "Initialize local database",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(); (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
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
