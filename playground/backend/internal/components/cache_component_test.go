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

package components

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/cache"
	"beam.apache.org/playground/backend/internal/cache/local"
	"beam.apache.org/playground/backend/internal/constants"
	db "beam.apache.org/playground/backend/internal/db/datastore"
	"beam.apache.org/playground/backend/internal/db/entity"
	"beam.apache.org/playground/backend/internal/tests/test_cleaner"
	"beam.apache.org/playground/backend/internal/utils"
)

var datastoreDb *db.EmulatedDatastore
var ctx context.Context
var cacheComponent *CacheComponent
var cacheService cache.Cache
var defaultCacheRequestTimeout = 10 * time.Second

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	teardown()
	os.Exit(code)
}

func setup() {
	ctx = context.Background()
	ctx = context.WithValue(ctx, constants.DatastoreNamespaceKey, "components")
	cacheService = local.New(ctx)
	var err error
	datastoreDb, err = db.NewEmulated(ctx)
	if err != nil {
		panic(err)
	}
	cacheComponent = NewService(cacheService, datastoreDb)
}

func teardown() {
	clientCloseErr := datastoreDb.Close()
	if clientCloseErr != nil {
		panic(clientCloseErr)
	}
}

func TestCacheComponent_GetSdkCatalogFromCacheOrDatastore(t *testing.T) {
	sdks := getSDKs()
	tests := []struct {
		name    string
		prepare func()
		wantErr bool
	}{
		{
			name: "Getting sdk catalog from cache in the usual case",
			prepare: func() {
				_ = cacheService.SetSdkCatalog(ctx, sdks)
			},
			wantErr: false,
		},
		{
			name: "Getting sdk catalog from datastore in the usual case",
			prepare: func() {
				_ = datastoreDb.PutSDKs(ctx, sdks)
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepare()
			result, err := cacheComponent.GetSdkCatalogFromCacheOrDatastore(ctx, defaultCacheRequestTimeout)
			if (err != nil) != tt.wantErr {
				t.Error("GetSdkCatalogFromCacheOrDatastore() unexpected error")
				return
			}
			if !reflect.DeepEqual(sdks, result) {
				t.Error("GetSdkCatalogFromCacheOrDatastore() unexpected result")
			}
		})
	}
}

func TestCacheComponent_GetCatalogFromCacheOrDatastore(t *testing.T) {
	catalog := getCatalog()
	tests := []struct {
		name      string
		storeType string
		prepare   func()
		wantErr   bool
		clean     func()
	}{
		{
			name:      "Getting catalog from cache in the usual case",
			storeType: "CACHE",
			prepare: func() {
				_ = cacheService.SetCatalog(ctx, catalog)
			},
			wantErr: false,
			clean: func() {
				_ = cacheService.SetCatalog(ctx, nil)
			},
		},
		{
			name:      "Getting catalog from datastore in the usual case",
			storeType: "DB",
			prepare: func() {
				_ = cacheService.SetSdkCatalog(ctx, getSDKs())
				exampleId := utils.GetIDWithDelimiter(pb.Sdk_SDK_JAVA.String(), "MOCK_EXAMPLE")
				saveExample("MOCK_EXAMPLE", pb.Sdk_SDK_JAVA.String())
				saveSnippet(exampleId, pb.Sdk_SDK_JAVA.String())
				savePCObjs(exampleId)
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
			result, err := cacheComponent.GetCatalogFromCacheOrDatastore(ctx, defaultCacheRequestTimeout)
			if (err != nil) != tt.wantErr {
				t.Error("GetCatalogFromCacheOrDatastore() unexpected error")
				return
			}
			switch tt.storeType {
			case "CACHE":
				if !reflect.DeepEqual(result, catalog) {
					t.Error("GetCatalogFromCacheOrDatastore() unexpected result")
				}
				tt.clean()
			case "DB":
				if result[0].GetSdk() != pb.Sdk_SDK_JAVA {
					t.Error("GetCatalogFromCacheOrDatastore() unexpected result: wrong sdk")
				}
				actualCatName := result[0].GetCategories()[0].GetCategoryName()
				actualPCObj := result[0].GetCategories()[0].GetPrecompiledObjects()[0]
				if actualCatName != "MOCK_CATEGORY" {
					t.Error("GetCatalogFromCacheOrDatastore() unexpected result: wrong category")
				}
				if actualPCObj.DefaultExample != false ||
					actualPCObj.Multifile != false ||
					actualPCObj.Name != "MOCK_EXAMPLE" ||
					actualPCObj.Type.String() != "PRECOMPILED_OBJECT_TYPE_EXAMPLE" ||
					actualPCObj.CloudPath != "SDK_JAVA_MOCK_EXAMPLE" ||
					actualPCObj.PipelineOptions != "MOCK_OPTIONS" ||
					actualPCObj.Description != "MOCK_DESCR" ||
					actualPCObj.Link != "MOCK_PATH" ||
					actualPCObj.ContextLine != 32 ||
					actualPCObj.Complexity != pb.Complexity_COMPLEXITY_MEDIUM {
					t.Error("GetCatalogFromCacheOrDatastore() unexpected result: wrong precompiled obj")
				}
				tt.clean()
			}
		})
	}
}

func TestCacheComponent_GetDefaultPrecompiledObjectFromCacheOrDatastore(t *testing.T) {
	pcObj := getPCObj()
	tests := []struct {
		name    string
		prepare func()
		wantErr bool
	}{
		{
			name: "Getting default precompiled object from cache in the usual case",
			prepare: func() {
				_ = cacheService.SetDefaultPrecompiledObject(ctx, pb.Sdk_SDK_JAVA, pcObj)
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepare()
			result, err := cacheComponent.GetDefaultPrecompiledObjectFromCacheOrDatastore(ctx, pb.Sdk_SDK_JAVA, defaultCacheRequestTimeout)
			if (err != nil) != tt.wantErr {
				t.Error("GetDefaultPrecompiledObjectFromCacheOrDatastore() unexpected error")
				return
			}
			if !reflect.DeepEqual(pcObj, result) {
				t.Error("GetDefaultPrecompiledObjectFromCacheOrDatastore() unexpected result")
			}
		})
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

func getPCObj() *pb.PrecompiledObject {
	return &pb.PrecompiledObject{
		CloudPath:       "MOCK_PATH",
		Name:            "MOCK_NAME",
		Description:     "MOCK_DESCR",
		Type:            1,
		PipelineOptions: "MOCK_OPTIONS",
		Link:            "MOCK_LINK",
		Multifile:       false,
		ContextLine:     32,
		DefaultExample:  true,
	}
}

func getCatalog() []*pb.Categories {
	return []*pb.Categories{
		{
			Sdk: pb.Sdk_SDK_JAVA,
			Categories: []*pb.Categories_Category{
				{
					CategoryName: "TestCategory", PrecompiledObjects: []*pb.PrecompiledObject{
						{
							CloudPath:   "SDK_JAVA/TestCategory/TestName.java",
							Name:        "TestName",
							Description: "TestDescription",
							Type:        pb.PrecompiledObjectType_PRECOMPILED_OBJECT_TYPE_EXAMPLE,
						},
					},
				},
				{
					CategoryName: "AnotherTestCategory", PrecompiledObjects: []*pb.PrecompiledObject{
						{
							CloudPath:   "SDK_JAVA/AnotherTestCategory/TestName.java",
							Name:        "TestName",
							Description: "TestDescription",
							Type:        pb.PrecompiledObjectType_PRECOMPILED_OBJECT_TYPE_EXAMPLE,
						},
					},
				},
			},
		},
		{
			Sdk: pb.Sdk_SDK_PYTHON,
			Categories: []*pb.Categories_Category{
				{
					CategoryName: "TestCategory", PrecompiledObjects: []*pb.PrecompiledObject{
						{
							CloudPath:   "SDK_PYTHON/TestCategory/TestName.java",
							Name:        "TestName",
							Description: "TestDescription",
							Type:        pb.PrecompiledObjectType_PRECOMPILED_OBJECT_TYPE_EXAMPLE,
						},
					},
				},
			},
		},
	}
}

func saveExample(name, sdk string) {
	_, _ = datastoreDb.Client.Put(ctx, utils.GetExampleKey(ctx, sdk, name), &entity.ExampleEntity{
		Name:        name,
		Sdk:         utils.GetSdkKey(ctx, sdk),
		Descr:       "MOCK_DESCR",
		Cats:        []string{"MOCK_CATEGORY"},
		Path:        "MOCK_PATH",
		UrlVCS:      "MOCK_URL_VCS",
		UrlNotebook: "MOCK_URL_NOTEBOOK",
		Type:        pb.PrecompiledObjectType_PRECOMPILED_OBJECT_TYPE_EXAMPLE.String(),
		Origin:      constants.ExampleOrigin,
		SchVer:      utils.GetSchemaVerKey(ctx, "MOCK_VERSION"),
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
