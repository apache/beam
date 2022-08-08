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

package mapper

import (
	"context"
	"testing"

	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/constants"
	"beam.apache.org/playground/backend/internal/db/dto"
	"beam.apache.org/playground/backend/internal/db/entity"
	"beam.apache.org/playground/backend/internal/utils"
)

var pcObjMapper = NewPrecompiledObjectMapper()
var pcObjMapperCtx = context.Background()

func TestPrecompiledObjectMapper_ToObjectInfo(t *testing.T) {
	actualResult := pcObjMapper.ToObjectInfo(getExampleDTO("MOCK_NAME", "MOCK_DEFAULT_EXAMPLE", pb.Sdk_SDK_JAVA.String()))
	if actualResult.Multifile != false ||
		actualResult.DefaultExample != false ||
		actualResult.Name != "MOCK_NAME" ||
		actualResult.CloudPath != "SDK_JAVA/PRECOMPILED_OBJECT_TYPE_EXAMPLE/MOCK_NAME" ||
		actualResult.Description != "MOCK_DESCR" ||
		actualResult.PipelineOptions != "MOCK_OPTIONS" ||
		actualResult.Link != "MOCK_PATH" ||
		actualResult.ContextLine != 32 ||
		len(actualResult.Categories) != 3 ||
		actualResult.Type.String() != "PRECOMPILED_OBJECT_TYPE_EXAMPLE" {
		t.Error("ToObjectInfo() unexpected result")
	}
}

func TestPrecompiledObjectMapper_ToArrayCategories(t *testing.T) {
	actualResult := pcObjMapper.ToArrayCategories(getCatalogDTO())
	javaCatalog := getCategoryBySdk(actualResult, pb.Sdk_SDK_JAVA)
	if len(javaCatalog.Categories) != 3 ||
		len(javaCatalog.Categories[0].PrecompiledObjects) != 1 ||
		javaCatalog.Categories[0].PrecompiledObjects[0].Name != "SDK_JAVA_MOCK_NAME" ||
		javaCatalog.Categories[0].PrecompiledObjects[0].Description != "MOCK_DESCR" ||
		javaCatalog.Categories[0].PrecompiledObjects[0].CloudPath != "SDK_JAVA/PRECOMPILED_OBJECT_TYPE_EXAMPLE/SDK_JAVA_MOCK_NAME" ||
		javaCatalog.Categories[0].PrecompiledObjects[0].Multifile != false ||
		javaCatalog.Categories[0].PrecompiledObjects[0].DefaultExample != false ||
		javaCatalog.Categories[0].PrecompiledObjects[0].Link != "MOCK_PATH" ||
		javaCatalog.Categories[0].PrecompiledObjects[0].PipelineOptions != "MOCK_OPTIONS" ||
		javaCatalog.Categories[0].PrecompiledObjects[0].ContextLine != 32 ||
		javaCatalog.Categories[0].PrecompiledObjects[0].Type.String() != "PRECOMPILED_OBJECT_TYPE_EXAMPLE" {
		t.Error("ToArrayCategories() unexpected result for Java Catalog")
	}
	goCatalog := getCategoryBySdk(actualResult, pb.Sdk_SDK_GO)
	if len(goCatalog.Categories) != 3 ||
		len(goCatalog.Categories[0].PrecompiledObjects) != 1 ||
		goCatalog.Categories[0].PrecompiledObjects[0].Name != "SDK_GO_MOCK_NAME" ||
		goCatalog.Categories[0].PrecompiledObjects[0].Description != "MOCK_DESCR" ||
		goCatalog.Categories[0].PrecompiledObjects[0].CloudPath != "SDK_GO/PRECOMPILED_OBJECT_TYPE_EXAMPLE/SDK_GO_MOCK_NAME" ||
		goCatalog.Categories[0].PrecompiledObjects[0].Multifile != false ||
		goCatalog.Categories[0].PrecompiledObjects[0].DefaultExample != false ||
		goCatalog.Categories[0].PrecompiledObjects[0].Link != "MOCK_PATH" ||
		goCatalog.Categories[0].PrecompiledObjects[0].PipelineOptions != "MOCK_OPTIONS" ||
		goCatalog.Categories[0].PrecompiledObjects[0].ContextLine != 32 ||
		goCatalog.Categories[0].PrecompiledObjects[0].Type.String() != "PRECOMPILED_OBJECT_TYPE_EXAMPLE" {
		t.Error("ToArrayCategories() unexpected result for Go Catalog")
	}
	pythonCatalog := getCategoryBySdk(actualResult, pb.Sdk_SDK_PYTHON)
	if len(pythonCatalog.Categories) != 3 ||
		len(pythonCatalog.Categories[0].PrecompiledObjects) != 1 ||
		pythonCatalog.Categories[0].PrecompiledObjects[0].Name != "SDK_PYTHON_MOCK_NAME" ||
		pythonCatalog.Categories[0].PrecompiledObjects[0].Description != "MOCK_DESCR" ||
		pythonCatalog.Categories[0].PrecompiledObjects[0].CloudPath != "SDK_PYTHON/PRECOMPILED_OBJECT_TYPE_EXAMPLE/SDK_PYTHON_MOCK_NAME" ||
		pythonCatalog.Categories[0].PrecompiledObjects[0].Multifile != false ||
		pythonCatalog.Categories[0].PrecompiledObjects[0].DefaultExample != false ||
		pythonCatalog.Categories[0].PrecompiledObjects[0].Link != "MOCK_PATH" ||
		pythonCatalog.Categories[0].PrecompiledObjects[0].PipelineOptions != "MOCK_OPTIONS" ||
		pythonCatalog.Categories[0].PrecompiledObjects[0].ContextLine != 32 ||
		pythonCatalog.Categories[0].PrecompiledObjects[0].Type.String() != "PRECOMPILED_OBJECT_TYPE_EXAMPLE" {
		t.Error("ToArrayCategories() unexpected result for Python Catalog")
	}
	scioCatalog := getCategoryBySdk(actualResult, pb.Sdk_SDK_SCIO)
	if len(scioCatalog.Categories) != 3 ||
		len(scioCatalog.Categories[0].PrecompiledObjects) != 1 ||
		scioCatalog.Categories[0].PrecompiledObjects[0].Name != "SDK_SCIO_MOCK_NAME" ||
		scioCatalog.Categories[0].PrecompiledObjects[0].Description != "MOCK_DESCR" ||
		scioCatalog.Categories[0].PrecompiledObjects[0].CloudPath != "SDK_SCIO/PRECOMPILED_OBJECT_TYPE_EXAMPLE/SDK_SCIO_MOCK_NAME" ||
		scioCatalog.Categories[0].PrecompiledObjects[0].Multifile != false ||
		scioCatalog.Categories[0].PrecompiledObjects[0].DefaultExample != false ||
		scioCatalog.Categories[0].PrecompiledObjects[0].Link != "MOCK_PATH" ||
		scioCatalog.Categories[0].PrecompiledObjects[0].PipelineOptions != "MOCK_OPTIONS" ||
		scioCatalog.Categories[0].PrecompiledObjects[0].ContextLine != 32 ||
		scioCatalog.Categories[0].PrecompiledObjects[0].Type.String() != "PRECOMPILED_OBJECT_TYPE_EXAMPLE" {
		t.Error("ToArrayCategories() unexpected result for Scio Catalog")
	}
}

func getCategoryBySdk(catalog []*pb.Categories, sdk pb.Sdk) *pb.Categories {
	for _, categories := range catalog {
		if categories.Sdk == sdk {
			return categories
		}
	}
	return nil
}

func getExampleDTO(name, defaultName, sdk string) *dto.ExampleDTO {
	return &dto.ExampleDTO{
		Example: &entity.ExampleEntity{
			Name:       name,
			Sdk:        utils.GetSdkKey(pcObjMapperCtx, sdk),
			Descr:      "MOCK_DESCR",
			Cats:       []string{"MOCK_CAT_1", "MOCK_CAT_2", "MOCK_CAT_3"},
			Complexity: "MEDIUM",
			Path:       "MOCK_PATH",
			Type:       "PRECOMPILED_OBJECT_TYPE_EXAMPLE",
			Origin:     constants.ExampleOrigin,
			SchVer:     utils.GetSchemaVerKey(pcObjMapperCtx, "MOCK_VERSION"),
		},
		Snippet: &entity.SnippetEntity{
			Sdk:           utils.GetSdkKey(pcObjMapperCtx, sdk),
			PipeOpts:      "MOCK_OPTIONS",
			Origin:        constants.ExampleOrigin,
			SchVer:        utils.GetSchemaVerKey(pcObjMapperCtx, "MOCK_VERSION"),
			NumberOfFiles: 1,
		},
		Files: []*entity.FileEntity{{
			Name:     "MOCK_NAME",
			Content:  "MOCK_CONTENT",
			CntxLine: 32,
			IsMain:   true,
		}},
		DefaultExampleName: defaultName,
	}
}

func getCatalogDTO() *dto.CatalogDTO {
	sdks := getSDKs()
	examples := make([]*entity.ExampleEntity, 0, 4)
	snippets := make([]*entity.SnippetEntity, 0, 4)
	files := make([]*entity.FileEntity, 0, 4)
	for _, sdk := range sdks {
		if sdk.Name == pb.Sdk_SDK_UNSPECIFIED.String() {
			continue
		}
		exampleDTO := getExampleDTO(utils.GetIDWithDelimiter(sdk.Name, "MOCK_NAME"), "MOCK_DEFAULT_EXAMPLE", sdk.Name)
		examples = append(examples, exampleDTO.Example)
		snippets = append(snippets, exampleDTO.Snippet)
		files = append(files, exampleDTO.Files[0])
	}
	return &dto.CatalogDTO{
		Examples:   examples,
		Snippets:   snippets,
		Files:      files,
		SdkCatalog: sdks,
	}
}

func getSDKs() []*entity.SDKEntity {
	var sdkEntities []*entity.SDKEntity
	for _, sdk := range pb.Sdk_name {
		sdkEntities = append(sdkEntities, &entity.SDKEntity{
			Name:           sdk,
			DefaultExample: "MOCK_DEFAULT_EXAMPLE",
		})
	}
	return sdkEntities
}
