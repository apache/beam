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
	"github.com/stretchr/testify/assert"
)

var pcObjMapper = NewPrecompiledObjectMapper()
var pcObjMapperCtx = context.Background()

func TestPrecompiledObjectMapper_ToObjectInfo(t *testing.T) {
	expected := &dto.ObjectInfo{
		Name:            "MOCK_NAME",
		CloudPath:       "SDK_JAVA_MOCK_NAME",
		Description:     "MOCK_DESCR",
		Type:            pb.PrecompiledObjectType_PRECOMPILED_OBJECT_TYPE_EXAMPLE,
		Complexity:      pb.Complexity_COMPLEXITY_MEDIUM,
		Categories:      []string{"MOCK_CAT_1", "MOCK_CAT_2", "MOCK_CAT_3"},
		PipelineOptions: "MOCK_OPTIONS",
		Link:            "MOCK_PATH",
		Multifile:       true,
		DefaultExample:  false,
		ContextLine:     32,
		Sdk:             pb.Sdk_SDK_JAVA,
		Datasets: []*pb.Dataset{
			{
				Type:        pb.EmulatorType_EMULATOR_TYPE_KAFKA,
				Options:     map[string]string{"Topic": "MOCK_TOPIC"},
				DatasetPath: "MOCK_PATH_0",
			},
		},
	}
	actualResult := pcObjMapper.ToObjectInfo(
		getExampleDTO("MOCK_NAME", "MOCK_DEFAULT_EXAMPLE", pb.Sdk_SDK_JAVA.String()),
	)
	assert.Equal(t, expected, actualResult)
}

func TestPrecompiledObjectMapper_ToPrecompiledObj(t *testing.T) {
	expected := &pb.PrecompiledObject{
		Name:            "MOCK_NAME",
		CloudPath:       "SDK_JAVA_MOCK_NAME",
		Description:     "MOCK_DESCR",
		Type:            pb.PrecompiledObjectType_PRECOMPILED_OBJECT_TYPE_EXAMPLE,
		Complexity:      pb.Complexity_COMPLEXITY_MEDIUM,
		PipelineOptions: "MOCK_OPTIONS",
		Link:            "MOCK_PATH",
		Multifile:       true,
		DefaultExample:  false,
		ContextLine:     32,
		Sdk:             pb.Sdk_SDK_JAVA,
		Datasets: []*pb.Dataset{
			{
				Type:        pb.EmulatorType_EMULATOR_TYPE_KAFKA,
				Options:     map[string]string{"Topic": "MOCK_TOPIC"},
				DatasetPath: "MOCK_PATH_0",
			},
		},
	}
	actualResult := pcObjMapper.ToPrecompiledObj("SDK_JAVA_MOCK_NAME",
		getExampleDTO("MOCK_NAME", "MOCK_DEFAULT_EXAMPLE", pb.Sdk_SDK_JAVA.String()),
	)
	assert.Equal(t, expected, actualResult)
}

func TestPrecompiledObjectMapper_ToDefaultPrecompiledObjects(t *testing.T) {
	actualResult := pcObjMapper.ToDefaultPrecompiledObjects(getDefaultExamplesDTO())
	expected := map[pb.Sdk]*pb.PrecompiledObject{
		pb.Sdk_SDK_JAVA: {
			DefaultExample:  true,
			Complexity:      pb.Complexity_COMPLEXITY_MEDIUM,
			Name:            "1_MOCK_DEFAULT_EXAMPLE",
			Multifile:       false,
			Type:            pb.PrecompiledObjectType_PRECOMPILED_OBJECT_TYPE_EXAMPLE,
			ContextLine:     32,
			Link:            "MOCK_PATH",
			Description:     "MOCK_DESCR",
			PipelineOptions: "MOCK_OPTIONS",
			CloudPath:       "SDK_JAVA_1_MOCK_DEFAULT_EXAMPLE",
			Sdk:             pb.Sdk_SDK_JAVA,
		},
		pb.Sdk_SDK_GO: {
			DefaultExample:  true,
			Complexity:      pb.Complexity_COMPLEXITY_MEDIUM,
			Name:            "2_MOCK_DEFAULT_EXAMPLE",
			Multifile:       false,
			Type:            pb.PrecompiledObjectType_PRECOMPILED_OBJECT_TYPE_EXAMPLE,
			ContextLine:     32,
			Link:            "MOCK_PATH",
			Description:     "MOCK_DESCR",
			PipelineOptions: "MOCK_OPTIONS",
			CloudPath:       "SDK_GO_2_MOCK_DEFAULT_EXAMPLE",
			Sdk:             pb.Sdk_SDK_GO,
		},
		pb.Sdk_SDK_PYTHON: {
			DefaultExample:  true,
			Complexity:      pb.Complexity_COMPLEXITY_MEDIUM,
			Name:            "3_MOCK_DEFAULT_EXAMPLE",
			Multifile:       false,
			Type:            pb.PrecompiledObjectType_PRECOMPILED_OBJECT_TYPE_EXAMPLE,
			ContextLine:     32,
			Link:            "MOCK_PATH",
			Description:     "MOCK_DESCR",
			PipelineOptions: "MOCK_OPTIONS",
			CloudPath:       "SDK_PYTHON_3_MOCK_DEFAULT_EXAMPLE",
			Sdk:             pb.Sdk_SDK_PYTHON,
		},
		pb.Sdk_SDK_SCIO: {
			DefaultExample:  true,
			Complexity:      pb.Complexity_COMPLEXITY_MEDIUM,
			Name:            "4_MOCK_DEFAULT_EXAMPLE",
			Multifile:       false,
			Type:            pb.PrecompiledObjectType_PRECOMPILED_OBJECT_TYPE_EXAMPLE,
			ContextLine:     32,
			Link:            "MOCK_PATH",
			Description:     "MOCK_DESCR",
			PipelineOptions: "MOCK_OPTIONS",
			CloudPath:       "SDK_SCIO_4_MOCK_DEFAULT_EXAMPLE",
			Sdk:             pb.Sdk_SDK_SCIO,
		},
	}
	assert.Equal(t, expected, actualResult)
}

func TestPrecompiledObjectMapper_ToArrayCategories(t *testing.T) {
	actualResult := pcObjMapper.ToArrayCategories(getCatalogDTO())
	javaCatalog := getCategoryBySdk(actualResult, pb.Sdk_SDK_JAVA)
	if len(javaCatalog.Categories) != 3 ||
		len(javaCatalog.Categories[0].PrecompiledObjects) != 1 ||
		javaCatalog.Categories[0].PrecompiledObjects[0].Name != "SDK_JAVA_MOCK_NAME" ||
		javaCatalog.Categories[0].PrecompiledObjects[0].Description != "MOCK_DESCR" ||
		javaCatalog.Categories[0].PrecompiledObjects[0].CloudPath != "SDK_JAVA_SDK_JAVA_MOCK_NAME" ||
		javaCatalog.Categories[0].PrecompiledObjects[0].Multifile != true ||
		javaCatalog.Categories[0].PrecompiledObjects[0].DefaultExample != false ||
		javaCatalog.Categories[0].PrecompiledObjects[0].Link != "MOCK_PATH" ||
		javaCatalog.Categories[0].PrecompiledObjects[0].PipelineOptions != "MOCK_OPTIONS" ||
		javaCatalog.Categories[0].PrecompiledObjects[0].ContextLine != 32 ||
		javaCatalog.Categories[0].PrecompiledObjects[0].Type.String() != "PRECOMPILED_OBJECT_TYPE_EXAMPLE" ||
		javaCatalog.Categories[0].PrecompiledObjects[0].Sdk != pb.Sdk_SDK_JAVA {
		t.Error("ToArrayCategories() unexpected result for Java Catalog")
	}
	goCatalog := getCategoryBySdk(actualResult, pb.Sdk_SDK_GO)
	if len(goCatalog.Categories) != 3 ||
		len(goCatalog.Categories[0].PrecompiledObjects) != 1 ||
		goCatalog.Categories[0].PrecompiledObjects[0].Name != "SDK_GO_MOCK_NAME" ||
		goCatalog.Categories[0].PrecompiledObjects[0].Description != "MOCK_DESCR" ||
		goCatalog.Categories[0].PrecompiledObjects[0].CloudPath != "SDK_GO_SDK_GO_MOCK_NAME" ||
		goCatalog.Categories[0].PrecompiledObjects[0].Multifile != true ||
		goCatalog.Categories[0].PrecompiledObjects[0].DefaultExample != false ||
		goCatalog.Categories[0].PrecompiledObjects[0].Link != "MOCK_PATH" ||
		goCatalog.Categories[0].PrecompiledObjects[0].PipelineOptions != "MOCK_OPTIONS" ||
		goCatalog.Categories[0].PrecompiledObjects[0].ContextLine != 32 ||
		goCatalog.Categories[0].PrecompiledObjects[0].Type.String() != "PRECOMPILED_OBJECT_TYPE_EXAMPLE" ||
		goCatalog.Categories[0].PrecompiledObjects[0].Sdk != pb.Sdk_SDK_GO {
		t.Error("ToArrayCategories() unexpected result for Go Catalog")
	}
	pythonCatalog := getCategoryBySdk(actualResult, pb.Sdk_SDK_PYTHON)
	if len(pythonCatalog.Categories) != 3 ||
		len(pythonCatalog.Categories[0].PrecompiledObjects) != 1 ||
		pythonCatalog.Categories[0].PrecompiledObjects[0].Name != "SDK_PYTHON_MOCK_NAME" ||
		pythonCatalog.Categories[0].PrecompiledObjects[0].Description != "MOCK_DESCR" ||
		pythonCatalog.Categories[0].PrecompiledObjects[0].CloudPath != "SDK_PYTHON_SDK_PYTHON_MOCK_NAME" ||
		pythonCatalog.Categories[0].PrecompiledObjects[0].Multifile != true ||
		pythonCatalog.Categories[0].PrecompiledObjects[0].DefaultExample != false ||
		pythonCatalog.Categories[0].PrecompiledObjects[0].Link != "MOCK_PATH" ||
		pythonCatalog.Categories[0].PrecompiledObjects[0].PipelineOptions != "MOCK_OPTIONS" ||
		pythonCatalog.Categories[0].PrecompiledObjects[0].ContextLine != 32 ||
		pythonCatalog.Categories[0].PrecompiledObjects[0].Type.String() != "PRECOMPILED_OBJECT_TYPE_EXAMPLE" ||
		pythonCatalog.Categories[0].PrecompiledObjects[0].Sdk != pb.Sdk_SDK_PYTHON {
		t.Error("ToArrayCategories() unexpected result for Python Catalog")
	}
	scioCatalog := getCategoryBySdk(actualResult, pb.Sdk_SDK_SCIO)
	if len(scioCatalog.Categories) != 3 ||
		len(scioCatalog.Categories[0].PrecompiledObjects) != 1 ||
		scioCatalog.Categories[0].PrecompiledObjects[0].Name != "SDK_SCIO_MOCK_NAME" ||
		scioCatalog.Categories[0].PrecompiledObjects[0].Description != "MOCK_DESCR" ||
		scioCatalog.Categories[0].PrecompiledObjects[0].CloudPath != "SDK_SCIO_SDK_SCIO_MOCK_NAME" ||
		scioCatalog.Categories[0].PrecompiledObjects[0].Multifile != true ||
		scioCatalog.Categories[0].PrecompiledObjects[0].DefaultExample != false ||
		scioCatalog.Categories[0].PrecompiledObjects[0].Link != "MOCK_PATH" ||
		scioCatalog.Categories[0].PrecompiledObjects[0].PipelineOptions != "MOCK_OPTIONS" ||
		scioCatalog.Categories[0].PrecompiledObjects[0].ContextLine != 32 ||
		scioCatalog.Categories[0].PrecompiledObjects[0].Type.String() != "PRECOMPILED_OBJECT_TYPE_EXAMPLE" ||
		scioCatalog.Categories[0].PrecompiledObjects[0].Sdk != pb.Sdk_SDK_SCIO {
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
			Name:   name,
			Sdk:    utils.GetSdkKey(pcObjMapperCtx, sdk),
			Descr:  "MOCK_DESCR",
			Cats:   []string{"MOCK_CAT_1", "MOCK_CAT_2", "MOCK_CAT_3"},
			Path:   "MOCK_PATH",
			Type:   pb.PrecompiledObjectType_PRECOMPILED_OBJECT_TYPE_EXAMPLE.String(),
			Origin: constants.ExampleOrigin,
			SchVer: utils.GetSchemaVerKey(pcObjMapperCtx, "MOCK_VERSION"),
		},
		Snippet: &entity.SnippetEntity{
			Sdk:           utils.GetSdkKey(pcObjMapperCtx, sdk),
			PipeOpts:      "MOCK_OPTIONS",
			Origin:        constants.ExampleOrigin,
			SchVer:        utils.GetSchemaVerKey(pcObjMapperCtx, "MOCK_VERSION"),
			NumberOfFiles: 2,
			Complexity:    pb.Complexity_COMPLEXITY_MEDIUM.String(),
		},
		Files: []*entity.FileEntity{
			{
				Name:     "MOCK_NAME_0",
				Content:  "MOCK_CONTENT",
				CntxLine: 32,
				IsMain:   true,
			},
			{
				Name:     "MOCK_NAME_1",
				Content:  "MOCK_CONTENT",
				CntxLine: 16,
				IsMain:   false,
			},
		},
		DefaultExampleName: defaultName,
		Datasets: []*dto.DatasetDTO{
			{
				Path: "MOCK_PATH_0",
				Config: map[string]string{
					"Topic": "MOCK_TOPIC",
				},
				Emulator: pb.EmulatorType_EMULATOR_TYPE_KAFKA,
			},
		},
	}
}

func getDefaultExamplesDTO() *dto.DefaultExamplesDTO {
	examples := make([]*entity.ExampleEntity, 0, 4)
	snippets := make([]*entity.SnippetEntity, 0, 4)
	files := make([]*entity.FileEntity, 0, 4)
	for sdk, sdkNum := range pb.Sdk_value {
		if sdk == pb.Sdk_SDK_UNSPECIFIED.String() {
			continue
		}
		exampleDTO := getExampleDTO(utils.GetIDWithDelimiter(sdkNum, "MOCK_DEFAULT_EXAMPLE"), "MOCK_DEFAULT_EXAMPLE", sdk)
		examples = append(examples, exampleDTO.Example)
		snippets = append(snippets, exampleDTO.Snippet)
		files = append(files, exampleDTO.Files[0])
	}
	return &dto.DefaultExamplesDTO{
		Examples: examples,
		Snippets: snippets,
		Files:    files,
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
		files = append(files, exampleDTO.Files...)
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
