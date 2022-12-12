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
	"encoding/json"
	"fmt"
	"strings"

	"beam.apache.org/playground/backend/internal/db/dto"

	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/db/entity"
)

type PrecompiledObjectMapper struct {
}

func NewPrecompiledObjectMapper() *PrecompiledObjectMapper {
	return &PrecompiledObjectMapper{}
}

func (pom *PrecompiledObjectMapper) ToObjectInfo(exampleDTO *dto.ExampleDTO) *dto.ObjectInfo {
	return &dto.ObjectInfo{
		Name:            exampleDTO.Example.Name,
		CloudPath:       getCloudPath(exampleDTO.Example),
		Description:     exampleDTO.Example.Descr,
		Type:            exampleDTO.GetType(),
		Categories:      exampleDTO.Example.Cats,
		PipelineOptions: exampleDTO.Snippet.PipeOpts,
		Link:            exampleDTO.Example.Path,
		Multifile:       exampleDTO.HasMultiFiles(),
		ContextLine:     exampleDTO.GetContextLine(),
		DefaultExample:  exampleDTO.IsDefault(),
		Sdk:             exampleDTO.GetSDK(),
		Complexity:      exampleDTO.GetComplexity(),
		Tags:            exampleDTO.Example.Tags,
		Datasets:        exampleDTO.GetDatasets(),
	}
}

func (pom *PrecompiledObjectMapper) ToArrayCategories(catalogDTO *dto.CatalogDTO) []*pb.Categories {
	sdkToDefaultExample := catalogDTO.GetSdkCatalogAsMap()
	numberOfExamples := len(catalogDTO.Examples)
	sdkToCategories := make(dto.SdkToCategories, 0)
	datasetBySnippetIDMap := catalogDTO.DatasetBySnippetIDMap
	for exampleIndx := 0; exampleIndx < numberOfExamples; exampleIndx++ {
		example := catalogDTO.Examples[exampleIndx]
		snippet := catalogDTO.Snippets[exampleIndx]
		files := []*entity.FileEntity{catalogDTO.Files[exampleIndx]}
		var datasetsDTO []*dto.DatasetDTO
		if len(datasetBySnippetIDMap) != 0 {
			datasetsDTO = datasetBySnippetIDMap[snippet.Key.Name]
		}
		objInfo := pom.ToObjectInfo(&dto.ExampleDTO{
			Example:            example,
			Snippet:            snippet,
			Files:              files,
			DefaultExampleName: sdkToDefaultExample[example.Sdk.Name],
			Datasets:           datasetsDTO,
		})
		for _, objCategory := range objInfo.Categories {
			appendPrecompiledObject(*objInfo, &sdkToCategories, objCategory, example.Sdk.Name)
		}
	}
	sdkCategories := make([]*pb.Categories, 0)
	for sdkName, categories := range sdkToCategories {
		sdkCategory := pb.Categories{Sdk: pb.Sdk(pb.Sdk_value[sdkName]), Categories: make([]*pb.Categories_Category, 0)}
		for categoryName, precompiledObjects := range categories {
			putPrecompiledObjectsToCategory(categoryName, &precompiledObjects, &sdkCategory)
		}
		sdkCategories = append(sdkCategories, &sdkCategory)
	}

	return sdkCategories
}

func (pom *PrecompiledObjectMapper) ToDefaultPrecompiledObjects(defaultExamplesDTO *dto.DefaultExamplesDTO) map[pb.Sdk]*pb.PrecompiledObject {
	result := make(map[pb.Sdk]*pb.PrecompiledObject)
	for exampleIndx, example := range defaultExamplesDTO.Examples {
		result[pb.Sdk(pb.Sdk_value[example.Sdk.Name])] = &pb.PrecompiledObject{
			CloudPath:       getCloudPath(example),
			Name:            example.Name,
			Description:     example.Descr,
			Type:            pb.PrecompiledObjectType(pb.PrecompiledObjectType_value[example.Type]),
			PipelineOptions: defaultExamplesDTO.Snippets[exampleIndx].PipeOpts,
			Link:            example.Path,
			Multifile:       false,
			ContextLine:     defaultExamplesDTO.Files[exampleIndx].CntxLine,
			DefaultExample:  true,
			Sdk:             pb.Sdk(pb.Sdk_value[example.Sdk.Name]),
			Tags:            example.Tags,
			Complexity:      pb.Complexity(pb.Complexity_value[defaultExamplesDTO.Snippets[exampleIndx].Complexity]),
		}
	}
	return result
}

func (pom *PrecompiledObjectMapper) ToPrecompiledObj(exampleDTO *dto.ExampleDTO) *pb.PrecompiledObject {
	return &pb.PrecompiledObject{
		CloudPath:       getCloudPath(exampleDTO.Example),
		Name:            exampleDTO.Example.Name,
		Description:     exampleDTO.Example.Descr,
		Type:            exampleDTO.GetType(),
		PipelineOptions: exampleDTO.Snippet.PipeOpts,
		Link:            exampleDTO.Example.Path,
		Multifile:       exampleDTO.HasMultiFiles(),
		ContextLine:     exampleDTO.GetContextLine(),
		DefaultExample:  exampleDTO.IsDefault(),
		Sdk:             exampleDTO.GetSDK(),
		Complexity:      exampleDTO.GetComplexity(),
		Tags:            exampleDTO.Example.Tags,
		Datasets:        exampleDTO.GetDatasets(),
	}
}

func (pom *PrecompiledObjectMapper) ToDatasetBySnippetIDMap(datasetEntities []*entity.DatasetEntity, snippets []*entity.SnippetEntity) (map[string][]*dto.DatasetDTO, error) {
	result := make(map[string][]*dto.DatasetDTO)
	datasetsMap := make(map[string]*entity.DatasetEntity)
	for _, dataset := range datasetEntities {
		datasetsMap[dataset.Key.Name] = dataset
	}
	for _, snippet := range snippets {
		if len(snippet.Datasets) != 0 {
			key := snippet.Key.Name
			datasets, ok := result[key]
			if !ok {
				newDatasets := make([]*dto.DatasetDTO, 0)
				datasetDto, err := toDatasetDTO(datasetsMap, snippet)
				if err != nil {
					return nil, err
				}
				newDatasets = append(newDatasets, datasetDto)
				result[key] = newDatasets
			} else {
				datasetDto, err := toDatasetDTO(datasetsMap, snippet)
				if err != nil {
					return nil, err
				}
				datasets = append(datasets, datasetDto)
			}
		}
	}
	return result, nil
}

func toDatasetDTO(datasetsMap map[string]*entity.DatasetEntity, snippet *entity.SnippetEntity) (*dto.DatasetDTO, error) {
	var configInterface map[string]interface{}
	if err := json.Unmarshal([]byte(snippet.Datasets[0].Config), &configInterface); err != nil {
		return nil, err
	}
	configString := make(map[string]string, len(configInterface))
	for k, v := range configInterface {
		strK := fmt.Sprintf("%v", k)
		strV := fmt.Sprintf("%v", v)
		configString[strK] = strV
	}
	return &dto.DatasetDTO{
		Path:     datasetsMap[snippet.Datasets[0].Dataset.Name].Path,
		Config:   configString,
		Emulator: pb.EmulatorType(pb.EmulatorType_value[fmt.Sprintf("EMULATOR_TYPE_%s", strings.ToUpper(snippet.Datasets[0].Emulator))]),
	}, nil
}

// appendPrecompiledObject add precompiled object to the common structure of precompiled objects
func appendPrecompiledObject(objectInfo dto.ObjectInfo, sdkToCategories *dto.SdkToCategories, categoryName string, sdk string) {
	categoryToPrecompiledObjects, ok := (*sdkToCategories)[sdk]
	if !ok {
		(*sdkToCategories)[sdk] = make(dto.CategoryToPrecompiledObjects, 0)
		categoryToPrecompiledObjects = (*sdkToCategories)[sdk]
	}
	objects, ok := categoryToPrecompiledObjects[categoryName]
	if !ok {
		categoryToPrecompiledObjects[categoryName] = make(dto.PrecompiledObjects, 0)
		objects = categoryToPrecompiledObjects[categoryName]
	}
	categoryToPrecompiledObjects[categoryName] = append(objects, objectInfo)
}

// putPrecompiledObjectsToCategory adds categories with precompiled objects to protobuf object
func putPrecompiledObjectsToCategory(categoryName string, precompiledObjects *dto.PrecompiledObjects, sdkCategory *pb.Categories) {
	category := pb.Categories_Category{
		CategoryName:       categoryName,
		PrecompiledObjects: make([]*pb.PrecompiledObject, 0),
	}
	for _, object := range *precompiledObjects {
		category.PrecompiledObjects = append(category.PrecompiledObjects, &pb.PrecompiledObject{
			CloudPath:       object.CloudPath,
			Name:            object.Name,
			Description:     object.Description,
			Type:            object.Type,
			PipelineOptions: object.PipelineOptions,
			Link:            object.Link,
			Multifile:       object.Multifile,
			ContextLine:     object.ContextLine,
			DefaultExample:  object.DefaultExample,
			Sdk:             object.Sdk,
			Complexity:      object.Complexity,
			Tags:            object.Tags,
			Datasets:        object.Datasets,
		})
	}
	sdkCategory.Categories = append(sdkCategory.Categories, &category)
}

// getCloudPath returns the cloud path by example entity
func getCloudPath(example *entity.ExampleEntity) string {
	return fmt.Sprintf("%s/%s/%s", example.Sdk.Name, example.Type, example.Name)
}
