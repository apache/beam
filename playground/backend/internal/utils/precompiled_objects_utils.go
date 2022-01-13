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

package utils

import (
	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/cache"
	"beam.apache.org/playground/backend/internal/cloud_bucket"
	"beam.apache.org/playground/backend/internal/errors"
	"beam.apache.org/playground/backend/internal/logger"
	"context"
	"github.com/google/uuid"
)

var (
	ExamplesDataPipelineId = uuid.Nil
)

// PutPrecompiledObjectsToCategory adds categories with precompiled objects to protobuf object
func PutPrecompiledObjectsToCategory(categoryName string, precompiledObjects *cloud_bucket.PrecompiledObjects, sdkCategory *pb.Categories) {
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
		})
	}
	sdkCategory.Categories = append(sdkCategory.Categories, &category)
}

// GetPrecompiledObjectsCatalogFromCache returns the precompiled objects catalog from the cache
func GetPrecompiledObjectsCatalogFromCache(ctx context.Context, cacheService cache.Cache) ([]*pb.Categories, error) {
	value, err := cacheService.GetValue(ctx, ExamplesDataPipelineId, cache.ExamplesCatalog)
	if err != nil {
		logger.Errorf("%s: cache.GetValue: %s\n", ExamplesDataPipelineId, err.Error())
		return nil, err
	}
	catalog, converted := value.([]*pb.Categories)
	if !converted {
		logger.Errorf("%s: couldn't convert value to catalog: %s", cache.ExamplesCatalog, value)
		return nil, errors.InternalError("Error during getting the catalog from cache", "Error during getting catalog")
	}
	return catalog, nil
}

// GetPrecompiledObjectsCatalogFromStorage returns the precompiled objects catalog from the cloud storage
func GetPrecompiledObjectsCatalogFromStorage(ctx context.Context, sdk pb.Sdk, category string) ([]*pb.Categories, error) {
	bucket := cloud_bucket.New()
	sdkToCategories, err := bucket.GetPrecompiledObjects(ctx, sdk, category)
	if err != nil {
		logger.Errorf("GetPrecompiledObjects(): cloud storage error: %s", err.Error())
		return nil, err
	}
	sdkCategories := make([]*pb.Categories, 0)
	for sdkName, categories := range *sdkToCategories {
		sdkCategory := pb.Categories{Sdk: pb.Sdk(pb.Sdk_value[sdkName]), Categories: make([]*pb.Categories_Category, 0)}
		for categoryName, precompiledObjects := range categories {
			PutPrecompiledObjectsToCategory(categoryName, &precompiledObjects, &sdkCategory)
		}
		sdkCategories = append(sdkCategories, &sdkCategory)
	}
	return sdkCategories, nil
}

// GetPrecompiledObjectsCatalogFromStorageToResponse returns the precompiled objects catalog from the cloud storage in the response format
func GetPrecompiledObjectsCatalogFromStorageToResponse(ctx context.Context, sdk pb.Sdk, category string) (*pb.GetPrecompiledObjectsResponse, error) {
	sdkCategories, err := GetPrecompiledObjectsCatalogFromStorage(ctx, sdk, category)
	if err != nil {
		return nil, err
	}
	return &pb.GetPrecompiledObjectsResponse{SdkCategories: sdkCategories}, nil
}
