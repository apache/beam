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
	"beam.apache.org/playground/backend/internal/cloud_bucket"
	"beam.apache.org/playground/backend/internal/logger"
	"context"
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
			Link:            object.Link,
		})
	}
	sdkCategory.Categories = append(sdkCategory.Categories, &category)
}

// GetCatalogFromStorage returns the precompiled objects catalog from the cloud storage
func GetCatalogFromStorage(ctx context.Context) ([]*pb.Categories, error) {
	bucket := cloud_bucket.New()
	sdkToCategories, err := bucket.GetPrecompiledObjects(ctx, pb.Sdk_SDK_UNSPECIFIED, "")
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

// FilterCatalog returns the catalog filtered by sdk and categoryName
func FilterCatalog(catalog []*pb.Categories, sdk pb.Sdk, categoryName string) []*pb.Categories {
	var result []*pb.Categories
	if sdk == pb.Sdk_SDK_UNSPECIFIED {
		result = catalog
	} else {
		for _, categoriesSdk := range catalog {
			if categoriesSdk.Sdk == sdk {
				result = append(result, categoriesSdk)
				break
			}
		}
	}
	if categoryName == "" {
		return result
	}
	for _, categoriesSdk := range result {
		foundCategory := false
		for _, category := range categoriesSdk.Categories {
			if category.CategoryName == categoryName {
				categoriesSdk.Categories = []*pb.Categories_Category{category}
				foundCategory = true
				break
			}
		}
		if !foundCategory {
			categoriesSdk.Categories = []*pb.Categories_Category{}
		}
	}
	return result
}
