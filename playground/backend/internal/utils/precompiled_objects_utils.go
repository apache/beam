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
)

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
