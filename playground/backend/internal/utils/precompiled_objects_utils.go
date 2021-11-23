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
)

// PutPrecompiledObjectsToCategory adds categories with precompiled objects to protobuf object
func PutPrecompiledObjectsToCategory(categoryName string, precompiledObjects *cloud_bucket.PrecompiledObjects, sdkCategory *pb.Categories) {
	category := pb.Categories_Category{
		CategoryName:       categoryName,
		PrecompiledObjects: make([]*pb.PrecompiledObject, 0),
	}
	for _, object := range *precompiledObjects {
		category.PrecompiledObjects = append(category.PrecompiledObjects, &pb.PrecompiledObject{
			CloudPath:   object.CloudPath,
			Name:        object.Name,
			Description: object.Description,
			Type:        object.Type,
		})
	}
	sdkCategory.Categories = append(sdkCategory.Categories, &category)
}
