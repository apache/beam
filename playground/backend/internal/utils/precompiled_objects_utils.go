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
