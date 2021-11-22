package utils

import (
	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/cloud_bucket"
	"reflect"
	"testing"
)

func TestPutPrecompiledObjectsToCategory(t *testing.T) {
	precompiledObjectToAdd := &cloud_bucket.PrecompiledObjects{
		{"TestName", "SDK_JAVA/TestCategory/TestName.java", "TestDescription", pb.PrecompiledObjectType_PRECOMPILED_OBJECT_TYPE_EXAMPLE, []string{""}},
	}
	type args struct {
		categoryName       string
		precompiledObjects *cloud_bucket.PrecompiledObjects
		sdkCategory        *pb.Categories
	}
	tests := []struct {
		name string
		args args
		want *pb.Categories
	}{
		{
			name: "Test PutPrecompiledObjectsToCategory",
			args: args{
				categoryName:       "TestCategory",
				precompiledObjects: precompiledObjectToAdd,
				sdkCategory: &pb.Categories{
					Sdk:        pb.Sdk_SDK_JAVA,
					Categories: []*pb.Categories_Category{},
				},
			},
			want: &pb.Categories{
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
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			PutPrecompiledObjectsToCategory(tt.args.categoryName, tt.args.precompiledObjects, tt.args.sdkCategory)
			got := tt.args.sdkCategory
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("PutPrecompiledObjectsToCategory() got = %v, want %v", got, tt.want)
			}
		})
	}
}
