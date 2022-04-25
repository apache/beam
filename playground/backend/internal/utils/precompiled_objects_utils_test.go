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
	"beam.apache.org/playground/backend/internal/cache/local"
	"beam.apache.org/playground/backend/internal/cloud_bucket"
	"beam.apache.org/playground/backend/internal/logger"
	"context"
	"reflect"
	"testing"
)

func TestPutPrecompiledObjectsToCategory(t *testing.T) {
	precompiledObjectToAdd := &cloud_bucket.PrecompiledObjects{
		{"TestName", "SDK_JAVA/TestCategory/TestName.java", "TestDescription", pb.PrecompiledObjectType_PRECOMPILED_OBJECT_TYPE_EXAMPLE, []string{""}, "", "", false, 1, false},
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
								ContextLine: 1,
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

func TestFilterPrecompiledObjects(t *testing.T) {
	catalog := []*pb.Categories{
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
	catalogWithSpecificCategory := []*pb.Categories{
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
	type args struct {
		catalog      []*pb.Categories
		sdk          pb.Sdk
		categoryName string
	}
	tests := []struct {
		name string
		args args
		want []*pb.Categories
	}{
		{
			name: "All catalog",
			args: args{
				catalog:      catalog,
				sdk:          pb.Sdk_SDK_UNSPECIFIED,
				categoryName: "",
			},
			want: catalog,
		},
		{
			name: "Specific SDK",
			args: args{
				catalog:      catalog,
				sdk:          pb.Sdk_SDK_JAVA,
				categoryName: "",
			},
			want: catalog[:1],
		},
		{
			name: "Specific Category",
			args: args{
				catalog:      catalog,
				sdk:          pb.Sdk_SDK_UNSPECIFIED,
				categoryName: "TestCategory",
			},
			want: catalogWithSpecificCategory,
		},
		{
			name: "Specific SDK and Category",
			args: args{
				catalog:      catalog,
				sdk:          pb.Sdk_SDK_JAVA,
				categoryName: "TestCategory",
			},
			want: catalogWithSpecificCategory[:1],
		},
		{
			name: "Category not in catalog",
			args: args{
				catalog:      catalog,
				sdk:          pb.Sdk_SDK_JAVA,
				categoryName: "Category1",
			},
			want: catalog[:1],
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FilterCatalog(tt.args.catalog, tt.args.sdk, tt.args.categoryName); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("FilterCatalog() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetDefaultPrecompiledObject(t *testing.T) {
	ctx := context.Background()
	cacheService := local.New(ctx)
	defaultPrecompiledObject := &pb.PrecompiledObject{
		CloudPath:       "cloudPath",
		Name:            "Name",
		Description:     "Description",
		Type:            pb.PrecompiledObjectType_PRECOMPILED_OBJECT_TYPE_EXAMPLE,
		PipelineOptions: "--key value",
		Link:            "Link",
		ContextLine:     1,
		DefaultExample:  true,
	}
	err := cacheService.SetDefaultPrecompiledObject(ctx, pb.Sdk_SDK_JAVA, defaultPrecompiledObject)
	if err != nil {
		logger.Errorf("Error during set up test")
	}

	type args struct {
		ctx          context.Context
		sdk          pb.Sdk
		cacheService cache.Cache
	}
	tests := []struct {
		name    string
		args    args
		want    *pb.PrecompiledObject
		wantErr bool
	}{
		{
			name: "there is default precompiled object",
			args: args{
				ctx:          ctx,
				sdk:          pb.Sdk_SDK_JAVA,
				cacheService: cacheService,
			},
			want:    defaultPrecompiledObject,
			wantErr: false,
		},
		{
			name: "there is no default precompiled object",
			args: args{
				ctx:          ctx,
				sdk:          pb.Sdk_SDK_UNSPECIFIED,
				cacheService: cacheService,
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetDefaultPrecompiledObject(tt.args.ctx, tt.args.sdk, tt.args.cacheService, "")
			if (err != nil) != tt.wantErr {
				t.Errorf("GetDefaultPrecompiledObject() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetDefaultPrecompiledObject() got = %v, want %v", got, tt.want)
			}
		})
	}
}
