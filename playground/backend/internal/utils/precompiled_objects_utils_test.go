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
	"context"
	"github.com/google/uuid"
	"reflect"
	"testing"
)

func TestPutPrecompiledObjectsToCategory(t *testing.T) {
	precompiledObjectToAdd := &cloud_bucket.PrecompiledObjects{
		{"TestName", "SDK_JAVA/TestCategory/TestName.java", "TestDescription", pb.PrecompiledObjectType_PRECOMPILED_OBJECT_TYPE_EXAMPLE, []string{""}, ""},
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

func TestGetPrecompiledObjectsCatalogFromCache(t *testing.T) {
	ctx := context.Background()
	sdkCategories := []*pb.Categories{
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
		}}
	type args struct {
		ctx          context.Context
		cacheService cache.Cache
	}
	tests := []struct {
		name     string
		args     args
		prepFunc func(cacheService cache.Cache) error
		want     []*pb.Categories
		wantErr  bool
	}{
		{
			// Test case with getting Precompiled Objects Catalog from cache when it exists.
			// As a result, want to receive an expected catalog from cache.
			name: "get existing catalog",
			args: args{
				ctx:          ctx,
				cacheService: local.New(ctx),
			},
			prepFunc: func(cacheService cache.Cache) error {
				err := cacheService.SetValue(ctx, uuid.Nil, cache.ExamplesCatalog, sdkCategories)
				if err != nil {
					return err
				}
				return nil
			},
			want:    sdkCategories,
			wantErr: false,
		},
		{
			// Test case with getting Precompiled Objects Catalog from cache when it doesn't exist.
			// As a result, want to receive an error.
			name: "get missing catalog",
			args: args{
				ctx:          ctx,
				cacheService: local.New(ctx),
			},
			prepFunc: func(cacheService cache.Cache) error {
				return nil
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.prepFunc(tt.args.cacheService); err != nil {
				t.Errorf("prepFunc() error = %v, wantErr %v", err, tt.wantErr)
			}
			got, err := GetPrecompiledObjectsCatalogFromCache(tt.args.ctx, tt.args.cacheService)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetPrecompiledObjectsCatalogFromCache() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetPrecompiledObjectsCatalogFromCache() got = %v, want %v", got, tt.want)
			}
		})
	}
}
