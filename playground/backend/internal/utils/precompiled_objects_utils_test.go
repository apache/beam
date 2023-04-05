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
	"reflect"
	"testing"

	pb "beam.apache.org/playground/backend/internal/api/v1"
)

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
