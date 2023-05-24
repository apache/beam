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

package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/go-redis/redismock/v8"
	"github.com/google/uuid"

	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/cache"
	"beam.apache.org/playground/backend/internal/db/entity"
)

func TestRedisCache_GetValue(t *testing.T) {
	pipelineId := uuid.New()
	subKey := cache.RunOutput
	value := "MOCK_OUTPUT"
	client, mock := redismock.NewClientMock()
	marshSubKey, _ := json.Marshal(subKey)
	marshValue, _ := json.Marshal(value)

	type fields struct {
		redisClient *redis.Client
	}
	type args struct {
		ctx        context.Context
		pipelineId uuid.UUID
		subKey     cache.SubKey
	}
	tests := []struct {
		name    string
		mocks   func()
		fields  fields
		args    args
		want    interface{}
		wantErr bool
	}{
		{
			name: "Error during HGet operation",
			mocks: func() {
				mock.ExpectHGet(pipelineId.String(), string(marshSubKey)).SetErr(fmt.Errorf("MOCK_ERROR"))
			},
			fields: fields{client},
			args: args{
				ctx:        context.TODO(),
				pipelineId: pipelineId,
				subKey:     subKey,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Get existing value",
			mocks: func() {
				mock.ExpectHGet(pipelineId.String(), string(marshSubKey)).SetVal(string(marshValue))
			},
			fields: fields{client},
			args: args{
				ctx:        context.TODO(),
				pipelineId: pipelineId,
				subKey:     subKey,
			},
			want:    value,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.mocks()
			rc := &Cache{
				tt.fields.redisClient,
			}
			got, err := rc.GetValue(tt.args.ctx, tt.args.pipelineId, tt.args.subKey)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetValue() got = %v, want %v", got, tt.want)
			}
			mock.ClearExpect()
		})
	}
}

func TestRedisCache_SetExpTime(t *testing.T) {
	pipelineId := uuid.New()
	expTime := time.Second
	client, mock := redismock.NewClientMock()

	type fields struct {
		redisClient *redis.Client
	}
	type args struct {
		ctx        context.Context
		pipelineId uuid.UUID
		expTime    time.Duration
	}
	tests := []struct {
		name    string
		mocks   func()
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "Error during Exists operation",
			mocks: func() {
				mock.ExpectExists(pipelineId.String()).SetErr(fmt.Errorf("MOCK_ERROR"))
			},
			fields: fields{client},
			args: args{
				ctx:        context.Background(),
				pipelineId: pipelineId,
				expTime:    expTime,
			},
			wantErr: true,
		},
		{
			name: "Exists operation returns 0",
			mocks: func() {
				mock.ExpectExists(pipelineId.String()).SetVal(0)
			},
			fields: fields{client},
			args: args{
				ctx:        context.Background(),
				pipelineId: pipelineId,
				expTime:    expTime,
			},
			wantErr: true,
		},
		{
			name: "Set expiration time with error during Expire operation",
			mocks: func() {
				mock.ExpectExists(pipelineId.String()).SetVal(1)
				mock.ExpectExpire(pipelineId.String(), expTime).SetErr(fmt.Errorf("MOCK_ERROR"))
			},
			fields: fields{client},
			args: args{
				ctx:        context.Background(),
				pipelineId: pipelineId,
				expTime:    expTime,
			},
			wantErr: true,
		},
		{
			name: "Set expiration time",
			mocks: func() {
				mock.ExpectExists(pipelineId.String()).SetVal(1)
				mock.ExpectExpire(pipelineId.String(), expTime).SetVal(true)
			},
			fields: fields{client},
			args: args{
				ctx:        context.Background(),
				pipelineId: pipelineId,
				expTime:    expTime,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.mocks()
			rc := &Cache{
				tt.fields.redisClient,
			}
			if err := rc.SetExpTime(tt.args.ctx, tt.args.pipelineId, tt.args.expTime); (err != nil) != tt.wantErr {
				t.Errorf("SetExpTime() error = %v, wantErr %v", err, tt.wantErr)
			}
			mock.ClearExpect()
		})
	}
}

func TestRedisCache_SetValue(t *testing.T) {
	pipelineId := uuid.New()
	subKey := cache.Status
	value := pb.Status_STATUS_FINISHED
	client, mock := redismock.NewClientMock()
	marshSubKey, _ := json.Marshal(subKey)
	marshValue, _ := json.Marshal(value)

	type fields struct {
		redisClient *redis.Client
	}
	type args struct {
		ctx        context.Context
		pipelineId uuid.UUID
		subKey     cache.SubKey
		value      interface{}
	}
	tests := []struct {
		name    string
		mocks   func()
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "Error during HSet operation",
			mocks: func() {
				mock.ExpectHSet(pipelineId.String(), marshSubKey, marshValue).SetErr(fmt.Errorf("MOCK_ERROR"))
			},
			fields: fields{client},
			args: args{
				ctx:        context.Background(),
				pipelineId: pipelineId,
				subKey:     subKey,
				value:      value,
			},
			wantErr: true,
		},
		{
			name: "Set correct value",
			mocks: func() {
				mock.ExpectHSet(pipelineId.String(), marshSubKey, marshValue).SetVal(1)
				mock.ExpectExpire(pipelineId.String(), time.Minute*15).SetVal(true)
			},
			fields: fields{client},
			args: args{
				ctx:        context.Background(),
				pipelineId: pipelineId,
				subKey:     subKey,
				value:      value,
			},
			wantErr: false,
		},
		{
			name: "Set incorrect value",
			mocks: func() {
				mock.ExpectHSet(pipelineId.String(), marshSubKey, marshValue).SetVal(1)
				mock.ExpectExpire(pipelineId.String(), time.Minute*15).SetVal(true)
			},
			fields: fields{client},
			args: args{
				ctx:        context.Background(),
				pipelineId: pipelineId,
				subKey:     subKey,
				value:      make(chan int),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.mocks()
			rc := &Cache{
				tt.fields.redisClient,
			}
			if err := rc.SetValue(tt.args.ctx, tt.args.pipelineId, tt.args.subKey, tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("SetValue() error = %v, wantErr %v", err, tt.wantErr)
			}
			mock.ClearExpect()
		})
	}
}

func TestCache_GetCatalog(t *testing.T) {
	client, mock := redismock.NewClientMock()
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
	catalogMarsh, _ := json.Marshal(catalog)
	type fields struct {
		Client *redis.Client
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name    string
		mocks   func()
		fields  fields
		args    args
		want    []*pb.Categories
		wantErr bool
	}{
		{
			name: "Get existing catalog",
			mocks: func() {
				mock.ExpectGet(cache.ExamplesCatalog).SetVal(string(catalogMarsh))
			},
			fields:  fields{client},
			args:    args{context.Background()},
			want:    catalog,
			wantErr: false,
		},
		{
			name: "Error during Get operation",
			mocks: func() {
				mock.ExpectGet(cache.ExamplesCatalog).SetErr(fmt.Errorf("MOCK_ERROR"))
			},
			fields:  fields{client},
			args:    args{context.Background()},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.mocks()
			rc := &Cache{
				Client: tt.fields.Client,
			}
			got, err := rc.GetCatalog(tt.args.ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetCatalog() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetCatalog() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCache_SetCatalog(t *testing.T) {
	client, mock := redismock.NewClientMock()
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
	catalogMarsh, _ := json.Marshal(catalog)
	type fields struct {
		Client *redis.Client
	}
	type args struct {
		ctx     context.Context
		catalog []*pb.Categories
	}
	tests := []struct {
		name    string
		mocks   func()
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "Set catalog",
			mocks: func() {
				mock.ExpectSet(cache.ExamplesCatalog, catalogMarsh, catalogExpire).SetVal("")
			},
			fields: fields{client},
			args: args{
				ctx:     context.Background(),
				catalog: catalog,
			},
			wantErr: false,
		},
		{
			name: "Error during Set operation",
			mocks: func() {
				mock.ExpectSet(cache.ExamplesCatalog, catalogMarsh, catalogExpire).SetErr(fmt.Errorf("MOCK_ERROR"))
			},
			fields: fields{client},
			args: args{
				ctx:     context.Background(),
				catalog: catalog,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.mocks()
			rc := &Cache{
				Client: tt.fields.Client,
			}
			if err := rc.SetCatalog(tt.args.ctx, tt.args.catalog); (err != nil) != tt.wantErr {
				t.Errorf("SetCatalog() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCache_SetSdkCatalog(t *testing.T) {
	client, mock := redismock.NewClientMock()
	sdks := getSDKs()
	sdksMarsh, _ := json.Marshal(sdks)
	type args struct {
		ctx  context.Context
		sdks []*entity.SDKEntity
	}
	tests := []struct {
		name    string
		mocks   func()
		args    args
		wantErr bool
	}{
		{
			name: "Setting sdk catalog in the usual case",
			mocks: func() {
				mock.ExpectSet(cache.SdksCatalog, sdksMarsh, 0).SetVal("")
			},
			args: args{
				ctx:  context.Background(),
				sdks: sdks,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.mocks()
			rc := &Cache{Client: client}
			if err := rc.SetSdkCatalog(tt.args.ctx, tt.args.sdks); (err != nil) != tt.wantErr {
				t.Errorf("SetSdkCatalog() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCache_GetSdkCatalog(t *testing.T) {
	client, mock := redismock.NewClientMock()
	sdks := getSDKs()
	sdksMarsh, _ := json.Marshal(sdks)
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name    string
		mocks   func()
		args    args
		wantErr bool
	}{
		{
			name: "Getting sdk catalog in the usual case",
			mocks: func() {
				mock.ExpectGet(cache.SdksCatalog).SetVal(string(sdksMarsh))
			},
			args:    args{ctx: context.Background()},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.mocks()
			rc := &Cache{Client: client}
			got, err := rc.GetSdkCatalog(tt.args.ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetSdkCatalog() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, sdks) {
				t.Errorf("GetSdkCatalog() got = %v, want %v", got, sdks)
			}
		})
	}
}

func Test_newRedisCache(t *testing.T) {
	address := "host:port"
	type args struct {
		ctx  context.Context
		addr string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "Error during Ping operation",
			args: args{
				ctx:  context.Background(),
				addr: address,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.args.ctx, tt.args.addr); (err != nil) != tt.wantErr {
				t.Errorf("newRedisCache() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_unmarshalBySubKey(t *testing.T) {
	status := pb.Status_STATUS_FINISHED
	statusValue, _ := json.Marshal(status)
	output := "MOCK_OUTPUT"
	outputValue, _ := json.Marshal(output)
	canceledValue, _ := json.Marshal(false)
	runOutputIndex := 0
	runOutputIndexValue, _ := json.Marshal(runOutputIndex)
	type args struct {
		ctx    context.Context
		subKey cache.SubKey
		value  string
	}
	tests := []struct {
		name    string
		args    args
		want    interface{}
		wantErr bool
	}{
		{
			name: "Status subKey",
			args: args{
				ctx:    context.Background(),
				subKey: cache.Status,
				value:  string(statusValue),
			},
			want:    status,
			wantErr: false,
		},
		{
			name: "RunOutput subKey",
			args: args{
				subKey: cache.RunOutput,
				value:  string(outputValue),
			},
			want:    output,
			wantErr: false,
		},
		{
			name: "CompileOutput subKey",
			args: args{
				subKey: cache.CompileOutput,
				value:  string(outputValue),
			},
			want:    output,
			wantErr: false,
		},
		{
			name: "Graph subKey",
			args: args{
				subKey: cache.Graph,
				value:  string(outputValue),
			},
			want:    output,
			wantErr: false,
		},
		{
			// Test case with calling unmarshalBySubKey method with Canceled subKey.
			// As a result, want to receive false.
			name: "Canceled subKey",
			args: args{
				subKey: cache.Canceled,
				value:  string(canceledValue),
			},
			want:    false,
			wantErr: false,
		},
		{
			// Test case with calling unmarshalBySubKey method with RunOutputIndex subKey.
			// As a result, want to receive expected runOutputIndex.
			name: "RunOutputIndex subKey",
			args: args{
				subKey: cache.RunOutputIndex,
				value:  string(runOutputIndexValue),
			},
			want:    float64(runOutputIndex),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := unmarshalBySubKey(tt.args.subKey, tt.args.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("unmarshalBySubKey() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("unmarshalBySubKey() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func getSDKs() []*entity.SDKEntity {
	var sdkEntities []*entity.SDKEntity
	for _, sdk := range pb.Sdk_name {
		if sdk == pb.Sdk_SDK_UNSPECIFIED.String() {
			continue
		}
		sdkEntities = append(sdkEntities, &entity.SDKEntity{
			Name:           sdk,
			DefaultExample: "MOCK_DEFAULT_EXAMPLE",
		})
	}
	return sdkEntities
}
