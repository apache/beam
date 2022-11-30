//Licensed to the Apache Software Foundation (ASF) under one or more
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

package local

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/goleak"

	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/cache"
	"beam.apache.org/playground/backend/internal/db/entity"
)

func samePipelineIdSlices(x, y []uuid.UUID) bool {
	if len(x) != len(y) {
		return false
	}
	// create a map of string -> int
	diff := make(map[uuid.UUID]int, len(x))
	for _, _x := range x {
		// 0 value for int is 0, so just increment a counter for the string
		diff[_x]++
	}
	for _, _y := range y {
		// If the string _y is not in diff bail out early
		if _, ok := diff[_y]; !ok {
			return false
		}
		diff[_y] -= 1
		if diff[_y] == 0 {
			delete(diff, _y)
		}
	}
	return len(diff) == 0
}

func TestLocalCache_GetValue(t *testing.T) {
	preparedId, _ := uuid.NewUUID()
	preparedSubKey := cache.CompileOutput
	value := "TEST_VALUE"
	preparedItemsMap := make(map[uuid.UUID]map[cache.SubKey]interface{})
	preparedItemsMap[preparedId] = make(map[cache.SubKey]interface{})
	preparedItemsMap[preparedId][preparedSubKey] = value
	preparedExpMap := make(map[uuid.UUID]time.Time)
	preparedExpMap[preparedId] = time.Now().Add(time.Millisecond)
	endedExpMap := make(map[uuid.UUID]time.Time)
	endedExpMap[preparedId] = time.Now().Add(-time.Millisecond)
	type fields struct {
		cleanupInterval     time.Duration
		items               map[uuid.UUID]map[cache.SubKey]interface{}
		pipelinesExpiration map[uuid.UUID]time.Time
	}
	type args struct {
		ctx        context.Context
		pipelineId uuid.UUID
		subKey     cache.SubKey
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    interface{}
		wantErr bool
	}{
		{
			name: "Get existing value",
			fields: fields{
				cleanupInterval:     cleanupInterval,
				items:               preparedItemsMap,
				pipelinesExpiration: preparedExpMap,
			},
			args: args{
				ctx:        context.Background(),
				pipelineId: preparedId,
				subKey:     preparedSubKey,
			},
			want:    value,
			wantErr: false,
		},
		{
			name: "Get not existing value",
			fields: fields{
				cleanupInterval: cleanupInterval,
				items:           make(map[uuid.UUID]map[cache.SubKey]interface{}),
			},
			args: args{
				pipelineId: preparedId,
				subKey:     preparedSubKey,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Get an existing value that is expiring",
			fields: fields{
				cleanupInterval:     cleanupInterval,
				items:               preparedItemsMap,
				pipelinesExpiration: endedExpMap,
			},
			args: args{
				ctx:        context.Background(),
				pipelineId: preparedId,
				subKey:     preparedSubKey,
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ls := &Cache{
				cleanupInterval:     tt.fields.cleanupInterval,
				items:               tt.fields.items,
				pipelinesExpiration: tt.fields.pipelinesExpiration,
			}
			got, err := ls.GetValue(tt.args.ctx, tt.args.pipelineId, tt.args.subKey)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetValue() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLocalCache_SetValue(t *testing.T) {
	preparedId, _ := uuid.NewUUID()
	preparedExpMap := make(map[uuid.UUID]time.Time)
	preparedExpMap[preparedId] = time.Now().Add(time.Millisecond)
	type fields struct {
		cleanupInterval     time.Duration
		items               map[uuid.UUID]map[cache.SubKey]interface{}
		pipelinesExpiration map[uuid.UUID]time.Time
	}
	type args struct {
		ctx        context.Context
		pipelineId uuid.UUID
		subKey     cache.SubKey
		value      interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "Set value",
			fields: fields{
				cleanupInterval:     cleanupInterval,
				items:               make(map[uuid.UUID]map[cache.SubKey]interface{}),
				pipelinesExpiration: preparedExpMap,
			},
			args: args{
				ctx:        context.Background(),
				pipelineId: preparedId,
				subKey:     cache.RunOutput,
				value:      "TEST_VALUE",
			},
			wantErr: false,
		},
		{
			name: "Set value for RunOutputIndex subKey",
			fields: fields{
				cleanupInterval:     cleanupInterval,
				items:               make(map[uuid.UUID]map[cache.SubKey]interface{}),
				pipelinesExpiration: preparedExpMap,
			},
			args: args{
				ctx:        context.Background(),
				pipelineId: preparedId,
				subKey:     cache.RunOutputIndex,
				value:      5,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lc := &Cache{
				cleanupInterval:     tt.fields.cleanupInterval,
				items:               tt.fields.items,
				pipelinesExpiration: tt.fields.pipelinesExpiration,
			}
			if err := lc.SetValue(tt.args.ctx, tt.args.pipelineId, tt.args.subKey, tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("SetValue() error = %v, wantErr %v", err, tt.wantErr)
			}
			_, err := lc.GetValue(tt.args.ctx, tt.args.pipelineId, tt.args.subKey)
			if err != nil {
				t.Errorf("Value with pipelineId: %s and subKey: %v not set in cache.", tt.args.pipelineId, tt.args.subKey)
			}
		})
	}
}

func TestLocalCache_SetExpTime(t *testing.T) {
	preparedId, _ := uuid.NewUUID()
	preparedItems := make(map[uuid.UUID]map[cache.SubKey]interface{})
	preparedItems[preparedId] = make(map[cache.SubKey]interface{})
	preparedItems[preparedId][cache.Status] = 1
	type fields struct {
		cleanupInterval     time.Duration
		items               map[uuid.UUID]map[cache.SubKey]interface{}
		pipelinesExpiration map[uuid.UUID]time.Time
	}
	type args struct {
		ctx        context.Context
		pipelineId uuid.UUID
		expTime    time.Duration
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "Set expiration time",
			fields: fields{
				cleanupInterval:     cleanupInterval,
				items:               preparedItems,
				pipelinesExpiration: make(map[uuid.UUID]time.Time),
			},
			args: args{
				ctx:        context.Background(),
				pipelineId: preparedId,
				expTime:    time.Minute,
			},
			wantErr: false,
		},
		{
			name: "Set expiration time for not existing value in cache items",
			fields: fields{
				cleanupInterval:     cleanupInterval,
				items:               make(map[uuid.UUID]map[cache.SubKey]interface{}),
				pipelinesExpiration: make(map[uuid.UUID]time.Time),
			},
			args: args{
				ctx:        context.Background(),
				pipelineId: preparedId,
				expTime:    time.Minute,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lc := &Cache{
				cleanupInterval:     tt.fields.cleanupInterval,
				items:               tt.fields.items,
				pipelinesExpiration: tt.fields.pipelinesExpiration,
			}
			err := lc.SetExpTime(tt.args.ctx, tt.args.pipelineId, tt.args.expTime)
			if (err != nil) != tt.wantErr {
				t.Errorf("SetExpTime() error = %v, wantErr %v", err, tt.wantErr)
				expTime, found := lc.pipelinesExpiration[tt.args.pipelineId]
				if expTime.Round(time.Second) != time.Now().Add(tt.args.expTime).Round(time.Second) || !found {
					t.Errorf("Expiration time of the pipeline: %s not set in cache.", tt.args.pipelineId)
				}
			}
		})
	}
}

func TestCache_SetCatalog(t *testing.T) {
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
	type fields struct {
		catalog []*pb.Categories
	}
	type args struct {
		ctx     context.Context
		catalog []*pb.Categories
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "Set catalog",
			fields: fields{
				catalog: catalog,
			},
			args: args{
				ctx:     context.Background(),
				catalog: catalog,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lc := &Cache{
				catalog: tt.fields.catalog,
			}
			if err := lc.SetCatalog(tt.args.ctx, tt.args.catalog); (err != nil) != tt.wantErr {
				t.Errorf("SetCatalog() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(lc.catalog, tt.args.catalog) {
				t.Errorf("SetCatalog() got = %v, want %v", lc.catalog, tt.args.catalog)
			}
		})
	}
}

func TestCache_GetCatalog(t *testing.T) {
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
	type fields struct {
		catalog []*pb.Categories
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []*pb.Categories
		wantErr bool
	}{
		{
			name:    "Get existing catalog",
			fields:  fields{catalog},
			args:    args{context.Background()},
			want:    catalog,
			wantErr: false,
		},
		{
			name:    "Get non existing catalog",
			fields:  fields{nil},
			args:    args{context.Background()},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lc := &Cache{
				catalog: tt.fields.catalog,
			}
			got, err := lc.GetCatalog(tt.args.ctx)
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

func TestCache_SetSdkCatalog(t *testing.T) {
	testable := new(Cache)
	sdks := getSDKs()
	type args struct {
		ctx  context.Context
		sdks []*entity.SDKEntity
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "Setting sdk catalog in the usual case",
			args: args{
				ctx:  context.Background(),
				sdks: sdks,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := testable.SetSdkCatalog(tt.args.ctx, tt.args.sdks)
			if (err != nil) != tt.wantErr {
				t.Errorf("SetSdkCatalog() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(sdks, testable.sdkCatalog) {
				t.Error("SetSdkCatalog() unexpected result")
			}
		})
	}
}

func TestCache_GetSdkCatalog(t *testing.T) {
	sdks := getSDKs()
	testable := &Cache{sdkCatalog: sdks}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "Getting sdk catalog in the usual case",
			args:    args{ctx: context.Background()},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actualResult, err := testable.GetSdkCatalog(tt.args.ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetSdkCatalog() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(sdks, actualResult) {
				t.Error("GetSdkCatalog() unexpected result")
			}
		})
	}
}

func TestLocalCache_startGC(t *testing.T) {
	ignoreOpenCensus := goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start")
	defer goleak.VerifyNone(t, ignoreOpenCensus)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	preparedId, _ := uuid.NewUUID()
	preparedItemsMap := make(map[uuid.UUID]map[cache.SubKey]interface{})
	preparedItemsMap[preparedId] = make(map[cache.SubKey]interface{})
	preparedItemsMap[preparedId][cache.CompileOutput] = "TEST_VALUE1"
	preparedItemsMap[preparedId][cache.RunOutput] = "TEST_VALUE2"
	preparedExpMap := make(map[uuid.UUID]time.Time)
	preparedExpMap[preparedId] = time.Now().Add(time.Microsecond)
	type fields struct {
		cleanupInterval     time.Duration
		items               map[uuid.UUID]map[cache.SubKey]interface{}
		pipelinesExpiration map[uuid.UUID]time.Time
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			name: "Checking for deleting expired pipelines",
			fields: fields{
				cleanupInterval:     time.Microsecond,
				items:               preparedItemsMap,
				pipelinesExpiration: preparedExpMap,
			},
		},
		{
			// Test case with calling startGC method with nil cache items.
			// As a result, items stay the same.
			name: "Checking for deleting expired pipelines with nil cache items",
			fields: fields{
				cleanupInterval:     time.Microsecond,
				items:               nil,
				pipelinesExpiration: preparedExpMap,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lc := &Cache{
				cleanupInterval:     tt.fields.cleanupInterval,
				items:               tt.fields.items,
				pipelinesExpiration: tt.fields.pipelinesExpiration,
			}
			go lc.startGC(ctx)
			time.Sleep(time.Millisecond)
			if len(tt.fields.items) != 0 {
				t.Errorf("Pipeline: %s not deleted in time.", preparedId)
			}
		})
	}
}

func TestLocalCache_expiredPipelines(t *testing.T) {
	preparedId1, _ := uuid.NewUUID()
	preparedId2, _ := uuid.NewUUID()
	preparedId3, _ := uuid.NewUUID()
	preparedExpMap := make(map[uuid.UUID]time.Time)
	preparedExpMap[preparedId1] = time.Now().Add(time.Microsecond)
	preparedExpMap[preparedId2] = time.Now().Add(time.Microsecond)
	preparedExpMap[preparedId3] = time.Now().Add(10 * time.Second)
	type fields struct {
		cleanupInterval     time.Duration
		items               map[uuid.UUID]map[cache.SubKey]interface{}
		pipelinesExpiration map[uuid.UUID]time.Time
	}
	tests := []struct {
		name          string
		fields        fields
		wantPipelines []uuid.UUID
	}{
		{
			name: "Getting expired pipelines",
			fields: fields{
				cleanupInterval:     cleanupInterval,
				items:               nil,
				pipelinesExpiration: preparedExpMap,
			},
			wantPipelines: []uuid.UUID{preparedId1, preparedId2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lc := &Cache{
				cleanupInterval:     tt.fields.cleanupInterval,
				items:               tt.fields.items,
				pipelinesExpiration: tt.fields.pipelinesExpiration,
			}
			time.Sleep(time.Second)
			if gotPipelines := lc.expiredPipelines(); !samePipelineIdSlices(gotPipelines, tt.wantPipelines) {
				t.Errorf("expiredPipelines() = %v, want %v", gotPipelines, tt.wantPipelines)
			}
		})
	}
}

func TestLocalCache_clearItems(t *testing.T) {
	preparedId1, _ := uuid.NewUUID()
	type fields struct {
		cleanupInterval     time.Duration
		items               map[uuid.UUID]map[cache.SubKey]interface{}
		pipelinesExpiration map[uuid.UUID]time.Time
	}
	type args struct {
		ctx       context.Context
		pipelines []uuid.UUID
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "Clear items",
			fields: fields{
				cleanupInterval:     cleanupInterval,
				items:               make(map[uuid.UUID]map[cache.SubKey]interface{}),
				pipelinesExpiration: make(map[uuid.UUID]time.Time),
			},
			args: args{
				ctx:       context.Background(),
				pipelines: []uuid.UUID{preparedId1},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lc := &Cache{
				cleanupInterval:     tt.fields.cleanupInterval,
				items:               tt.fields.items,
				pipelinesExpiration: tt.fields.pipelinesExpiration,
			}
			err := lc.SetValue(tt.args.ctx, preparedId1, cache.RunOutput, "TEST_VALUE")
			if err != nil {
				t.Error(err)
			}
			err = lc.SetExpTime(tt.args.ctx, preparedId1, time.Microsecond)
			if err != nil {
				t.Error(err)
			}
			lc.clearItems(tt.args.pipelines)
			if _, found := tt.fields.items[preparedId1]; found {
				t.Errorf("Pipeline: %s has not been deleted.", preparedId1)
			}
		})
	}
}

func TestNew(t *testing.T) {
	items := make(map[uuid.UUID]map[cache.SubKey]interface{})
	pipelinesExpiration := make(map[uuid.UUID]time.Time)
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name string
		args args
		want *Cache
	}{
		{
			name: "Initialize local cache",
			args: args{ctx: context.Background()},
			want: &Cache{
				cleanupInterval:     cleanupInterval,
				items:               items,
				pipelinesExpiration: pipelinesExpiration,
				catalog:             nil,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := New(tt.args.ctx); !reflect.DeepEqual(fmt.Sprint(got), fmt.Sprint(tt.want)) {
				t.Errorf("New() = %v, want %v", got, tt.want)
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
