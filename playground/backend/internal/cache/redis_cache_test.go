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

package cache

import (
	pb "beam.apache.org/playground/backend/internal/api"
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"reflect"
	"testing"
	"time"
)

func TestRedisCache_GetValue(t *testing.T) {
	expectedStatus := pb.Status_STATUS_FINISHED
	expectedOutput := "MOCK_RUN_OUTPUT"
	type fields struct {
		redisClient *redis.Client
	}
	type args struct {
		ctx        context.Context
		pipelineId uuid.UUID
		subKey     SubKey
	}
	tests := []struct {
		name     string
		mockFunc func()
		fields   fields
		args     args
		want     interface{}
		wantErr  bool
	}{
		{
			name: "get status",
			mockFunc: func() {
				hGet = func(redisClient *redis.Client, ctx context.Context, key, subKey string) (string, error) {
					value, _ := json.Marshal(expectedStatus)
					return string(value), nil
				}
			},
			fields: fields{redisClient: redis.NewClient(&redis.Options{})},
			args: args{
				ctx:        context.Background(),
				pipelineId: uuid.New(),
				subKey:     SubKey_Status,
			},
			want:    &expectedStatus,
			wantErr: false,
		},
		{
			name: "get run output",
			mockFunc: func() {
				hGet = func(redisClient *redis.Client, ctx context.Context, key, subKey string) (string, error) {
					value, _ := json.Marshal(expectedOutput)
					return string(value), nil
				}
			},
			fields: fields{redisClient: redis.NewClient(&redis.Options{})},
			args: args{
				ctx:        context.Background(),
				pipelineId: uuid.New(),
				subKey:     SubKey_RunOutput,
			},
			want:    expectedOutput,
			wantErr: false,
		},
		{
			name: "error during HGet operation",
			mockFunc: func() {
				hGet = func(redisClient *redis.Client, ctx context.Context, key, subKey string) (string, error) {
					return "", fmt.Errorf("MOCK_ERROR")
				}
			},
			fields: fields{redisClient: redis.NewClient(&redis.Options{})},
			args: args{
				ctx:        context.Background(),
				pipelineId: uuid.New(),
				subKey:     SubKey_RunOutput,
			},
			wantErr: true,
		},
	}
	origHGetFunc := hGet
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.mockFunc()
			rc := &RedisCache{
				redisClient: tt.fields.redisClient,
			}
			got, err := rc.GetValue(tt.args.ctx, tt.args.pipelineId, tt.args.subKey)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetValue() got = %v, want %v", got, tt.want)
			}
		})
	}
	hGet = origHGetFunc
}

func TestRedisCache_SetExpTime(t *testing.T) {
	type fields struct {
		redisClient *redis.Client
	}
	type args struct {
		ctx        context.Context
		pipelineId uuid.UUID
		expTime    time.Duration
	}
	tests := []struct {
		name     string
		mockFunc func()
		fields   fields
		args     args
		wantErr  bool
	}{
		{
			name: "error during Exists operation",
			mockFunc: func() {
				exists = func(redisClient *redis.Client, ctx context.Context, pipelineId uuid.UUID) (int64, error) {
					return 0, fmt.Errorf("MOCK_ERROR")
				}
			},
			fields: fields{redisClient: redis.NewClient(&redis.Options{})},
			args: args{
				ctx:        context.Background(),
				pipelineId: uuid.New(),
				expTime:    time.Second,
			},
			wantErr: true,
		},
		{
			name: "Exists operation returns 0",
			mockFunc: func() {
				exists = func(redisClient *redis.Client, ctx context.Context, pipelineId uuid.UUID) (int64, error) {
					return 0, nil
				}
			},
			fields: fields{redisClient: redis.NewClient(&redis.Options{})},
			args: args{
				ctx:        context.Background(),
				pipelineId: uuid.New(),
				expTime:    time.Second,
			},
			wantErr: true,
		},
		{
			name: "error during Expire operation",
			mockFunc: func() {
				exists = func(redisClient *redis.Client, ctx context.Context, pipelineId uuid.UUID) (int64, error) {
					return 1, nil
				}
				expire = func(redisClient *redis.Client, ctx context.Context, key string, expTime time.Duration) (bool, error) {
					return false, fmt.Errorf("MOCK_ERROR")
				}
			},
			fields: fields{redisClient: redis.NewClient(&redis.Options{})},
			args: args{
				ctx:        context.Background(),
				pipelineId: uuid.New(),
				expTime:    time.Second,
			},
			wantErr: true,
		},
		{
			name: "all success",
			mockFunc: func() {
				exists = func(redisClient *redis.Client, ctx context.Context, pipelineId uuid.UUID) (int64, error) {
					return 1, nil
				}
				expire = func(redisClient *redis.Client, ctx context.Context, key string, expTime time.Duration) (bool, error) {
					return false, nil
				}
			},
			fields: fields{redisClient: redis.NewClient(&redis.Options{})},
			args: args{
				ctx:        context.Background(),
				pipelineId: uuid.New(),
				expTime:    time.Second,
			},
			wantErr: false,
		},
	}
	origExistsFunc := exists
	origExpireFunc := expire
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.mockFunc()
			rc := &RedisCache{
				redisClient: tt.fields.redisClient,
			}
			if err := rc.SetExpTime(tt.args.ctx, tt.args.pipelineId, tt.args.expTime); (err != nil) != tt.wantErr {
				t.Errorf("SetExpTime() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
	exists = origExistsFunc
	expire = origExpireFunc
}

func TestRedisCache_SetValue(t *testing.T) {
	type fields struct {
		redisClient *redis.Client
	}
	type args struct {
		ctx        context.Context
		pipelineId uuid.UUID
		subKey     SubKey
		value      interface{}
	}
	tests := []struct {
		name     string
		mockFunc func()
		fields   fields
		args     args
		wantErr  bool
	}{
		{
			name: "error during HSet operation",
			mockFunc: func() {
				hSet = func(redisClient *redis.Client, ctx context.Context, key string, subKeyMarsh, valueMarsh []byte) (int64, error) {
					return 0, fmt.Errorf("MOCK_ERROR")
				}
			},
			fields: fields{redisClient: redis.NewClient(&redis.Options{})},
			args: args{
				ctx:        context.Background(),
				pipelineId: uuid.New(),
				subKey:     SubKey_Status,
				value:      pb.Status_STATUS_FINISHED,
			},
			wantErr: true,
		},
		{
			name: "error during Expire operation",
			mockFunc: func() {
				hSet = func(redisClient *redis.Client, ctx context.Context, key string, subKeyMarsh, valueMarsh []byte) (int64, error) {
					return 0, nil
				}
				expire = func(redisClient *redis.Client, ctx context.Context, key string, expTime time.Duration) (bool, error) {
					return false, fmt.Errorf("MOCK_FUNCTION")
				}
			},
			fields: fields{redisClient: redis.NewClient(&redis.Options{})},
			args: args{
				ctx:        context.Background(),
				pipelineId: uuid.New(),
				subKey:     SubKey_Status,
				value:      pb.Status_STATUS_FINISHED,
			},
			wantErr: true,
		},
		{
			name: "all success",
			mockFunc: func() {
				hSet = func(redisClient *redis.Client, ctx context.Context, key string, subKeyMarsh, valueMarsh []byte) (int64, error) {
					return 0, nil
				}
				expire = func(redisClient *redis.Client, ctx context.Context, key string, expTime time.Duration) (bool, error) {
					return false, nil
				}
			},
			fields: fields{redisClient: redis.NewClient(&redis.Options{})},
			args: args{
				ctx:        context.Background(),
				pipelineId: uuid.New(),
				subKey:     SubKey_Status,
				value:      pb.Status_STATUS_FINISHED,
			},
			wantErr: false,
		},
	}
	origHSetFunc := hSet
	origExpireFunc := expire
	for _, tt := range tests {
		tt.mockFunc()
		t.Run(tt.name, func(t *testing.T) {
			rc := &RedisCache{
				redisClient: tt.fields.redisClient,
			}
			if err := rc.SetValue(tt.args.ctx, tt.args.pipelineId, tt.args.subKey, tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("SetValue() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
	hSet = origHSetFunc
	expire = origExpireFunc
}

func Test_newRedisCache(t *testing.T) {
	address := "host:port"
	type args struct {
		ctx  context.Context
		addr string
	}
	tests := []struct {
		name     string
		mockFunc func()
		args     args
		wantErr  bool
	}{
		{
			name: "error during Ping operation",
			mockFunc: func() {
				ping = func(redisClient *redis.Client, ctx context.Context) (string, error) {
					return "", fmt.Errorf("MOCK_ERROR")
				}
			},
			args: args{
				ctx:  context.Background(),
				addr: "host:port",
			},
			wantErr: true,
		},
		{
			name: "all success",
			mockFunc: func() {
				ping = func(redisClient *redis.Client, ctx context.Context) (string, error) {
					return "", nil
				}
			},
			args: args{
				ctx:  context.Background(),
				addr: address,
			},
			wantErr: false,
		},
	}
	origPingOperation := ping
	for _, tt := range tests {
		tt.mockFunc()
		t.Run(tt.name, func(t *testing.T) {
			if _, err := newRedisCache(tt.args.ctx, tt.args.addr); (err != nil) != tt.wantErr {
				t.Errorf("newRedisCache() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
	ping = origPingOperation
}

func Test_unmarshalBySubKey(t *testing.T) {
	status := pb.Status_STATUS_FINISHED
	statusValue, _ := json.Marshal(status)
	output := "MOCK_OUTPUT"
	outputValue, _ := json.Marshal(output)
	type args struct {
		subKey SubKey
		value  string
	}
	tests := []struct {
		name    string
		args    args
		want    interface{}
		wantErr bool
	}{
		{
			name: "status subKey",
			args: args{
				subKey: SubKey_Status,
				value:  string(statusValue),
			},
			want:    &status,
			wantErr: false,
		},
		{
			name: "runOutput subKey",
			args: args{
				subKey: SubKey_RunOutput,
				value:  string(outputValue),
			},
			want:    output,
			wantErr: false,
		},
		{
			name: "compileOutput subKey",
			args: args{
				subKey: SubKey_CompileOutput,
				value:  string(outputValue),
			},
			want:    output,
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
