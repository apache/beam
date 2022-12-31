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

package mongodbio

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func Test_encodeBSONMap(t *testing.T) {
	tests := []struct {
		name    string
		m       bson.M
		want    []byte
		wantErr bool
	}{
		{
			name:    "Encode bson.M",
			m:       bson.M{"key": "val"},
			want:    []byte{18, 0, 0, 0, 2, 107, 101, 121, 0, 4, 0, 0, 0, 118, 97, 108, 0, 0},
			wantErr: false,
		},
		{
			name:    "Encode empty bson.M",
			m:       bson.M{},
			want:    []byte{5, 0, 0, 0, 0},
			wantErr: false,
		},
		{
			name:    "Encode nil bson.M",
			m:       bson.M(nil),
			want:    []byte{5, 0, 0, 0, 0},
			wantErr: false,
		},
		{
			name:    "Error - invalid bson.M",
			m:       bson.M{"key": make(chan int)},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := encodeBSONMap(tt.m)
			if (err != nil) != tt.wantErr {
				t.Fatalf("encodeBSONMap() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !cmp.Equal(got, tt.want) {
				t.Errorf("encodeBSONMap() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_decodeBSONMap(t *testing.T) {
	tests := []struct {
		name    string
		bytes   []byte
		want    bson.M
		wantErr bool
	}{
		{
			name:    "Decode bson.M",
			bytes:   []byte{18, 0, 0, 0, 2, 107, 101, 121, 0, 4, 0, 0, 0, 118, 97, 108, 0, 0},
			want:    bson.M{"key": "val"},
			wantErr: false,
		},
		{
			name:    "Decode empty bson.M",
			bytes:   []byte{5, 0, 0, 0, 0},
			want:    bson.M{},
			wantErr: false,
		},
		{
			name:    "Error - invalid bson.M",
			bytes:   []byte{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := decodeBSONMap(tt.bytes)
			if (err != nil) != tt.wantErr {
				t.Fatalf("decodeBSONMap() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !cmp.Equal(got, tt.want) {
				t.Errorf("decodeBSONMap() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_encodeObjectID(t *testing.T) {
	tests := []struct {
		name     string
		objectID primitive.ObjectID
		want     []byte
	}{
		{
			name:     "Encode object ID",
			objectID: objectIDFromHex(t, "5f1b2c3d4e5f60708090a0b0"),
			want:     []byte{95, 27, 44, 61, 78, 95, 96, 112, 128, 144, 160, 176},
		},
		{
			name:     "Encode nil object ID",
			objectID: primitive.NilObjectID,
			want:     []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := encodeObjectID(tt.objectID); !cmp.Equal(got, tt.want) {
				t.Errorf("encodeObjectID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_decodeObjectID(t *testing.T) {
	tests := []struct {
		name  string
		bytes []byte
		want  primitive.ObjectID
	}{
		{
			name:  "Decode object ID",
			bytes: []byte{95, 27, 44, 61, 78, 95, 96, 112, 128, 144, 160, 176},
			want:  objectIDFromHex(t, "5f1b2c3d4e5f60708090a0b0"),
		},
		{
			name:  "Decode nil object ID",
			bytes: []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			want:  primitive.NilObjectID,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := decodeObjectID(tt.bytes); !cmp.Equal(got, tt.want) {
				t.Errorf("decodeObjectID() = %v, want %v", got, tt.want)
			}
		})
	}
}
