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

func Test_encodeDecodeBSONMap(t *testing.T) {
	tests := []struct {
		name string
		val  bson.M
	}{
		{
			name: "Encode/decode bson.M",
			val:  bson.M{"key": "val"},
		},
		{
			name: "Encode/decode empty bson.M",
			val:  bson.M{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := encodeBSON[bson.M](tt.val)
			if err != nil {
				t.Fatalf("encodeBSON[bson.M]() error = %v", err)
			}

			decoded, err := decodeBSON[bson.M](encoded)
			if err != nil {
				t.Fatalf("decodeBSON[bson.M]() error = %v", err)
			}

			if diff := cmp.Diff(tt.val, decoded); diff != "" {
				t.Errorf("encode/decode mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func Test_encodeDecodeIDRangeRestriction(t *testing.T) {
	tests := []struct {
		name string
		rest idRangeRestriction
	}{
		{
			name: "Encode/decode idRangeRestriction",
			rest: idRangeRestriction{
				IDRange: idRange{
					Min:          objectIDFromHex(t, "5f1b2c3d4e5f60708090a0b0"),
					MinInclusive: true,
					Max:          objectIDFromHex(t, "5f1b2c3d4e5f60708090a0b9"),
					MaxInclusive: true,
				},
				CustomFilter: bson.M{"key": "val"},
				Count:        5,
			},
		},
		{
			name: "Encode/decode empty idRangeRestriction",
			rest: idRangeRestriction{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := encodeBSON[idRangeRestriction](tt.rest)
			if err != nil {
				t.Fatalf("encodeBSON[idRangeRestriction]() error = %v", err)
			}

			decoded, err := decodeBSON[idRangeRestriction](encoded)
			if err != nil {
				t.Fatalf("decodeBSON[idRangeRestriction]() error = %v", err)
			}

			if diff := cmp.Diff(tt.rest, decoded); diff != "" {
				t.Errorf("encode/decode mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func Test_encodeDecodeObjectID(t *testing.T) {
	tests := []struct {
		name     string
		objectID primitive.ObjectID
	}{
		{
			name:     "Encode/decode object ID",
			objectID: objectIDFromHex(t, "5f1b2c3d4e5f60708090a0b0"),
		},
		{
			name:     "Encode/decode nil object ID",
			objectID: primitive.NilObjectID,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := encodeObjectID(tt.objectID)
			decoded := decodeObjectID(encoded)

			if !cmp.Equal(decoded, tt.objectID) {
				t.Errorf("decodeObjectID() = %v, want %v", decoded, tt.objectID)
			}
		})
	}
}
