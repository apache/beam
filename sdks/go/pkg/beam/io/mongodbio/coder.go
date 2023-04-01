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
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func init() {
	beam.RegisterCoder(
		reflect.TypeOf((*idRangeRestriction)(nil)).Elem(),
		encodeRestriction,
		decodeRestriction,
	)
	beam.RegisterCoder(
		reflect.TypeOf((*idRange)(nil)).Elem(),
		encodeRange,
		decodeRange,
	)
	beam.RegisterCoder(
		reflect.TypeOf((*primitive.ObjectID)(nil)).Elem(),
		encodeObjectID,
		decodeObjectID,
	)
}

// Working around https://github.com/apache/beam/issues/26066
// with explicit wrappers for the generic functions.

func encodeRestriction(in idRangeRestriction) ([]byte, error) {
	return encodeBSON(in)
}
func decodeRestriction(in []byte) (idRangeRestriction, error) {
	return decodeBSON[idRangeRestriction](in)
}

func encodeRange(in idRange) ([]byte, error) {
	return encodeBSON(in)
}
func decodeRange(in []byte) (idRange, error) {
	return decodeBSON[idRange](in)
}

func encodeBSON[T any](in T) ([]byte, error) {
	out, err := bson.Marshal(in)
	if err != nil {
		return nil, fmt.Errorf("error encoding BSON: %w", err)
	}

	return out, nil
}

func decodeBSON[T any](in []byte) (T, error) {
	var out T
	if err := bson.Unmarshal(in, &out); err != nil {
		return out, fmt.Errorf("error decoding BSON: %w", err)
	}

	return out, nil
}

func encodeObjectID(objectID primitive.ObjectID) []byte {
	return objectID[:]
}

func decodeObjectID(bytes []byte) primitive.ObjectID {
	var out primitive.ObjectID

	copy(out[:], bytes[:])

	return out
}
