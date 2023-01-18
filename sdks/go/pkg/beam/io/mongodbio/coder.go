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
		reflect.TypeOf((*bson.M)(nil)).Elem(),
		encodeBSONMap,
		decodeBSONMap,
	)
	beam.RegisterCoder(
		reflect.TypeOf((*primitive.ObjectID)(nil)).Elem(),
		encodeObjectID,
		decodeObjectID,
	)
}

func encodeBSONMap(m bson.M) ([]byte, error) {
	bytes, err := bson.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("error encoding BSON: %w", err)
	}

	return bytes, nil
}

func decodeBSONMap(bytes []byte) (bson.M, error) {
	var out bson.M
	if err := bson.Unmarshal(bytes, &out); err != nil {
		return nil, fmt.Errorf("error decoding BSON: %w", err)
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
