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

package protox

import (
	"encoding/base64"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"google.golang.org/protobuf/proto"
)

// MustEncodeBase64 encodes a proto wrapped in base64 and panics on failure.
func MustEncodeBase64(msg proto.Message) string {
	ret, err := EncodeBase64(msg)
	if err != nil {
		panic(err)
	}
	return ret
}

// EncodeBase64 encodes a proto wrapped in base64.
func EncodeBase64(msg proto.Message) (string, error) {
	data, err := proto.Marshal(msg)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(data), nil
}

// DecodeBase64 decodes a base64 wrapped proto.
func DecodeBase64(data string, ret proto.Message) error {
	decoded, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return errors.Wrap(err, "base64 decoding failed")
	}
	return proto.Unmarshal(decoded, ret)
}
