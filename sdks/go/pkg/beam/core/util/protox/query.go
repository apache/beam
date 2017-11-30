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
	"net/url"

	"github.com/golang/protobuf/proto"
)

// Some Beam data types use URL query escaping to transmit byte strings
// in JSON fields. These utility functions support encoding and decoding
// those message types.

// EncodeQueryEscaped encodes the supplied message using URL query escaping.
func EncodeQueryEscaped(msg proto.Message) (string, error) {
	data, err := proto.Marshal(msg)
	if err != nil {
		return "", err
	}
	return url.QueryEscape(string(data)), nil
}

// DecodeQueryEscaped recovers a protocol buffer message from the supplied
// URL query escaped string.
func DecodeQueryEscaped(data string, ret proto.Message) error {
	decoded, err := url.QueryUnescape(data)
	if err != nil {
		return err
	}
	return proto.Unmarshal([]byte(decoded), ret)
}
