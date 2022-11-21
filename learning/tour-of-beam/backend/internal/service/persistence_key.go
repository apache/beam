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

package service

import (
	"crypto/sha256"
	"encoding/base64"
	"os"
	"strings"

	tob "beam.apache.org/learning/tour-of-beam/backend/internal"
)

func makePersistenceKey(sdk tob.Sdk, unitId, uid string) string {
	h := sha256.New()
	// never share!
	plainKey := strings.Join(
		[]string{os.Getenv("PERSISTENCE_KEY_SALT"), sdk.String(), unitId, uid},
		"|")
	_, err := h.Write([]byte(plainKey))
	if err != nil {
		panic(err)
	}
	raw := h.Sum(nil)

	// base64 encode to pass as protobuf string
	encoded := base64.URLEncoding.EncodeToString(raw)

	return encoded
}
