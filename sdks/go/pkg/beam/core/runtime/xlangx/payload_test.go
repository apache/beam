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

package xlangx

import (
	"reflect"
	"testing"
)

type testPayload struct {
	Foo string
	Bar int
}

func TestDecodeStructPayload(t *testing.T) {
	payload := testPayload{"foo", 100}

	bytes, err := EncodeStructPayload(testPayload{"foo", 100})
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := DecodeStructPayload(bytes)
	if err != nil {
		t.Fatal(err)
	}

	got, ok := decoded.(testPayload)
	if !ok {
		t.Fatalf("decoded type = %T, want testPayload", decoded)
	}
	if !reflect.DeepEqual(got, payload) {
		t.Errorf("decoded payload = %v, want %v", got, payload)
	}
}
