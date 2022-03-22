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

package ioutilx

import (
	"bufio"
	"bytes"
	"testing"
)

func TestWriteUnsafe(t *testing.T) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	var data = []byte("hello world!")

	length, err := WriteUnsafe(writer, data)
	// err is expected to be nil
	if err != nil {
		t.Errorf("failed to write data, got error: %v", err)
	}
	// length of data is expected to match input
	if got, want := length, len(data); got != want {
		t.Errorf("got length %v, wanted %v", got, want)
	}
	writer.Flush()
	// buf is expected to contain the input data
	if got, want := buf.String(), string(data); got != want {
		t.Errorf("got string %q, wanted %q", got, want)
	}
}
