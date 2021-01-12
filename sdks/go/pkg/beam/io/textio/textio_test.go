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

// Package textio contains transforms for reading and writing text files.
package textio

import (
	"testing"

	_ "github.com/apache/beam/sdks/go/pkg/beam/io/filesystem/local"
)

func TestRead(t *testing.T) {
	f := "../../../../data/textio_test.txt"

	receivedLines := []string{}
	getLines := func(line string) {
		receivedLines = append(receivedLines, line)
	}

	err := readFn(nil, f, getLines)
	if err != nil {
		t.Fatalf("failed with %v", err)
	}
	want := 1
	if len(receivedLines) != 1 {
		t.Fatalf("received %v lines, want %v", len(receivedLines), want)
	}

}
