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

package main

import (
	"archive/zip"
	"bytes"
	"io"
	"strings"
	"testing"
)

func TestWritePathingJar(t *testing.T) {
	var buf bytes.Buffer
	input := []string{"a.jar", "b.jar", "c.jar"}
	err := writePathingJar(input, &buf)
	if err != nil {
		t.Errorf("writePathingJar(%v) = %v, want nil", input, err)
	}

	zr, err := zip.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Errorf("zip.NewReader() = %v, want nil", err)
	}

	var found bool
	for _, f := range zr.File {
		if f.Name != "META-INF/MANIFEST.MF" {
			continue
		}
		found = true
		fr, err := f.Open()
		if err != nil {
			t.Errorf("(%v).Open() = %v, want nil", f.Name, err)
		}
		all, err := io.ReadAll(fr)
		if err != nil {
			t.Errorf("(%v).Open() = %v, want nil", f.Name, err)
		}
		want := "Class-Path: file:a.jar file:b.jar file:c.jar"
		if !strings.Contains(string(all), want) {
			t.Errorf("%v = %v, want nil", f.Name, err)
		}
	}
	if !found {
		t.Error("Jar didn't contain manifest")
	}
}
