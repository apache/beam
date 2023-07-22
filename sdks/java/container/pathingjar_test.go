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
	"bufio"
	"bytes"
	"io"
	"strings"
	"testing"
)

func TestWritePathingJar(t *testing.T) {
	var buf bytes.Buffer
	input := []string{"a.jar", "b.jar", "c.jar", "thisVeryLongJarNameIsOverSeventyTwoCharactersLongAndNeedsToBeSplitCorrectly.jar"}
	err := writePathingJar(input, &buf)
	if err != nil {
		t.Errorf("writePathingJar(%v) = %v, want nil", input, err)
	}

	zr, err := zip.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Errorf("zip.NewReader() = %v, want nil", err)
	}

	f := getManifest(zr)
	if f == nil {
		t.Fatalf("Jar didn't contain manifest")
	}

	{
		fr, err := f.Open()
		if err != nil {
			t.Errorf("(%v).Open() = %v, want nil", f.Name, err)
		}
		all, err := io.ReadAll(fr)
		if err != nil {
			t.Errorf("(%v).Open() = %v, want nil", f.Name, err)
		}
		fr.Close()
		want := "\nClass-Path: file:a.jar file:b.jar file:c.jar"
		if !strings.Contains(string(all), want) {
			t.Errorf("%v = %v, want nil", f.Name, err)
		}
	}

	{
		fr, err := f.Open()
		if err != nil {
			t.Errorf("(%v).Open() = %v, want nil", f.Name, err)
		}
		defer fr.Close()
		fs := bufio.NewScanner(fr)
		fs.Split(bufio.ScanLines)

		for fs.Scan() {
			if got, want := len(fs.Bytes()), 72; got > want {
				t.Errorf("Manifest line exceeds limit got %v:, want %v line: %q", got, want, fs.Text())
			}
		}
	}

}

// getManifest extracts the java manifest from the zip file.
func getManifest(zr *zip.Reader) *zip.File {
	for _, f := range zr.File {
		if f.Name != "META-INF/MANIFEST.MF" {
			continue
		}
		return f
	}
	return nil
}
