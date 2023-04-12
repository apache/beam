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
	"fmt"
	"io"
	"os"
	"strings"
)

// makePathingJar produces a context or 'pathing' jar which only contains the relative
// classpaths in its META-INF/MANIFEST.MF.
//
// Since we build with Java 8 as a minimum, this is the only supported way of reducing
// command line length, since argsfile support wasn't added until Java 9.
//
// https://docs.oracle.com/javase/8/docs/technotes/guides/jar/jar.html is the spec.
//
// In particular, we only need to populate the Jar with a Manifest-Version and Class-Path
// attributes.
// Per https://docs.oracle.com/javase/8/docs/technotes/guides/jar/jar.html#classpath
// the classpath URLs must be relative for security reasons.
func makePathingJar(classpaths []string) (string, error) {
	f, err := os.Create("pathing.jar")
	if err != nil {
		return "", fmt.Errorf("unable to create pathing.jar: %w", err)
	}
	defer f.Close()
	if err := writePathingJar(classpaths, f); err != nil {
		return "", fmt.Errorf("unable to write pathing.jar: %w", err)
	}
	return f.Name(), nil
}

var lineBreak = []byte{'\r', '\n'}
var continuation = []byte{' '}

func writePathingJar(classpaths []string, w io.Writer) error {
	jar := zip.NewWriter(w)
	defer jar.Close()

	if _, err := jar.Create("META-INF/"); err != nil {
		return fmt.Errorf("unable to create META-INF/ directory: %w", err)
	}

	zf, err := jar.Create("META-INF/MANIFEST.MF")
	if err != nil {
		return fmt.Errorf("unable to create META-INF/MANIFEST.MF: %w", err)
	}

	zf.Write([]byte("Manifest-Version: 1.0"))
	zf.Write(lineBreak)
	zf.Write([]byte("Created-By: sdks/java/container/pathingjar.go"))
	zf.Write(lineBreak)
	// Class-Path: must have a sequence of relative URIs for the paths
	// which we assume outright in this case.

	// We could do this memory efficiently, but it's not worthwhile compared to the complexity
	// at this stage.
	allCPs := "Class-Path: file:" + strings.Join(classpaths, " file:")

	const lineLenMax = 71 // it's actually 72, but we remove 1 to account for the continuation line prefix.
	buf := make([]byte, lineLenMax)
	cur := 0
	for cur+lineLenMax < len(allCPs) {
		next := cur + lineLenMax
		copy(buf, allCPs[cur:next])
		zf.Write(buf)
		zf.Write(lineBreak)
		zf.Write(continuation)
		cur = next
	}
	zf.Write([]byte(allCPs[cur:]))
	zf.Write(lineBreak)
	return nil
}
