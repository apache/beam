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

	zf.Write([]byte("Manifest-Version: 1.0\n"))
	zf.Write([]byte("Created-By: sdks/java/container/pathingjar.go"))
	zf.Write([]byte("Class-Path: " + strings.Join(classpaths, " ")))
	zf.Write([]byte("\n"))
	return nil
}
