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

package fileio_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/fileio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/memfs"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"
)

func ExampleMatchFiles() {
	beam.Init()
	p, s := beam.NewPipelineWithRoot()

	matches := fileio.MatchFiles(s, "gs://path/to/*.gz")
	debug.Print(s, matches)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}

func ExampleMatchAll() {
	beam.Init()
	p, s := beam.NewPipelineWithRoot()

	globs := beam.Create(s, "gs://path/to/sub1/*.gz", "gs://path/to/sub2/*.gz")
	matches := fileio.MatchAll(s, globs)
	debug.Print(s, matches)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}

func ExampleMatchContinuously() {
	beam.Init()
	p, s := beam.NewPipelineWithRoot()

	matches := fileio.MatchContinuously(s, "gs://path/to/*.json", 10*time.Second)
	debug.Print(s, matches)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}

func ExampleReadMatches() {
	beam.Init()
	p, s := beam.NewPipelineWithRoot()

	matches := fileio.MatchFiles(s, "gs://path/to/*.gz")
	files := fileio.ReadMatches(s, matches)
	debug.Print(s, files)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}

// A format reader can use positional reads when available and buffer unsupported
// files. This also illustrates adapting ReaderAt to io.ReadSeeker.
func ExampleReadableFile_OpenSeekable() {
	ctx := context.Background()
	const path = "memfs://open-seekable-example"
	memfs.Write(path, []byte("header-footer"))
	file := fileio.ReadableFile{Metadata: fileio.FileMetadata{
		Path: path, Size: 13, ReadSeekEfficient: true,
	}}
	positional, err := file.OpenSeekable(ctx)
	var reader io.ReaderAt
	var size int64
	switch {
	case errors.Is(err, filesystem.ErrNotSeekable):
		data, readErr := file.Read(ctx)
		if readErr != nil {
			log.Fatal(readErr)
		}
		reader, size = bytes.NewReader(data), int64(len(data))
	case err != nil:
		log.Fatal(err)
	default:
		defer positional.Close()
		reader, size = positional, file.Metadata.Size
	}
	section := io.NewSectionReader(reader, 0, size)
	if _, err := section.Seek(-6, io.SeekEnd); err != nil {
		log.Fatal(err)
	}
	footer, err := io.ReadAll(section)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(footer))
	// Output: footer
}
