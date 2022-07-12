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

package diagnostics

import (
	"bufio"
	"context"
	"os"

	"runtime/debug"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem"
)

func UploadHeapDump(ctx context.Context, dest string) error {
	heapDumpLoc := "heapDump"

	f, err := os.Create(heapDumpLoc)
	if err != nil {
		return err
	}
	debug.WriteHeapDump(f.Fd())

	heapDump, err := os.Open(heapDumpLoc)
	if err != nil {
		return err
	}
	defer heapDump.Close()
	heapDumpReader := bufio.NewReader(heapDump)

	fs, err := filesystem.New(ctx, dest)
	if err != nil {
		return err
	}
	defer fs.Close()
	fd, err := fs.OpenWrite(ctx, dest)
	if err != nil {
		return err
	}
	buf := bufio.NewWriterSize(fd, 1<<20) // use 1MB buffer

	_, err = heapDumpReader.WriteTo(buf)
	if err != nil {
		return err
	}

	if err := buf.Flush(); err != nil {
		return err
	}

	return fd.Close()
}
