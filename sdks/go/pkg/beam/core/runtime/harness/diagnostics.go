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

package harness

import (
	"bufio"
	"context"
	"os"

	"io/ioutil"
	"runtime/debug"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/gcs"
)

func UploadHeapDump(ctx context.Context, dest string, additionalDataToWrite string) error {
	heapDumpLoc := "heapDump"
	if heapDumpLoc == "" {
		return nil
	}
	err := generateHeapDump(heapDumpLoc)
	if err != nil {
		return err
	}
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

	buf.WriteString(additionalDataToWrite)

	_, err = heapDumpReader.WriteTo(buf)
	if err != nil {
		return err
	}

	if err := buf.Flush(); err != nil {
		return err
	}

	return fd.Close()
}

func generateHeapDump(location string) error {
	f, err := os.Create(location)
	if err != nil {
		return err
	}

	debug.WriteHeapDump(f.Fd())

	return nil
}

func findCoreDump() string {
	location := "dannystest"
	f, err := os.Create(location)
	if err != nil {
		return ""
	}
	// if _, err := f.Write([]byte(listDirContents("/bin"))); err != nil {
	// 	return ""
	// }
	// if _, err := f.Write([]byte(listDirContents("/var/lib/systemd/"))); err != nil {
	// 	return ""
	// }
	// if _, err := f.Write([]byte(listDirContents("/lib/systemd/system"))); err != nil {
	// 	return ""
	// }
	// if _, err := f.Write([]byte(listDirContents("./"))); err != nil {
	// 	return ""
	// }
	if _, err := f.Write([]byte(listFilesRecursive("/"))); err != nil {
		return ""
	}
	dat, _ := os.ReadFile("/proc/sys/kernel/core_pattern")
	if _, err := f.Write(dat); err != nil {
		return ""
	}

	if err := f.Close(); err != nil {
		return ""
	}

	return location
}

func listFilesRecursive(dir string) string{
	files, _ := ioutil.ReadDir(dir)
	fileString := ""
	for _, f := range files {
		if f.IsDir() {
			fileString += listFilesRecursive(dir + "/" + f.Name())
		} else {
			fileString += dir + "/" + f.Name() + "\n"
		}
	}
	return fileString
}