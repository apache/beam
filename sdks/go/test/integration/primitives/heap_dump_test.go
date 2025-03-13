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

package primitives

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/gcs"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/v2/go/test/integration"
)

func TestOomParDo(t *testing.T) {
	integration.CheckFilters(t)
	if flag.Lookup("temp_location") == nil {
		t.Fatalf("A temp_location must be provided to correctly run TestOomParDo")
	}
	tempLocation := flag.Lookup("temp_location").Value.(flag.Getter).Get().(string)
	if tempLocation == "" {
		t.Fatalf("A temp_location must be provided to correctly run TestOomParDo")
	}
	dumpLocation := fmt.Sprintf("%v/heapProfiles/*", strings.TrimSuffix(tempLocation, "/"))
	ctx := context.Background()

	fs, err := filesystem.New(ctx, dumpLocation)
	if err != nil {
		t.Fatalf("Failed to connect to filesystem: %v", err)
	}
	defer fs.Close()

	files, err := fs.List(ctx, dumpLocation)
	if err != nil {
		t.Fatalf("Failed to connect to filesystem: %v", err)
	}
	startFiles := len(files)

	ptest.Run(OomParDo())

	files, err = fs.List(ctx, dumpLocation)
	if err != nil {
		t.Fatalf("Failed to connect to filesystem: %v", err)
	}
	endFiles := len(files)

	if startFiles >= endFiles {
		t.Fatalf("No new heap dumps generated on OOM. There were %v dumps before running and %v after", startFiles, endFiles)
	}
}
