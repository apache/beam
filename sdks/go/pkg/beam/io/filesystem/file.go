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

package filesystem

import (
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*FileMetadata)(nil)).Elem())
}

// FileMetadata contains metadata about a file, namely its path and size in bytes.
type FileMetadata struct {
	Path string
	Size int64
}

// Compression is the type of compression used to compress a file.
type Compression int

const (
	// CompressionAuto indicates that the compression type should be auto-detected.
	CompressionAuto Compression = iota
	// CompressionGzip indicates that the file is compressed using gzip.
	CompressionGzip
	// CompressionUncompressed indicates that the file is not compressed.
	CompressionUncompressed
)
