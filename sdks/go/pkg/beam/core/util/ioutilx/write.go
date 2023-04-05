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

package ioutilx

import (
	"io"
	"unsafe"
)

// WriteUnsafe writes the buffer to the given writer, but breaks
// escape analysis so that the passed in []byte can be allocated
// to the stack. Use with caution, and only when you're certain
// the writer doesn't retain the passed in byte buffer.
func WriteUnsafe(w io.Writer, b []byte) (int, error) {
	ret := *(*[]byte)(noescape(unsafe.Pointer(&b)))
	return w.Write(ret)
}
