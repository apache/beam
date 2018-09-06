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
	"context"
	"io/ioutil"
)

// Read fully reads the given file from the file system.
func Read(ctx context.Context, fs Interface, filename string) ([]byte, error) {
	r, err := fs.OpenRead(ctx, filename)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	return ioutil.ReadAll(r)
}

// Write writes the given content to the file system.
func Write(ctx context.Context, fs Interface, filename string, data []byte) error {
	w, err := fs.OpenWrite(ctx, filename)
	if err != nil {
		return err
	}

	if _, err := w.Write(data); err != nil {
		return err
	}
	return w.Close()
}
