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

package passert

import (
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"sort"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
)

// Hash validates that the incoming PCollection<string> has the given size and
// base64-encoded MD5 hash code. It buffers the entire PCollection in memory
// and sorts it for determinism.
func Hash(s beam.Scope, col beam.PCollection, name, hash string, size int) {
	s = s.Scope(fmt.Sprintf("passert.Hash(%v)", name))

	keyed := beam.AddFixedKey(s, col)
	grouped := beam.GroupByKey(s, keyed)
	beam.ParDo0(s, &hashFn{Name: name, Size: size, Hash: hash}, grouped)
}

type hashFn struct {
	Name string `json:"name,omitempty"`
	Size int    `json:"size,omitempty"`
	Hash string `json:"hash,omitempty"`
}

func (f *hashFn) ProcessElement(_ int, lines func(*string) bool) error {
	var col []string
	var str string
	for lines(&str) {
		col = append(col, str)
	}
	sort.Strings(col)

	md5W := md5.New()
	for _, str := range col {
		if _, err := md5W.Write([]byte(str)); err != nil {
			panic(err) // cannot fail
		}
	}
	hash := base64.StdEncoding.EncodeToString(md5W.Sum(nil))

	if f.Size != len(col) || f.Hash != hash {
		return errors.Errorf("passert.Hash(%v) = (%v,%v), want (%v,%v)", f.Name, len(col), hash, f.Size, f.Hash)
	}
	return nil
}
