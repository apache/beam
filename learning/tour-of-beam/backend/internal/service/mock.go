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

package service

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"path"
	"runtime"

	tob "beam.apache.org/learning/tour-of-beam/backend/internal"
)

func getSamplesPath() string {
	_, filepath, _, _ := runtime.Caller(1)
	return path.Join(path.Dir(filepath), "..", "..", "samples", "api")
}

type Mock struct{}

// check if the interface is implemented.
var _ IContent = &Mock{}

func (d *Mock) GetContentTree(_ context.Context, sdk tob.Sdk, userId *string) (ct tob.ContentTree, err error) {
	content, _ := ioutil.ReadFile(path.Join(getSamplesPath(), "get_content_tree.json"))
	_ = json.Unmarshal(content, &ct)
	return ct, nil
}

func (d *Mock) GetUnitContent(_ context.Context, sdk tob.Sdk, unitId string, userId *string) (u tob.Unit, err error) {
	content, _ := ioutil.ReadFile(path.Join(getSamplesPath(), "get_unit_content.json"))
	err = json.Unmarshal(content, &u)
	return u, err
}
