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

package storage

import (
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"path"
	"runtime"
	"strings"

	tob "beam.apache.org/learning/tour-of-beam/backend/internal"
)

func getSamplesPath() string {
	_, filepath, _, _ := runtime.Caller(1)
	return path.Join(path.Dir(filepath), "..", "..", "samples", "api")
}

type Mock struct{}

// check if the interface is implemented.
var _ Iface = &Mock{}

func (d *Mock) GetContentTree(_ context.Context, sdk tob.Sdk) (ct tob.ContentTree, err error) {
	// this sdk is special: we use it as an empty learning path
	if sdk == tob.SDK_SCIO {
		return ct, errors.New("empty sdk tree")
	}
	content, _ := ioutil.ReadFile(path.Join(getSamplesPath(), "get_content_tree.json"))
	_ = json.Unmarshal(content, &ct)
	return ct, nil
}

func (d *Mock) SaveContentTrees(_ context.Context, _ []tob.ContentTree) error {
	return nil
}

func (d *Mock) GetUnitContent(_ context.Context, sdk tob.Sdk, unitId string) (u *tob.Unit, err error) {
	if strings.HasPrefix(unitId, "unknown_") {
		return u, tob.ErrNoUnit
	}
	content, _ := ioutil.ReadFile(path.Join(getSamplesPath(), "get_unit_content.json"))
	err = json.Unmarshal(content, &u)
	return u, err
}

func (d *Mock) CheckUnitExists(ctx context.Context, sdk tob.Sdk, unitId string) error {
	for _, existingId := range []string{"intro-unit", "example1", "challenge1"} {
		if unitId == existingId {
			return nil
		}
	}
	return tob.ErrNoUnit
}

func (d *Mock) SaveUser(ctx context.Context, uid string) error {
	return nil
}

func (d *Mock) GetUserProgress(_ context.Context, sdk tob.Sdk, userId string) (sp *tob.SdkProgress, err error) {
	content, _ := ioutil.ReadFile(path.Join(getSamplesPath(), "get_user_progress.json"))
	_ = json.Unmarshal(content, &sp)
	return sp, nil
}

func (d *Mock) SetUnitComplete(ctx context.Context, sdk tob.Sdk, unitId, uid string) error {
	return nil
}

func (d *Mock) SaveUserSnippetId(
	ctx context.Context, sdk tob.Sdk, unitId, uid string,
	externalSave func(string) (string, error),
) error {
	return nil
}

func (d *Mock) DeleteProgress(ctx context.Context, uid string) error {
	return nil
}
