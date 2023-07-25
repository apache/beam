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

package internal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParse(t *testing.T) {
	for _, s := range []struct {
		str      string
		expected Sdk
	}{
		{"go", SDK_GO},
		{"python", SDK_PYTHON},
		{"java", SDK_JAVA},
		{"scio", SDK_SCIO},

		{"Go", SDK_GO},
		{"Python", SDK_PYTHON},
		{"Java", SDK_JAVA},
		{"SCIO", SDK_SCIO},

		{"", SDK_UNDEFINED},
	} {
		assert.Equal(t, s.expected, ParseSdk(s.str))
	}
}

func TestSerialize(t *testing.T) {
	for _, s := range []struct {
		expectedId        string
		expectedTitle     string
		expectedStorageId string
		sdk               Sdk
	}{
		{"go", "Go", "SDK_GO", SDK_GO},
		{"python", "Python", "SDK_PYTHON", SDK_PYTHON},
		{"java", "Java", "SDK_JAVA", SDK_JAVA},
		{"scio", "SCIO", "SDK_SCIO", SDK_SCIO},
		{"", "", "", SDK_UNDEFINED},
	} {
		assert.Equal(t, s.expectedId, s.sdk.String())
		if s.sdk == SDK_UNDEFINED {
			assert.Panics(t, func() { s.sdk.Title() })
		} else {
			assert.Equal(t, s.expectedTitle, s.sdk.Title())
		}
		if s.sdk == SDK_UNDEFINED {
			assert.Panics(t, func() { s.sdk.StorageID() })
		} else {
			assert.Equal(t, s.expectedStorageId, s.sdk.StorageID())
		}
	}
}

func TestSdkList(t *testing.T) {

	assert.Equal(t, SdkList{
		[]SdkItem{
			{"java", "Java"},
			{"python", "Python"},
			{"go", "Go"},
		},
	}, MakeSdkList())
}
