//go:build integration
// +build integration

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	PORT_SDK_LIST             = "PORT_SDK_LIST"
	PORT_GET_CONTENT_TREE     = "PORT_GET_CONTENT_TREE"
	PORT_GET_UNIT_CONTENT     = "PORT_GET_UNIT_CONTENT"
	PORT_GET_USER_PROGRESS    = "PORT_GET_USER_PROGRESS"
	PORT_POST_UNIT_COMPLETE   = "PORT_POST_UNIT_COMPLETE"
	PORT_POST_USER_CODE       = "PORT_POST_USER_CODE"
	PORT_POST_DELETE_PROGRESS = "PORT_POST_DELETE_PROGRESS"
)

// scenarios:
// + Get SDK list
// + Get content tree for existing SDK
// + Get content tree for non-existing SDK: 404 Not Found
// + Get unit content for existing SDK, existing unitId
// + Get unit content for non-existing SDK/unitId: 404 Not Found
// + Save user code/progress for a registered user
// + (negative) Save user code/progress w/o user token/bad token
// + (negative) Save user code/progress for non-existing SDK/unitId: 404 Not Found

func loadJson(path string, dst interface{}) error {
	fh, err := os.Open(path)
	if err != nil {
		return err
	}
	return json.NewDecoder(fh).Decode(dst)
}

func TestSdkList(t *testing.T) {
	port := os.Getenv(PORT_SDK_LIST)
	if port == "" {
		t.Fatal(PORT_SDK_LIST, "env not set")
	}
	url := "http://localhost:" + port

	mock_path := filepath.Join("..", "samples", "api", "get_sdk_list.json")
	var exp SdkList
	if err := loadJson(mock_path, &exp); err != nil {
		t.Fatal(err)
	}

	resp, err := GetSdkList(url)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, exp, resp)
}

func TestGetContentTree(t *testing.T) {
	port := os.Getenv(PORT_GET_CONTENT_TREE)
	if port == "" {
		t.Fatal(PORT_GET_CONTENT_TREE, "env not set")
	}
	url := "http://localhost:" + port

	mock_path := filepath.Join("..", "samples", "api", "get_content_tree.json")
	var exp ContentTree
	if err := loadJson(mock_path, &exp); err != nil {
		t.Fatal(err)
	}

	resp, err := GetContentTree(url, "python")
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, exp, resp)
}

func TestGetUnitContent(t *testing.T) {
	port := os.Getenv(PORT_GET_UNIT_CONTENT)
	if port == "" {
		t.Fatal(PORT_GET_UNIT_CONTENT, "env not set")
	}
	url := "http://localhost:" + port

	mock_path := filepath.Join("..", "samples", "api", "get_unit_content.json")
	var exp Unit
	if err := loadJson(mock_path, &exp); err != nil {
		t.Fatal(err)
	}

	resp, err := GetUnitContent(url, "python", "challenge1")
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, exp, resp)
}

func TestNegative(t *testing.T) {
	for i, params := range []struct {
		portEnvName  string
		queryParams  map[string]string
		headers      map[string]string
		expectedCode int
		expected     ErrorResponse
	}{
		{PORT_GET_CONTENT_TREE, nil, nil,
			http.StatusBadRequest,
			ErrorResponse{
				Code:    "BAD_FORMAT",
				Message: "unknown sdk",
			},
		},
		{PORT_GET_CONTENT_TREE, map[string]string{"sdk": "scio"}, nil,
			// TODO: actually here should be a NOT_FOUND error
			http.StatusInternalServerError,
			ErrorResponse{Code: "INTERNAL_ERROR", Message: "storage error"},
		},
		{PORT_GET_UNIT_CONTENT, map[string]string{"sdk": "python", "id": "unknown_unitId"},
			nil,
			http.StatusNotFound,
			ErrorResponse{
				Code:    "NOT_FOUND",
				Message: "unit not found",
			},
		},
		// bad authorization header we can test w/o Firebase auth emulator
		// for functional tests see auth_test.go
		{PORT_GET_USER_PROGRESS,
			map[string]string{"sdk": "python"},
			map[string]string{"authorization": "bad_header"},
			http.StatusUnauthorized,
			ErrorResponse{
				Code:    "UNAUTHORIZED",
				Message: "bad auth header",
			},
		},
	} {
		t.Log("Scenario", i)
		port := os.Getenv(params.portEnvName)
		if port == "" {
			t.Fatal(params.portEnvName, "env not set")
		}
		url := "http://localhost:" + port

		var resp ErrorResponse
		err := Get(&resp, url, params.queryParams, params.headers)
		checkBadHttpCode(t, err, params.expectedCode)
		assert.Equal(t, params.expected, resp)
	}
}
