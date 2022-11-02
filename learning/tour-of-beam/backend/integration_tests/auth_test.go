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
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

var emulator *EmulatorClient

func TestMain(m *testing.M) {
	// to parse go test * flags m.Run consumes
	flag.Parse()

	emulator = makeEmulatorCiient()
	emulator.waitApi()

	os.Exit(m.Run())
}

func TestSaveGetProgress(t *testing.T) {
	idToken := emulator.getIDToken()

	t.Run("save_complete", func(t *testing.T) {
		port := os.Getenv(PORT_POST_UNIT_COMPLETE)
		if port == "" {
			t.Fatal(PORT_POST_UNIT_COMPLETE, "env not set")
		}
		url := "http://localhost:" + port

		err := PostUnitComplete(url, "python", "unit_id_1", idToken)
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("save_code", func(t *testing.T) {
		port := os.Getenv(PORT_POST_USER_CODE)
		if port == "" {
			t.Fatal(PORT_POST_USER_CODE, "env not set")
		}
		url := "http://localhost:" + port
		req := UserCodeRequest{
			Files: []UserCodeFile{
				{Name: "main.py", Content: "import sys; sys.exit(0)", IsMain: true},
			},
			PipelineOptions: "some opts",
		}

		_, err := PostUserCode(url, "python", "unit_id_2", idToken, req)
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("save_code_fail", func(t *testing.T) {
		port := os.Getenv(PORT_POST_USER_CODE)
		if port == "" {
			t.Fatal(PORT_POST_USER_CODE, "env not set")
		}
		url := "http://localhost:" + port
		req := UserCodeRequest{
			Files: []UserCodeFile{
				// empty content doesn't pass validation
				{Name: "main.py", Content: "", IsMain: true},
			},
			PipelineOptions: "some opts",
		}

		resp, err := PostUserCode(url, "python", "unit_id_1", idToken, req)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, "INTERNAL_ERROR", resp.Code)
		msg := "playground api error"
		assert.Equal(t, msg, resp.Message[:len(msg)])

	})
	t.Run("get", func(t *testing.T) {
		port := os.Getenv(PORT_GET_USER_PROGRESS)
		if port == "" {
			t.Fatal(PORT_GET_USER_PROGRESS, "env not set")
		}
		url := "http://localhost:" + port

		mock_path := filepath.Join("..", "samples", "api", "get_user_progress.json")
		var exp SdkProgress
		if err := loadJson(mock_path, &exp); err != nil {
			t.Fatal(err)
		}

		resp, err := GetUserProgress(url, "python", idToken)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, exp, resp)
	})
}
