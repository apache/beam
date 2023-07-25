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
	"context"
	"flag"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

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

func makeUserCodeRequest() UserCodeRequest {
	return UserCodeRequest{
		Files: []UserCodeFile{
			{Name: "main.py", Content: "import sys; sys.exit(0)", IsMain: true},
		},
		PipelineOptions: "some opts",
	}
}

func checkBadHttpCode(t *testing.T, err error, code int) {
	if err == nil {
		t.Fatal("error expected")
	}
	if err, ok := err.(*ErrBadResponse); ok {
		if err.Code == code {
			return
		}
	}
	t.Fatalf("Expected ErrBadResponse with code %v, got %v", code, err)
}

func TestSaveGetProgress(t *testing.T) {
	idToken := emulator.getIDToken("a@b.c")

	// postUnitCompleteURL
	port := os.Getenv(PORT_POST_UNIT_COMPLETE)
	if port == "" {
		t.Fatal(PORT_POST_UNIT_COMPLETE, "env not set")
	}
	postUnitCompleteURL := "http://localhost:" + port

	// postUserCodeURL
	port = os.Getenv(PORT_POST_USER_CODE)
	if port == "" {
		t.Fatal(PORT_POST_USER_CODE, "env not set")
	}
	postUserCodeURL := "http://localhost:" + port

	// getUserProgressURL
	port = os.Getenv(PORT_GET_USER_PROGRESS)
	if port == "" {
		t.Fatal(PORT_GET_USER_PROGRESS, "env not set")
	}
	getUserProgressURL := "http://localhost:" + port

	// postDeleteProgressURL
	port = os.Getenv(PORT_POST_DELETE_PROGRESS)
	if port == "" {
		t.Fatal(PORT_POST_DELETE_PROGRESS, "env not set")
	}
	postDeleteProgressURL := "http://localhost:" + port

	t.Run("save_complete_no_unit", func(t *testing.T) {
		resp, err := PostUnitComplete(postUnitCompleteURL, "python", "unknown_unit_id_1", idToken)
		checkBadHttpCode(t, err, http.StatusNotFound)
		assert.Equal(t, "NOT_FOUND", resp.Code)
		assert.Equal(t, "unit not found", resp.Message)
	})
	t.Run("save_complete", func(t *testing.T) {
		_, err := PostUnitComplete(postUnitCompleteURL, "python", "challenge1", idToken)
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("save_code", func(t *testing.T) {
		req := makeUserCodeRequest()
		_, err := PostUserCode(postUserCodeURL, "python", "example1", idToken, req)
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("save_code_playground_fail", func(t *testing.T) {
		req := makeUserCodeRequest()

		// empty content doesn't pass validation
		req.Files[0].Content = ""

		resp, err := PostUserCode(postUserCodeURL, "python", "example1", idToken, req)
		checkBadHttpCode(t, err, http.StatusInternalServerError)
		assert.Equal(t, "INTERNAL_ERROR", resp.Code)
		msg := "playground api error"
		assert.Equal(t, msg, resp.Message[:len(msg)])

	})
	t.Run("save_code_no_unit", func(t *testing.T) {
		req := makeUserCodeRequest()
		resp, err := PostUserCode(postUserCodeURL, "python", "unknown_unit_id_1", idToken, req)
		checkBadHttpCode(t, err, http.StatusNotFound)
		assert.Equal(t, "NOT_FOUND", resp.Code)
		assert.Equal(t, "unit not found", resp.Message)

	})
	t.Run("get", func(t *testing.T) {

		mock_path := filepath.Join("..", "samples", "api", "get_user_progress.json")
		var exp SdkProgress
		if err := loadJson(mock_path, &exp); err != nil {
			t.Fatal(err)
		}

		resp, err := GetUserProgress(getUserProgressURL, "python", idToken)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, len(exp.Units), len(resp.Units))
		assert.Equal(t, exp.Units[1].Id, resp.Units[1].Id)
		// snippet_id is derived from random uid
		exp.Units[1].UserSnippetId = resp.Units[1].UserSnippetId
		assert.Equal(t, exp, resp)
	})
	t.Run("delete_progress", func(t *testing.T) {
		_, err := PostDeleteProgress(postDeleteProgressURL, idToken)
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("delete_progress_retry", func(t *testing.T) {
		_, err := PostDeleteProgress(postDeleteProgressURL, idToken)
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("get_deleted", func(t *testing.T) {
		resp, err := GetUserProgress(getUserProgressURL, "python", idToken)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, 0, len(resp.Units))
	})
}

func TestUserCode(t *testing.T) {
	var snippetId1, snippetId2, snippetId3 string
	idToken1 := emulator.getIDToken("a1@b.c")
	idToken2 := emulator.getIDToken("a2@b.c")
	req := makeUserCodeRequest()
	originalCode := req.Files[0].Content

	// postUserCodeURL
	port := os.Getenv(PORT_POST_USER_CODE)
	if port == "" {
		t.Fatal(PORT_POST_USER_CODE, "env not set")
	}
	postUserCodeURL := "http://localhost:" + port

	// getUserProgressURL
	port = os.Getenv(PORT_GET_USER_PROGRESS)
	if port == "" {
		t.Fatal(PORT_GET_USER_PROGRESS, "env not set")
	}
	getUserProgressURL := "http://localhost:" + port

	t.Run("save_code_user1_example1", func(t *testing.T) {
		_, err := PostUserCode(postUserCodeURL, "python", "example1", idToken1, req)
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("save_code_user2_example1", func(t *testing.T) {
		_, err := PostUserCode(postUserCodeURL, "python", "example1", idToken2, req)
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("check1", func(t *testing.T) {
		resp, err := GetUserProgress(getUserProgressURL, "python", idToken1)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, "example1", resp.Units[0].Id)
		snippetId1 = resp.Units[0].UserSnippetId
	})
	t.Run("check2", func(t *testing.T) {
		resp, err := GetUserProgress(getUserProgressURL, "python", idToken2)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, "example1", resp.Units[0].Id)
		snippetId2 = resp.Units[0].UserSnippetId
		assert.NotEqual(t, snippetId1, snippetId2, "different users, same snippet ids")
	})
	t.Run("save_code_user1_updated", func(t *testing.T) {
		// modify snippet code
		req.Files[0].Content += "; sys.exit(1)"

		_, err := PostUserCode(postUserCodeURL, "python", "example1", idToken1, req)
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("check3", func(t *testing.T) {
		resp, err := GetUserProgress(getUserProgressURL, "python", idToken1)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, "example1", resp.Units[0].Id)
		snippetId3 = resp.Units[0].UserSnippetId
		assert.NotEqual(t, snippetId1, snippetId3, "updated code, same snippet ids")
	})
	t.Run("check_snippet1", func(t *testing.T) {
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		_, err := GetSnippet(ctx, snippetId1)
		assert.NotNil(t, err, "previous snippet available")

		resp, err := GetSnippet(ctx, snippetId3)
		assert.Nil(t, err)
		assert.Equal(t, req.Files[0].Content, resp.Files[0].Content)
		assert.Equal(t, req.Files[0].IsMain, resp.Files[0].IsMain)
		assert.Equal(t, req.Files[0].Name, resp.Files[0].Name)
		assert.Equal(t, req.PipelineOptions, resp.PipelineOptions)
	})
	t.Run("check_snippet2", func(t *testing.T) {
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		resp, err := GetSnippet(ctx, snippetId2)
		assert.Nil(t, err)
		assert.Equal(t, originalCode, resp.Files[0].Content)
		assert.Equal(t, req.Files[0].IsMain, resp.Files[0].IsMain)
		assert.Equal(t, req.Files[0].Name, resp.Files[0].Name)
		assert.Equal(t, req.PipelineOptions, resp.PipelineOptions)
	})
}
