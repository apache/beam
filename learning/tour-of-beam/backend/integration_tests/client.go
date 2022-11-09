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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
)

var (
	ExpectedHeaders = map[string]string{
		"Access-Control-Allow-Origin": "*",
		"Content-Type":                "application/json",
	}
)

func verifyHeaders(header http.Header) error {
	for k, v := range ExpectedHeaders {
		if actual := header.Get(k); actual != v {
			return fmt.Errorf("header %s mismatch: %s (expected %s)", k, actual, v)
		}
	}

	return nil
}

func GetSdkList(url string) (SdkList, error) {
	var result SdkList
	err := Get(&result, url, nil, nil)
	return result, err
}

func GetContentTree(url, sdk string) (ContentTree, error) {
	var result ContentTree
	err := Get(&result, url, map[string]string{"sdk": sdk}, nil)
	return result, err
}

func GetUnitContent(url, sdk, unitId string) (Unit, error) {
	var result Unit
	err := Get(&result, url, map[string]string{"sdk": sdk, "id": unitId}, nil)
	return result, err
}

func GetUserProgress(url, sdk, token string) (SdkProgress, error) {
	var result SdkProgress
	err := Get(&result, url, map[string]string{"sdk": sdk},
		map[string]string{"Authorization": "Bearer " + token})
	return result, err
}

func PostUnitComplete(url, sdk, unitId, token string) error {
	var result interface{}
	err := Do(&result, http.MethodPost, url, map[string]string{"sdk": sdk, "id": unitId},
		map[string]string{"Authorization": "Bearer " + token}, nil)
	return err
}

func PostUserCode(url, sdk, unitId, token string, body UserCodeRequest) (ErrorResponse, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return ErrorResponse{}, err
	}

	var result ErrorResponse
	err = Do(&result, http.MethodPost, url, map[string]string{"sdk": sdk, "id": unitId},
		map[string]string{"Authorization": "Bearer " + token}, bytes.NewReader(raw))
	return result, err
}

func Get(dst interface{}, url string, queryParams, headers map[string]string) error {
	return Do(dst, http.MethodGet, url, queryParams, headers, nil)
}

// Generic HTTP call wrapper
// params:
// * dst: response struct pointer
// * url: request  url
// * query_params: url query params, as a map (we don't use multiple-valued params)
func Do(dst interface{}, method, url string, queryParams, headers map[string]string, body io.Reader) error {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Add(k, v)
	}

	if len(queryParams) > 0 {
		q := req.URL.Query()
		for k, v := range queryParams {
			q.Add(k, v)
		}
		req.URL.RawQuery = q.Encode()
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if err := verifyHeaders(resp.Header); err != nil {
		return err
	}

	tee := io.TeeReader(resp.Body, os.Stdout)
	defer os.Stdout.WriteString("\n")
	return json.NewDecoder(tee).Decode(dst)
}
