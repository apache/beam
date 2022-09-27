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
	"io"
	"net/http"
	"os"
)

func SdkList(url string) (sdkListResponse, error) {
	var result sdkListResponse
	err := Get(&result, url, nil)
	return result, err
}

func GetContentTree(url, sdk string) (ContentTree, error) {
	var result ContentTree
	err := Get(&result, url, map[string]string{"sdk": sdk})
	return result, err
}

func GetUnitContent(url, sdk, unitId string) (Unit, error) {
	var result Unit
	err := Get(&result, url, map[string]string{"sdk": sdk, "unitId": unitId})
	return result, err
}

// Generic HTTP call wrapper
// params:
// * dst: response struct pointer
// * url: request  url
// * query_params: url query params, as a map (we don't use multiple-valued params)
func Get(dst interface{}, url string, queryParams map[string]string) error {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")
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

	tee := io.TeeReader(resp.Body, os.Stdout)
	return json.NewDecoder(tee).Decode(dst)
}
