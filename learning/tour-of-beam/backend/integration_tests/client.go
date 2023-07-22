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

type ErrBadResponse struct {
	Code int
}

func (e *ErrBadResponse) Error() string {
	return fmt.Sprintf("http code %d", e.Code)
}

var (
	ExpectedHeaders = map[string]string{
		"Access-Control-Allow-Origin": "*",
		"Content-Type":                "application/json",
	}
)

func makeCorsHeaders(method string) map[string]string {
	return map[string]string{
		"User-Agent":                     "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:106.0) Gecko/20100101 Firefox/106.0",
		"Accept":                         "*/*",
		"Accept-Language":                "en-US,en;q=0.5",
		"Accept-Encoding":                "gzip, deflate, br",
		"Access-Control-Request-Method":  method,
		"Access-Control-Request-Headers": "authorization",
		"Referer":                        "http://localhost:40001/",
		"Origin":                         "http://localhost:40001",
		"Connection":                     "keep-alive",
		"Sec-Fetch-Dest":                 "empty",
		"Sec-Fetch-Mode":                 "cors",
		"Sec-Fetch-Site":                 "cross-site",
		"TE":                             "trailers",
	}
}

func verifyServerHeaders(header http.Header) error {
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

func PostUnitComplete(url, sdk, unitId, token string) (ErrorResponse, error) {
	var result ErrorResponse
	err := Do(&result, http.MethodPost, url, map[string]string{"sdk": sdk, "id": unitId},
		map[string]string{"Authorization": "Bearer " + token}, nil)
	return result, err
}

func PostUserCode(url, sdk, unitId, token string, body UserCodeRequest) (ErrorResponse, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return ErrorResponse{}, err
	}
	headers := map[string]string{
		"Content-Type":  "application/json",
		"Authorization": "Bearer " + token,
	}
	queryParams := map[string]string{"sdk": sdk, "id": unitId}

	var result ErrorResponse
	err = Post(&result, url, queryParams, headers, bytes.NewReader(raw))
	return result, err
}

func PostDeleteProgress(url, token string) (ErrorResponse, error) {
	var result ErrorResponse
	err := Do(&result, http.MethodPost, url, nil,
		map[string]string{"Authorization": "Bearer " + token}, nil)
	return result, err
}

func Post(dst interface{}, url string, queryParams, headers map[string]string, body io.Reader) error {
	if err := Options(http.MethodPost, url, queryParams); err != nil {
		return fmt.Errorf("pre-flight request error: %w", err)
	}
	return Do(dst, http.MethodPost, url, queryParams, headers, body)
}

func Get(dst interface{}, url string, queryParams, headers map[string]string) error {
	if err := Options(http.MethodGet, url, queryParams); err != nil {
		return fmt.Errorf("pre-flight request error: %w", err)
	}
	return Do(dst, http.MethodGet, url, queryParams, headers, nil)
}

func Options(method, url string, queryParams map[string]string) error {
	optionsHeaders := makeCorsHeaders(method)
	return Do(nil, http.MethodOptions, url, queryParams, optionsHeaders, nil)
}

// Generic HTTP call wrapper
// params:
// * dst: response struct pointer
// * url: request  url
// * query_params: url query params, as a map (we don't use multiple-valued params)
// * headers: client headers as a map
// * body: as io.Reader interface
func Do(dst interface{}, method, url string, queryParams, headers map[string]string, body io.Reader) error {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return err
	}
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

	if err := verifyServerHeaders(resp.Header); err != nil {
		return err
	}

	if method == http.MethodOptions {
		if resp.StatusCode != http.StatusNoContent {
			return fmt.Errorf("options request failed, http code %d", resp.StatusCode)
		}
		// don't proceed to json body decoding, there's none
		return nil
	}

	tee := io.TeeReader(resp.Body, os.Stdout)
	defer os.Stdout.WriteString("\n")
	if err := json.NewDecoder(tee).Decode(dst); err != nil {
		return fmt.Errorf("response decode err: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return &ErrBadResponse{resp.StatusCode}
	}
	return nil
}
