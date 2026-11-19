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

package dataflowlib

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/oauth2"
	df "google.golang.org/api/dataflow/v1b3"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

func TestWaitForCompletion(t *testing.T) {
	tests := []struct {
		name      string
		statuses  []int
		state     string
		wantCalls int32
		wantCode  int
		wantError string
	}{
		{name: "done", statuses: []int{http.StatusOK}, state: "JOB_STATE_DONE", wantCalls: 1},
		{name: "unavailable_then_done", statuses: []int{http.StatusServiceUnavailable, http.StatusOK}, state: "JOB_STATE_DONE", wantCalls: 2},
		{name: "rate_limited_then_done", statuses: []int{http.StatusTooManyRequests, http.StatusOK}, state: "JOB_STATE_DONE", wantCalls: 2},
		{name: "connection_closed_then_done", statuses: []int{0, http.StatusOK}, state: "JOB_STATE_DONE", wantCalls: 2},
		{name: "unauthorized", statuses: []int{http.StatusUnauthorized}, wantCalls: 1, wantCode: http.StatusUnauthorized},
		{name: "forbidden", statuses: []int{http.StatusForbidden}, wantCalls: 1, wantCode: http.StatusForbidden},
		{name: "not_found", statuses: []int{http.StatusNotFound}, wantCalls: 1, wantCode: http.StatusNotFound},
		{name: "retries_exhausted", statuses: []int{http.StatusServiceUnavailable}, wantCalls: 5, wantCode: http.StatusServiceUnavailable, wantError: "may still be running"},
		{name: "job_failed", statuses: []int{http.StatusOK}, state: "JOB_STATE_FAILED", wantCalls: 1, wantError: "Job test-job failed"},
		{name: "unavailable_then_job_failed", statuses: []int{http.StatusServiceUnavailable, http.StatusOK}, state: "JOB_STATE_FAILED", wantCalls: 2, wantError: "Job test-job failed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Path != "/v1b3/projects/test-project/locations/us-central1/jobs/test-job" {
					t.Errorf("unexpected request: %s %s", r.Method, r.URL)
				}
				index := min(int(calls.Add(1))-1, len(test.statuses)-1)
				status := test.statuses[index]
				if status == 0 {
					conn, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Errorf("hijack connection: %v", err)
						return
					}
					conn.Close()
					return
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(status)
				if status == http.StatusOK {
					json.NewEncoder(w).Encode(&df.Job{CurrentState: test.state})
				} else {
					json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": status, "message": "status request failed"}})
				}
			}))
			defer server.Close()
			client, err := df.NewService(context.Background(), option.WithEndpoint(server.URL+"/"), option.WithHTTPClient(server.Client()))
			if err != nil {
				t.Fatal(err)
			}
			err = WaitForCompletion(context.Background(), client, "test-project", "us-central1", "test-job")
			if test.wantCode != 0 {
				var apiError *googleapi.Error
				if !errors.As(err, &apiError) || apiError.Code != test.wantCode {
					t.Errorf("WaitForCompletion() error = %v, want API error %d", err, test.wantCode)
				}
			} else if test.wantError == "" && err != nil {
				t.Errorf("WaitForCompletion() = %v, want nil", err)
			}
			if test.wantError != "" && (err == nil || !strings.Contains(err.Error(), test.wantError)) {
				t.Errorf("WaitForCompletion() error = %v, want %q", err, test.wantError)
			}
			if got := calls.Load(); got != test.wantCalls {
				t.Errorf("status requests = %d, want %d", got, test.wantCalls)
			}
		})
	}
}

func TestWaitForCompletionCanceledDuringRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		time.AfterFunc(100*time.Millisecond, cancel)
	}))
	defer server.Close()
	client, err := df.NewService(context.Background(), option.WithEndpoint(server.URL+"/"), option.WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatal(err)
	}
	if err := WaitForCompletion(ctx, client, "test-project", "us-central1", "test-job"); !errors.Is(err, context.Canceled) {
		t.Errorf("WaitForCompletion() = %v, want context.Canceled", err)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("status requests = %d, want 1", got)
	}
}

func TestWaitForCompletionCanceled(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		json.NewEncoder(w).Encode(&df.Job{CurrentState: "JOB_STATE_DONE"})
	}))
	defer server.Close()
	client, err := df.NewService(context.Background(), option.WithEndpoint(server.URL+"/"), option.WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := WaitForCompletion(ctx, client, "test-project", "us-central1", "test-job"); !errors.Is(err, context.Canceled) {
		t.Errorf("WaitForCompletion() = %v, want context.Canceled", err)
	}
	if got := calls.Load(); got != 0 {
		t.Errorf("status requests = %d, want 0", got)
	}
}

func TestWaitForCompletionTokenErrors(t *testing.T) {
	tests := []struct {
		name      string
		statuses  []int
		wantCalls int32
		wantCode  int
	}{
		{name: "unauthorized", statuses: []int{http.StatusUnauthorized}, wantCalls: 1, wantCode: http.StatusUnauthorized},
		{name: "forbidden", statuses: []int{http.StatusForbidden}, wantCalls: 1, wantCode: http.StatusForbidden},
		{name: "bad_request", statuses: []int{http.StatusBadRequest}, wantCalls: 1, wantCode: http.StatusBadRequest},
		{name: "unavailable_then_authorized", statuses: []int{http.StatusServiceUnavailable, http.StatusOK}, wantCalls: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var tokenCalls, jobCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path == "/token" {
					if r.Method != http.MethodPost {
						t.Errorf("token request method = %s, want POST", r.Method)
					}
					index := min(int(tokenCalls.Add(1))-1, len(test.statuses)-1)
					status := test.statuses[index]
					w.WriteHeader(status)
					if status == http.StatusOK {
						json.NewEncoder(w).Encode(map[string]any{"access_token": "test-access-token", "token_type": "Bearer", "expires_in": 3600})
					} else {
						json.NewEncoder(w).Encode(map[string]string{"error": "invalid_grant", "error_description": "token request failed"})
					}
					return
				}
				jobCalls.Add(1)
				if got := r.Header.Get("Authorization"); got != "Bearer test-access-token" {
					t.Errorf("Authorization = %q, want test access token", got)
				}
				json.NewEncoder(w).Encode(&df.Job{CurrentState: "JOB_STATE_DONE"})
			}))
			defer server.Close()
			ctx := context.Background()
			config := oauth2.Config{
				ClientID: "test-client",
				Endpoint: oauth2.Endpoint{TokenURL: server.URL + "/token", AuthStyle: oauth2.AuthStyleInParams},
			}
			tokenSource := config.TokenSource(ctx, &oauth2.Token{RefreshToken: "test-refresh-token"})
			client, err := df.NewService(ctx, option.WithEndpoint(server.URL+"/"), option.WithHTTPClient(oauth2.NewClient(ctx, tokenSource)))
			if err != nil {
				t.Fatal(err)
			}
			err = WaitForCompletion(ctx, client, "test-project", "us-central1", "test-job")
			var wantJobCalls int32 = 1
			if test.wantCode != 0 {
				wantJobCalls = 0
				var tokenError *oauth2.RetrieveError
				if !errors.As(err, &tokenError) || tokenError.Response.StatusCode != test.wantCode {
					t.Errorf("WaitForCompletion() = %v, want token error %d", err, test.wantCode)
				}
			} else if err != nil {
				t.Errorf("WaitForCompletion() = %v, want nil", err)
			}
			if got := tokenCalls.Load(); got != test.wantCalls {
				t.Errorf("token requests = %d, want %d", got, test.wantCalls)
			}
			if got := jobCalls.Load(); got != wantJobCalls {
				t.Errorf("job requests = %d, want %d", got, wantJobCalls)
			}
		})
	}
}
