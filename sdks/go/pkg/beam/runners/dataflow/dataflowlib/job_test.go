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
	"fmt"
	"reflect"
	"testing"
)

func TestValidateWorkerSettings(t *testing.T) {
	ctx := context.Background()
	testsWithErr := []struct {
		name       string
		jobOptions JobOptions
		errMessage string
	}{
		{
			name: "test_zone_and_worker_region_mutual_exclusion",
			jobOptions: JobOptions{
				Zone:         "foo",
				WorkerRegion: "bar",
			},
			errMessage: "cannot use option zone with workerRegion; prefer either workerZone or workerRegion",
		},
		{
			name: "test_zone_and_worker_zone_mutual_exclusion",
			jobOptions: JobOptions{
				Zone:       "foo",
				WorkerZone: "bar",
			},
			errMessage: "cannot use option zone with workerZone; prefer workerZone",
		},
		{
			name: "test_worker_zone_and_worker_region_mutual_exclusion",
			jobOptions: JobOptions{
				WorkerRegion: "foo",
				WorkerZone:   "bar",
			},
			errMessage: "workerRegion and workerZone options are mutually exclusive",
		},
		{
			name: "test_experiment_worker_region_and_worker_region_mutual_exclusion",
			jobOptions: JobOptions{
				Experiments:  []string{"worker_region"},
				WorkerRegion: "bar",
			},
			errMessage: "experiment worker_region and option workerRegion are mutually exclusive",
		},
		{
			name: "test_experiment_worker_region_and_worker_zone_mutual_exclusion",
			jobOptions: JobOptions{
				Experiments: []string{"worker_region"},
				WorkerZone:  "bar",
			},
			errMessage: "experiment worker_region and option workerZone are mutually exclusive",
		},
		{
			name: "test_experiment_worker_region_and_zone_mutual_exclusion",
			jobOptions: JobOptions{
				Experiments: []string{"worker_region"},
				Zone:        "foo",
			},
			errMessage: "experiment worker_region and option Zone are mutually exclusive",
		},
		{
			name: "test_num_workers_cannot_be_negative",
			jobOptions: JobOptions{
				NumWorkers: -1,
			},
			errMessage: "num_workers (-1) cannot be negative",
		},
		{
			name: "test_max_num_workers_cannot_be_negative",
			jobOptions: JobOptions{
				MaxNumWorkers: -1,
			},
			errMessage: "max_num_workers (-1) cannot be negative",
		},
		{
			name: "test_num_workers_cannot_exceed_max_num_workers",
			jobOptions: JobOptions{
				NumWorkers:    43,
				MaxNumWorkers: 42,
			},
			errMessage: "num_workers (43) cannot exceed max_num_workers (42)",
		},
	}

	for _, test := range testsWithErr {
		t.Run(test.name, func(t *testing.T) {
			err := validateWorkerSettings(ctx, &test.jobOptions)
			if err == nil {
				t.Fatalf("expect error: %v, got no error", test.errMessage)
			}
			if err.Error() != test.errMessage {
				t.Fatalf("expect error: %v, got error: %v", test.errMessage, err.Error())
			}
		})
	}

	tests := []struct {
		name     string
		opts     JobOptions
		expected JobOptions
	}{
		{
			name:     "test_replace_worker_zone_with_zone",
			opts:     JobOptions{Zone: "foo"},
			expected: JobOptions{WorkerZone: "foo"},
		},
		{
			name:     "test_single_worker_zone",
			opts:     JobOptions{WorkerZone: "foo"},
			expected: JobOptions{WorkerZone: "foo"},
		},
		{
			name:     "test_single_worker_region",
			opts:     JobOptions{WorkerRegion: "foo"},
			expected: JobOptions{WorkerRegion: "foo"},
		},
		{
			name: "test_num_workers_can_equal_max_num_workers",
			opts: JobOptions{
				NumWorkers:    42,
				MaxNumWorkers: 42,
			},
			expected: JobOptions{
				NumWorkers:    42,
				MaxNumWorkers: 42,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateWorkerSettings(ctx, &test.opts)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(test.expected, test.opts) {
				t.Fatalf("expected job options: %v, got job options: %v", test.expected, test.opts)
			}
		})
	}
}

func TestCurrentStateMessage(t *testing.T) {
	tests := []struct {
		state   string
		term    bool
		want    string
		wantErr error
	}{
		{state: "JOB_STATE_DONE", want: "Job JorbID-09876 succeeded!", term: true},
		{state: "JOB_STATE_DRAINED", want: "Job JorbID-09876 drained", term: true},
		{state: "JOB_STATE_UPDATED", want: "Job JorbID-09876 updated", term: true},
		{state: "JOB_STATE_CANCELLED", want: "Job JorbID-09876 cancelled", term: true},
		{state: "JOB_STATE_RUNNING", want: "Job still running ...", term: false},
		{state: "JOB_STATE_FAILED", wantErr: fmt.Errorf("Job JorbID-09876 failed"), term: true},
		{state: "Ossiphrage", want: "Job state: Ossiphrage ...", term: false},
	}
	for _, test := range tests {
		t.Run(test.state, func(t *testing.T) {
			const jobID = "JorbID-09876"
			term, got, err := currentStateMessage(test.state, jobID)
			if term != test.term {
				termGot, termWant := "false (continues)", "true (terminal)"
				if !test.term {
					termGot, termWant = termWant, termGot
				}
				t.Errorf("currentStateMessage(%v, %q) = %v, want %v", test.state, jobID, termGot, termWant)
			}
			if err != nil && err.Error() != test.wantErr.Error() {
				t.Errorf("currentStateMessage(%v, %q) = %v, want %v", test.state, jobID, err, test.wantErr)
			}
			if got != test.want {
				t.Errorf("currentStateMessage(%v, %q) = %v, want %v", test.state, jobID, got, test.want)
			}
		})
	}
}
