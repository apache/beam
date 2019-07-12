/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	df "google.golang.org/api/dataflow/v1b3"
	"reflect"
	"testing"
	"time"
)

var (
	currentTime   time.Time = time.Now()
	jobsReturned            = []*df.Job{}
	cancelledJobs           = []*df.Job{}
)

type fakeClient struct{}

func (c fakeClient) ListJobs(projectId string) ([]*df.Job, error) {
	return jobsReturned, nil
}

func (c fakeClient) CancelJob(job *df.Job) error {
	cancelledJobs = append(cancelledJobs, job)
	return nil
}

func (c fakeClient) CurrentTime() time.Time {
	return currentTime
}

func helperForJobCancel(t *testing.T, hoursStale float64, jobList []*df.Job, expectedCancelled []*df.Job) {
	var c fakeClient
	jobsReturned = jobList
	cancelledJobs = []*df.Job{}
	cleanDataflowJobs(c, "some-project-id", 2.0)
	if !reflect.DeepEqual(cancelledJobs, expectedCancelled) {
		t.Errorf("Cancelled arrays not as expected actual=%v, expected=%v", cancelledJobs, expectedCancelled)
	}
}

func TestEmptyJobList(t *testing.T) {
	helperForJobCancel(t, 2.0, []*df.Job{}, []*df.Job{})
}

func TestNotExpiredJob(t *testing.T) {
	// Just under 2 hours.
	createTime := currentTime.Add(-(2*time.Hour - time.Second))
	helperForJobCancel(t, 2.0, []*df.Job{&df.Job{CreateTime: createTime.Format(time.RFC3339)}}, []*df.Job{})
}

func TestExpiredJob(t *testing.T) {
	// Just over 2 hours.
	createTime := currentTime.Add(-(2*time.Hour + time.Second))
	job := &df.Job{CreateTime: createTime.Format(time.RFC3339)}
	helperForJobCancel(t, 2.0, []*df.Job{job}, []*df.Job{job})
}
