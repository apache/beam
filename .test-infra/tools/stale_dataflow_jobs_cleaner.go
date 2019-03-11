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
	"context"
	"log"
	"strings"
	"time"

	"golang.org/x/oauth2/google"
	df "google.golang.org/api/dataflow/v1b3"
)

const (
	longRunningPrefix = "long-running-"
)

// client contains methods for listing and cancelling jobs, extracted to allow easier testing.
type client interface {
	CurrentTime() time.Time
	ListJobs(projectId string) ([]*df.Job, error)
	CancelJob(job *df.Job) error
}

// dataflowClient implements the client interface for Google Cloud Dataflow.
type dataflowClient struct {
	s *df.ProjectsJobsService
}

// newDataflowClient creates a new Dataflow ProjectsJobsService.
func newDataflowClient() (*dataflowClient, error) {
	ctx := context.Background()
	cl, err := google.DefaultClient(ctx, df.CloudPlatformScope)
	if err != nil {
		return nil, err
	}
	service, err := df.New(cl)
	if err != nil {
		return nil, err
	}
	return &dataflowClient{s: df.NewProjectsJobsService(service)}, nil
}

// CurrentTime gets the time Now.
func (c dataflowClient) CurrentTime() time.Time {
	return time.Now()
}

// ListJobs lists the active Dataflow jobs for a project.
func (c dataflowClient) ListJobs(projectId string) ([]*df.Job, error) {
	resp, err := c.s.Aggregated(projectId).Filter("ACTIVE").Fields("jobs(id,name,projectId,createTime)").Do()
	if err != nil {
		return nil, err
	}
	return resp.Jobs, nil
}

// CancelJob requests the cancellation od a Dataflow job.
func (c dataflowClient) CancelJob(job *df.Job) error {
	jobDone := df.Job{
		RequestedState: "JOB_STATE_DONE",
	}
	_, err := c.s.Update(job.ProjectId, job.Id, &jobDone).Do()
	return err
}

// cleanDataflowJobs cancels stale Dataflow jobs, excluding the longRunningPrefix prefixed jobs.
func cleanDataflowJobs(c client, projectId string, hoursStale float64) error {
	now := c.CurrentTime()
	jobs, err := c.ListJobs(projectId)
	if err != nil {
		return err
	}
	for _, j := range jobs {
		t, err := time.Parse(time.RFC3339, j.CreateTime)
		if err != nil {
			return err
		}
		hoursSinceCreate := now.Sub(t).Hours()
		log.Printf("Job %v %v %v %v %.2f\n", j.ProjectId, j.Id, j.Name, j.CreateTime, hoursSinceCreate)
		if hoursSinceCreate > hoursStale && !strings.HasPrefix(j.Name, longRunningPrefix) {
			log.Printf("Attempting to cancel %v\n", j.Id)
			c.CancelJob(j)
		}
	}
	return nil
}

func main() {
	client, err := newDataflowClient()
	if err != nil {
		log.Fatalf("Error creating dataflow client, %v", err)
	}
	// Cancel any jobs older than 3 hours.
	err = cleanDataflowJobs(client, "apache-beam-testing", 3.0)
	if err != nil {
		log.Fatalf("Error cleaning dataflow jobs, %v", err)
	}
	log.Printf("Done")
}
