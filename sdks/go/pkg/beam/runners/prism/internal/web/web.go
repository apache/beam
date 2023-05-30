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

// Package web serves a web UI for Prism when it is operating as a stand alone runner.
// It's not
package web

import (
	"bytes"
	"context"
	"embed"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"sort"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/metrics"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/metricsx"
	jobpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/jobmanagement_v1"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"golang.org/x/exp/slog"
	"golang.org/x/sync/errgroup"
)

//go:embed index.html
var indexTemplate string

//go:embed jobdetails.html
var jobTemplate string

//go:embed assets/*
var assets embed.FS

type Tforms struct {
	ID        string
	Transform *pipepb.PTransform
	Metrics   []string
}

// Initialize the web client to talk to the given Job Management Client.
func Initialize(ctx context.Context, port int, jobcli jobpb.JobServiceClient) {
	indexPage := template.Must(template.New("index").Parse(indexTemplate))
	jobPage := template.Must(template.New("job").Parse(jobTemplate))
	assetsFs := http.FileServer(http.FS(assets))

	mux := http.NewServeMux()

	mux.Handle("/assets/", assetsFs)
	mux.HandleFunc("/job/", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.EscapedPath()
		i := strings.LastIndex(path, "/")
		jobID := path[i+1:]
		w.WriteHeader(200)
		data := struct {
			Title          string
			WelcomeMessage string
			Transforms     []Tforms
			PCols          map[metrics.StepKey]metrics.PColResult
		}{
			Title:          "Hello from 	Go",
			WelcomeMessage: "I am embedded in the program!",
		}

		errg, ctx := errgroup.WithContext(ctx)

		var pipeResp *jobpb.GetJobPipelineResponse
		var metsResp *jobpb.GetJobMetricsResponse
		errg.Go(func() error {
			resp, err := jobcli.GetPipeline(ctx, &jobpb.GetJobPipelineRequest{JobId: jobID})
			pipeResp = resp
			return err
		})
		errg.Go(func() error {
			resp, err := jobcli.GetJobMetrics(ctx, &jobpb.GetJobMetricsRequest{JobId: jobID})
			metsResp = resp
			return err
		})

		if err := errg.Wait(); err != nil {
			data.Title = "Error on GetPipeline or GetMetrics"
			data.WelcomeMessage = err.Error()
		} else {
			data.Title = jobID
			mets := metsResp.GetMetrics()
			results := metricsx.FromMonitoringInfos(pipeResp.GetPipeline(), mets.GetAttempted(), mets.GetCommitted())

			pcols := map[metrics.StepKey]metrics.PColResult{}
			for _, res := range results.AllMetrics().PCols() {
				pcols[res.Key] = res
			}
			data.PCols = pcols
			trs := pipeResp.GetPipeline().GetComponents().GetTransforms()
			data.Transforms = make([]Tforms, 0, len(trs))
			for id, pt := range pipeResp.GetPipeline().GetComponents().GetTransforms() {
				if len(pt.GetSubtransforms()) > 0 {
					continue
				}
				var strMets []string
				for local := range pt.GetInputs() {
					name := pt.GetUniqueName() + "." + local
					r, ok := pcols[metrics.StepKey{Step: name}]
					if ok {
						strMets = append(strMets, fmt.Sprintf("Input %v ElementCount: %v", name, r.Committed.ElementCount))
					}
				}

				data.Transforms = append(data.Transforms, Tforms{
					ID:        id,
					Transform: pt,
					Metrics:   strMets,
				})
			}
			sort.Slice(data.Transforms, func(i, j int) bool {
				a, b := data.Transforms[i], data.Transforms[j]
				return a.Transform.GetUniqueName() < b.Transform.GetUniqueName()
			})
		}
		var buf bytes.Buffer
		if err := jobPage.Execute(&buf, data); err != nil {
			data.Title = "Error on Template parsing"
			data.WelcomeMessage = err.Error()
		}
		w.Write(buf.Bytes())
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		data := struct {
			Title          string
			WelcomeMessage string
			Jobs           []*jobpb.JobInfo
		}{
			Title:          "Hello from Go",
			WelcomeMessage: "I am embedded in the program!",
		}

		resp, err := jobcli.GetJobs(ctx, &jobpb.GetJobsRequest{})
		if err != nil {
			data.Title = "Error on GetJobs"
			data.WelcomeMessage = err.Error()
		} else {
			data.Title = fmt.Sprintf("there are %v jobs", len(resp.GetJobInfo()))
			data.Jobs = resp.GetJobInfo()
		}
		sort.Slice(data.Jobs, func(i, j int) bool {
			a, b := data.Jobs[i], data.Jobs[j]
			return a.JobId < b.JobId
		})
		var buf bytes.Buffer
		err = indexPage.Execute(&buf, data)
		if err != nil {
			data.Title = "Error on Template parsing"
			data.WelcomeMessage = err.Error()
		}
		w.Write(buf.Bytes())
	})

	endpoint := fmt.Sprintf(":%d", port)

	slog.Info("Serving WebUI", slog.String("endpoint", endpoint))
	err := http.ListenAndServe(endpoint, mux)

	if err != nil {
		log.Fatal(err)
	}
}
