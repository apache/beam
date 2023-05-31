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
	"sync"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/metrics"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/metricsx"
	jobpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/jobmanagement_v1"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"golang.org/x/exp/slog"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

//go:embed index.html
var indexTemplate string

//go:embed jobdetails.html
var jobTemplate string

//go:embed assets/*
var assets embed.FS

var (
	indexPage = template.Must(template.New("index").Parse(indexTemplate))
	jobPage   = template.Must(template.New("job").Parse(jobTemplate))
)

type errorSetter interface {
	SetError(err error)
}

func renderPage(page *template.Template, data errorSetter, w http.ResponseWriter) {
	var buf bytes.Buffer
	if err := page.Execute(&buf, data); err != nil {
		data.SetError(err)
	}
	w.Write(buf.Bytes())
}

type Tforms struct {
	ID        string
	Transform *pipepb.PTransform
	Metrics   []string
}

type jobDetailsData struct {
	JobID, JobName string
	Error          string
	Transforms     []Tforms
	PCols          map[metrics.StepKey]metrics.PColResult
	DisplayData    []*pipepb.LabelledPayload
}

func (jd *jobDetailsData) SetError(err error) {
	jd.Error = err.Error()
}

type jobDetailsHandler struct {
	Jobcli     jobpb.JobServiceClient
	jobDetails sync.Map
}

func (h *jobDetailsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(200)

	path := r.URL.EscapedPath()
	i := strings.LastIndex(path, "/")
	jobID := path[i+1:]
	data := jobDetailsData{
		JobID: jobID,
	}

	errg, ctx := errgroup.WithContext(r.Context())
	errg.Go(func() error {
		v, ok := h.jobDetails.Load(jobID)
		if ok {
			data.JobName = v.(string)
			return nil
		}
		resp, err := h.Jobcli.GetJobs(ctx, &jobpb.GetJobsRequest{})
		if err != nil {
			return err
		}
		for _, j := range resp.GetJobInfo() {
			if j.GetJobId() == jobID {
				data.JobName = j.GetJobName()
			}
			h.jobDetails.Store(j.GetJobId(), j.GetJobName())
		}
		return nil
	})

	var pipeResp *jobpb.GetJobPipelineResponse
	var metsResp *jobpb.GetJobMetricsResponse
	errg.Go(func() error {
		resp, err := h.Jobcli.GetPipeline(ctx, &jobpb.GetJobPipelineRequest{JobId: jobID})
		pipeResp = resp
		return err
	})
	errg.Go(func() error {
		resp, err := h.Jobcli.GetJobMetrics(ctx, &jobpb.GetJobMetricsRequest{JobId: jobID})
		metsResp = resp
		return err
	})

	if err := errg.Wait(); err != nil {
		data.Error = err.Error()
		renderPage(jobPage, &data, w)
		return
	}

	for i, dd := range pipeResp.GetPipeline().GetDisplayData() {
		if dd.GetUrn() != "beam:display_data:labelled:v1" {
			// There's only one type of display data, but lets take care.
			continue
		}

		lbl := &pipepb.LabelledPayload{}
		if err := proto.Unmarshal(dd.GetPayload(), lbl); err != nil {
			slog.Debug("unable to decode DisplayData payload", slog.Any("error", err), slog.Any("index", i))
			continue
		}
		data.DisplayData = append(data.DisplayData, lbl)
	}

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

	renderPage(jobPage, &data, w)
}

type jobsConsoleHandler struct {
	Jobcli jobpb.JobServiceClient
}

type jobsConsoleData struct {
	Error string
	Jobs  []*jobpb.JobInfo
}

func (jd *jobsConsoleData) SetError(err error) {
	jd.Error = err.Error()
}

func (h *jobsConsoleHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(200)
	data := &jobsConsoleData{}
	resp, err := h.Jobcli.GetJobs(r.Context(), &jobpb.GetJobsRequest{})
	if err != nil {
		data.SetError(err)
		renderPage(indexPage, data, w)
		return
	}
	data.Jobs = resp.GetJobInfo()
	sort.Slice(data.Jobs, func(i, j int) bool {
		a, b := data.Jobs[i], data.Jobs[j]
		return a.JobId < b.JobId
	})

	renderPage(indexPage, data, w)
}

// Initialize the web client to talk to the given Job Management Client.
func Initialize(ctx context.Context, port int, jobcli jobpb.JobServiceClient) {
	assetsFs := http.FileServer(http.FS(assets))
	mux := http.NewServeMux()

	mux.Handle("/assets/", assetsFs)
	mux.Handle("/job/", &jobDetailsHandler{Jobcli: jobcli})
	mux.Handle("/", &jobsConsoleHandler{Jobcli: jobcli})

	endpoint := fmt.Sprintf("localhost:%d", port)

	slog.Info("Serving WebUI", slog.String("endpoint", "http://"+endpoint))
	err := http.ListenAndServe(endpoint, mux)

	if err != nil {
		log.Fatal(err)
	}
}
