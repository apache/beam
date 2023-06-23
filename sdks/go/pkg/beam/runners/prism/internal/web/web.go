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

// Package web serves a web UI for Prism.
//
// Assets are embedded allowing the UI to be served from arbitrary binaries.
package web

import (
	"bytes"
	"context"
	"embed"
	"fmt"
	"html/template"
	"net/http"
	"sort"
	"strings"
	"sync"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/metrics"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/metricsx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/pipelinex"
	jobpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/jobmanagement_v1"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"golang.org/x/exp/maps"
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

type pTransform struct {
	ID        string
	Transform *pipepb.PTransform
	Metrics   []string
}

type errorHolder struct {
	Error string
}

func (jd *errorHolder) SetError(err error) {
	jd.Error = err.Error()
}

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

// Page Handlers and Data.

type jobDetailsData struct {
	JobID, JobName string
	State          jobpb.JobState_Enum
	Transforms     []pTransform
	PCols          map[metrics.StepKey]metrics.PColResult
	DisplayData    []*pipepb.LabelledPayload

	errorHolder
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

	// Job names aren't included in pipeline information.
	// Retain a lookup cache of ids to names and update them here if not present.
	errg.Go(func() error {
		v, ok := h.jobDetails.Load(jobID)
		if ok {
			data.JobName = v.(string)
			return nil
		}
		// Name isn't available, so update the cache for future requests.
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
	var stateResp *jobpb.JobStateEvent
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
	errg.Go(func() error {
		resp, err := h.Jobcli.GetState(ctx, &jobpb.GetJobStateRequest{JobId: jobID})
		stateResp = resp
		return err
	})

	if err := errg.Wait(); err != nil {
		data.Error = err.Error()
		renderPage(jobPage, &data, w)
		return
	}

	data.State = stateResp.GetState()
	for i, dd := range pipeResp.GetPipeline().GetDisplayData() {
		if dd.GetUrn() != "beam:display_data:labelled:v1" {
			// There's only one type of display data, but let's take care.
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
	allMetsPCol := results.AllMetrics().PCols()
	for _, res := range allMetsPCol {
		pcols[res.Key] = res
	}

	data.PCols = pcols
	trs := pipeResp.GetPipeline().GetComponents().GetTransforms()
	col2T, topo := preprocessTransforms(trs)

	data.Transforms = make([]pTransform, 0, len(trs))
	for _, id := range topo {
		pt := trs[id]
		var inMets []string
		locals := maps.Keys(pt.GetInputs())
		sort.Strings(locals)
		for _, local := range locals {
			global := pt.GetInputs()[local]
			inP := col2T[global]
			name := inP.T.GetUniqueName() + "." + local
			r, ok := pcols[metrics.StepKey{Step: name}]
			if ok {
				inMets = append(inMets, fmt.Sprintf("\n- %v: %v", name, r.Committed.ElementCount))
			}
		}

		var strMets []string
		if len(inMets) > 0 {
			strMets = append(strMets, "Inputs read")
			strMets = append(strMets, inMets...)
		}
		var outMets []string
		locals = maps.Keys(pt.GetOutputs())
		sort.Strings(locals)
		for _, local := range locals {
			name := pt.GetUniqueName() + "." + local
			r, ok := pcols[metrics.StepKey{Step: name}]
			if ok {
				outMets = append(outMets, fmt.Sprintf("\n- %v: %v", name, r.Committed.ElementCount))
			}
		}
		if len(outMets) > 0 {
			strMets = append(strMets, "Outputs written")
			strMets = append(strMets, outMets...)
		}

		data.Transforms = append(data.Transforms, pTransform{
			ID:        id,
			Transform: pt,
			Metrics:   strMets,
		})
	}

	renderPage(jobPage, &data, w)
}

type pcolParent struct {
	L string
	T *pipepb.PTransform
}

// preprocessTransforms returns the leaf transform parents of pcollections
// and the topological ordering of leaf transforms.
func preprocessTransforms(trs map[string]*pipepb.PTransform) (map[string]pcolParent, []string) {
	ret := map[string]pcolParent{}
	var leaves []string
	for id, t := range trs {
		// Skip composites at this time.
		if len(t.GetSubtransforms()) > 0 {
			continue
		}
		leaves = append(leaves, id)
		for local, global := range t.GetOutputs() {
			ret[global] = pcolParent{
				L: local,
				T: t,
			}
		}
	}
	return ret, pipelinex.TopologicalSort(trs, leaves)
}

type jobsConsoleHandler struct {
	Jobcli jobpb.JobServiceClient
}

type jobsConsoleData struct {
	Jobs []*jobpb.JobInfo

	errorHolder
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
func Initialize(ctx context.Context, port int, jobcli jobpb.JobServiceClient) error {
	assetsFs := http.FileServer(http.FS(assets))
	mux := http.NewServeMux()

	mux.Handle("/assets/", assetsFs)
	mux.Handle("/job/", &jobDetailsHandler{Jobcli: jobcli})
	mux.Handle("/", &jobsConsoleHandler{Jobcli: jobcli})

	endpoint := fmt.Sprintf("localhost:%d", port)

	slog.Info("Serving WebUI", slog.String("endpoint", "http://"+endpoint))
	return http.ListenAndServe(endpoint, mux)
}
