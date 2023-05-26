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

	jobpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/jobmanagement_v1"
	"golang.org/x/exp/slog"
)

//go:embed index.html
var indexTemplate string

//go:embed assets/*
var assets embed.FS

// Initialize the web client to talk to the given Job Management Client.
func Initialize(ctx context.Context, port int, jobcli jobpb.JobServiceClient) {
	assetsFs := http.FileServer(http.FS(assets))

	mux := http.NewServeMux()

	mux.Handle("/assets/", assetsFs)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)

		template, err := template.New("index").Parse(indexTemplate)
		if err != nil {
			w.WriteHeader(500)
			return
		}

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
		err = template.Execute(&buf, data)
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
