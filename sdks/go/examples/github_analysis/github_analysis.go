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

// github_analysis is an example pipeline for gathering metrics from GitHub issues
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"reflect"
	"regexp"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/sdf"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/rtrackers/offsetrange"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	log2 "github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/stats"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

var (
	repo = flag.String("repo", "damccorm/github-analysis-pipeline-go", "Repo to read from, formatted owner/repo")

	githubToken = flag.String("githubToken", "", "Token for reading/writing to GitHub repo (required).")

	output = flag.String("output", "", "Output file (required).")
)

func init() {
	beam.RegisterType(reflect.TypeOf((*extractFn)(nil)))
}

// extractFn is a DoFn that emits the words in a given line and keeps a count for small words.
type extractFn struct {
	token string
}

func (fn *extractFn) CreateInitialRestriction(filename string) offsetrange.Restriction {
	return offsetrange.Restriction{
		Start: 1,
		End:   100000,
	}
}

func (fn *extractFn) CreateTracker(rest offsetrange.Restriction) *sdf.LockRTracker {
	return sdf.NewLockRTracker(offsetrange.NewTracker(rest))
}

func (fn *extractFn) RestrictionSize(_ string, rest offsetrange.Restriction) float64 {
	size := rest.Size()
	return size
}

func (fn *extractFn) SplitRestriction(_ string, rest offsetrange.Restriction) []offsetrange.Restriction {
	return []offsetrange.Restriction{rest}
}

type IssueResponse struct {
	Created string `json:"created_at"`
	Body    string `json:"body"`
}

func getIssueDescriptionAndTimetamp(ctx context.Context, repo string, issueNumber int64) (string, time.Time, bool, bool) {
	targetUrl := fmt.Sprintf("https://api.github.com/repos/%v/issues/%v", repo, issueNumber)
	resp, err := http.Get(targetUrl)
	if err != nil {
		log2.Warnf(ctx, "Oh no 1, %v", err)
		return "", time.Now(), false, false
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log2.Warnf(ctx, "Oh no 2, %v", err)
		return "", time.Now(), false, false
	}
	sb := string(body)
	data := IssueResponse{}
	json.Unmarshal([]byte(sb), &data)
	if data.Created == "" || data.Body == "" {
		return "", time.Now(), false, true
	}
	t, err := time.Parse("2006-01-02T15:04:05Z", data.Created)
	if err != nil {
		log2.Warnf(ctx, "Oh no 3: %v", err)
		return "", time.Now(), false, false
	}
	return data.Body, t, true, false
}

func (f *extractFn) ProcessElement(ctx context.Context, e *sdf.TimestampObservingWatermarkEstimator, rt *sdf.LockRTracker, repo string, emit func(beam.EventTime, string)) sdf.ProcessContinuation {
	log2.Warn(ctx, "Entering ProcessElement")
	i := rt.GetRestriction().(offsetrange.Restriction).Start
	d, t, ok, updateWatermark := getIssueDescriptionAndTimetamp(ctx, repo, i)
	if !ok {
		if updateWatermark {
			e.ObserveTimestamp(time.Now())
		}

		// Failed to get issue description, either due to github issue or it not existing
		// Resume later
		log2.Warnf(ctx, "Resuming in 5 minutes")
		return sdf.ResumeProcessingIn(5 * time.Minute)
	}
	for rt.TryClaim(i) {
		log2.Warnf(ctx, "Claiming element %v", i)
		emit(mtime.FromTime(t), d)
		i++
		d, t, ok, updateWatermark = getIssueDescriptionAndTimetamp(ctx, repo, i)
		if !ok {
			if updateWatermark {
				e.ObserveTimestamp(time.Now())
			}

			// Failed to get issue description, either due to github issue or it not existing
			// Resume later
			log2.Warnf(ctx, "Resuming in 5 minutes")
			return sdf.ResumeProcessingIn(5 * time.Minute)
		}
	}
	if rt.GetError() != nil || rt.IsDone() {
		log2.Warnf(ctx, "Oh no 4: %v, %v", i, rt.GetError())
		// Stop processing on error or completion
		return sdf.StopProcessing()
	} else {
		log2.Warnf(ctx, "Oh no 5: %v", i)
		// Failed to claim but no error, resume later.
		return sdf.ResumeProcessingIn(5 * time.Second)
	}
	return nil
}

func (fn *extractFn) CreateWatermarkEstimator(t time.Time) *sdf.TimestampObservingWatermarkEstimator {
	return &sdf.TimestampObservingWatermarkEstimator{State: t}
}

func (fn *extractFn) InitialWatermarkEstimatorState(_ beam.EventTime, _ offsetrange.Restriction, _ string) time.Time {
	return time.Date(2022, 5, 1, 1, 1, 1, 1, time.UTC)
}

func (fn *extractFn) WatermarkEstimatorState(e *sdf.TimestampObservingWatermarkEstimator) time.Time {
	return e.State
}

var (
	wordRE = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)
)

func extractWords(description string, emit func(string)) {
	for _, word := range wordRE.FindAllString(description, -1) {
		emit(word)
	}
}

func formatFn(w string, c int) string {
	return fmt.Sprintf("%s: %v", w, c)
}

func main() {
	flag.Parse()
	beam.Init()

	// Input validation is done as usual. Note that it must be after Init().
	if *output == "" {
		log.Fatal("No output provided")
	}

	if *githubToken == "" {
		log.Fatal("No githubToken provided")
	}

	p := beam.NewPipeline()
	s := p.Root()

	imp := beam.Create(s, *repo)
	issues := beam.ParDo(s, &extractFn{token: *githubToken}, imp)
	wIssues := beam.WindowInto(s,
		window.NewFixedWindows(5*time.Minute),
		issues)
	words := beam.ParDo(s, extractWords, wIssues)
	counted := stats.Count(s, words)
	formatted := beam.ParDo(s, formatFn, counted)
	textio.Write(s, *output, formatted)

	// Concept #1: The beamx.Run convenience wrapper allows a number of
	// pre-defined runners to be used via the --runner flag.
	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
