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

package web

import (
	"fmt"
	"runtime/debug"
	"runtime/metrics"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
)

type debugzData struct {
	Metrics []goRuntimeMetric

	errorHolder
}

type goRuntimeMetric struct {
	Name, Value, Description string
}

func dumpMetrics() debugzData {
	// Get descriptions for all supported metrics.
	descs := metrics.All()

	// Create a sample for each metric.
	samples := make([]metrics.Sample, len(descs))
	for i := range samples {
		samples[i].Name = descs[i].Name
	}

	// Sample the metrics. Re-use the samples slice if you can!
	metrics.Read(samples)

	var data debugzData

	// Iterate over all results.
	for i, sample := range samples {
		// Pull out the name and value.
		name, value := sample.Name, sample.Value

		m := goRuntimeMetric{
			Name:        strings.TrimSpace(name),
			Description: descs[i].Description,
		}

		// Handle each sample.
		switch value.Kind() {
		case metrics.KindUint64:
			if strings.HasSuffix(name, "bytes") {
				m.Value = humanize.Bytes(value.Uint64())
			} else {
				m.Value = humanize.FormatInteger("", int(value.Uint64()))
			}
		case metrics.KindFloat64:
			if strings.HasSuffix(name, "seconds") {
				m.Value = time.Duration(float64(time.Second) * value.Float64()).String()
			} else {
				m.Value = humanize.FormatFloat("", value.Float64())
			}
		case metrics.KindFloat64Histogram:
			m.Value = fmt.Sprintf("%f", medianBucket(value.Float64Histogram()))
			// The histogram may be quite large, so let's just pull out
			// a crude estimate for the median for the sake of this example.
		case metrics.KindBad:
			// This should never happen because all metrics are supported
			// by construction.
			m.Value = "bug in runtime/metrics package: KindBad"
		default:
			// This may happen as new metrics get added.
			//
			// The safest thing to do here is to simply log it somewhere
			// as something to look into, but ignore it for now.
			// In the worst case, you might temporarily miss out on a new metric.
			m.Value = fmt.Sprintf("%s: unexpected metric Kind: %v - %s\n", name, value.Kind(), descs[i].Description)
		}
		data.Metrics = append(data.Metrics, m)
	}

	var b strings.Builder
	buildInfo(&b)

	data.Metrics = append(data.Metrics, goRuntimeMetric{
		Name:        "BUILD INFO",
		Value:       b.String(),
		Description: "result from runtime/debug.ReadBuildInfo()",
	})

	b.Reset()
	goroutineDump(&b)
	data.Metrics = append(data.Metrics, goRuntimeMetric{
		Name:        "GOROUTINES",
		Value:       b.String(),
		Description: "consolidated active goroutines",
	})

	b.Reset()
	profiles(&b)

	data.Metrics = append(data.Metrics, goRuntimeMetric{
		Name:        "PROFILES",
		Value:       b.String(),
		Description: "List of available runtime/pprof profiles.",
	})
	return data
}

func goroutineDump(statusInfo *strings.Builder) {
	profile := pprof.Lookup("goroutine")
	if profile != nil {
		profile.WriteTo(statusInfo, 1)
	}
}

func buildInfo(statusInfo *strings.Builder) {
	if info, ok := debug.ReadBuildInfo(); ok {
		statusInfo.WriteString(info.String())
	}
}

func profiles(statusInfo *strings.Builder) {
	ps := pprof.Profiles()
	statusInfo.WriteString(fmt.Sprintf("profiles %v\n", len(ps)))
	for _, p := range ps {
		statusInfo.WriteString(p.Name())
		statusInfo.WriteRune('\n')
	}
}

func medianBucket(h *metrics.Float64Histogram) float64 {
	total := uint64(0)
	for _, count := range h.Counts {
		total += count
	}
	thresh := total / 2
	total = 0
	for i, count := range h.Counts {
		total += count
		if total >= thresh {
			return h.Buckets[i]
		}
	}
	panic("should not happen")
}
