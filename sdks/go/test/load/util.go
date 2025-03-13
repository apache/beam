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

package load

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/metrics"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
)

const (
	runtimeMetricNamespace = "RuntimeMonitor"
	runtimeMetricName      = "runtime"
)

var (
	influxMeasurement = flag.String(
		"influx_measurement",
		"",
		`An InfluxDB measurement where metrics should be published to.
		If empty, no metrics will be send to InfluxDB.`)
	influxDatabase = flag.String(
		"influx_db_name",
		"",
		"InfluxDB database name. If empty, no metrics will be send to InfluxDB.")
	influxHost = flag.String(
		"influx_hostname",
		"http://localhost:8086",
		"Hostname and port to connect to InfluxDB. Defaults to http://localhost:8086.")
	influxNamespace = flag.String(
		"influx_namespace",
		"",
		`A namespace to be used when constructing InfluxDB's data points.
		Used to make some points different from others within the same measurement.`)
	runtime = beam.NewDistribution(runtimeMetricNamespace, runtimeMetricName)
)

func init() {
	register.DoFn3x0[[]byte, []byte, func([]byte, []byte)]((*RuntimeMonitor)(nil))
	register.Emitter2[[]byte, []byte]()
}

// RuntimeMonitor is a DoFn to record processing time in the pipeline.
//
// It uses a distribution metric which is updated every time a new bundle
// starts or finishes. The processing time can be extracted by calculating
// the difference of the maximum and the minimum value of the distribution
// metric.
type RuntimeMonitor struct{}

// StartBundle updates a distribution metric.
func (fn *RuntimeMonitor) StartBundle(ctx context.Context, emit func([]byte, []byte)) {
	runtime.Update(ctx, time.Now().UnixNano())
}

// FinishBundle updates a distribution metric.
func (fn *RuntimeMonitor) FinishBundle(ctx context.Context, emit func([]byte, []byte)) {
	runtime.Update(ctx, time.Now().UnixNano())
}

// ProcessElement emits unmodified input elements.
func (fn *RuntimeMonitor) ProcessElement(key, value []byte, emit func([]byte, []byte)) {
	emit(key, value)
}

type influxDBOptions struct {
	measurement string
	dbName      string
	hostname    string
	user        string
	password    string
}

func newInfluxDBOptions() *influxDBOptions {
	return &influxDBOptions{
		measurement: *influxMeasurement,
		dbName:      *influxDatabase,
		hostname:    *influxHost,
		user:        os.Getenv("INFLUXDB_USER"),
		password:    os.Getenv("INFLUXDB_USER_PASSWORD")}
}

func (options influxDBOptions) validate() bool {
	return options.measurement != "" && options.dbName != ""
}

func (options influxDBOptions) httpAuthEnabled() bool {
	return options.user != "" && options.password != ""
}

// loadTestResult represents a single data record that has: a timestamp,
// a type of a metric, and a value.
type loadTestResult struct {
	timestamp int64
	metric    string
	value     float64
}

func newLoadTestResult(value float64) loadTestResult {
	metric := ""
	if *influxNamespace == "" {
		metric = runtimeMetricName
	} else {
		metric = fmt.Sprintf("%v_%v", *influxNamespace, runtimeMetricName)
	}
	return loadTestResult{timestamp: time.Now().Unix(), metric: metric, value: value}
}

// PublishMetrics calculates the runtime and sends the result to InfluxDB database.
func PublishMetrics(results metrics.QueryResults) {
	options := newInfluxDBOptions()
	ress := toLoadTestResults(results)
	for _, res := range ress {
		log.Printf("%s %v", res.metric, time.Duration(float64(time.Second)*res.value))
	}
	if len(ress) == 0 {
		log.Print("No metrics returned.")
		return
	}
	if options.validate() {
		publishMetricstoInfluxDB(options, ress)
	} else {
		log.Print("Missing InfluxDB options. Metrics will not be published to InfluxDB")
	}
}

func toLoadTestResults(results metrics.QueryResults) []loadTestResult {
	res := make([]loadTestResult, 0)
	matched := make([]metrics.DistributionResult, 0)

	for _, dist := range results.Distributions() {
		if dist.Key.Namespace == runtimeMetricNamespace &&
			dist.Key.Name == runtimeMetricName {
			matched = append(matched, dist)
		}
	}

	if len(matched) > 0 {
		res = append(res, newLoadTestResult(extractRuntimeValue(matched)))
	}
	return res
}

// extractRuntimeValue returns a difference between the maximum of maximum
// values and the minimum of minimum values in seconds.
func extractRuntimeValue(dists []metrics.DistributionResult) float64 {
	min := dists[0].Result().Min
	max := min

	for _, dist := range dists {
		res := dist.Result()
		if min > res.Min {
			min = res.Min
		}
		if max < res.Max {
			max = res.Max
		}
	}
	return float64(max-min) / float64(time.Second)
}

func publishMetricstoInfluxDB(options *influxDBOptions, results []loadTestResult) {
	url := fmt.Sprintf("%v/write", options.hostname)
	payload := buildPayload(options, results)

	request, err := http.NewRequest("POST", url, strings.NewReader(payload))
	if err != nil {
		log.Print(err)
		return
	}

	query := request.URL.Query()
	query.Add("db", options.dbName)
	query.Add("precision", "s")
	request.URL.RawQuery = query.Encode()

	if options.httpAuthEnabled() {
		request.SetBasicAuth(options.user, options.password)
	}

	client := &http.Client{}
	resp, err := client.Do(request)
	if err != nil {
		log.Print(err)
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Print(err)
		return
	}

	if resp.StatusCode != 204 {
		jsonData := make(map[string]string)
		json.Unmarshal(body, &jsonData)
		log.Printf("Failed to publish metrics to InfluxDB. Received status code %v "+
			"with an error message: %v", resp.StatusCode, jsonData["error"])
	}
}

func buildPayload(options *influxDBOptions, results []loadTestResult) string {
	points := make([]string, len(results))
	for i, result := range results {
		points[i] = fmt.Sprintf("%v,metric=%v value=%f %d", options.measurement,
			result.metric, result.value, result.timestamp)
	}
	return strings.Join(points, "\n")
}
