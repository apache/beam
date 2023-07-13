<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Performance alerts for Beam Python performance and load tests

##  Alerts
Performance regressions or improvements detected with the [Change Point Analysis](https://en.wikipedia.org/wiki/Change_detection) using [edivisive](https://github.com/apache/beam/blob/0a91d139dea4276dc46176c4cdcdfce210fc50c4/.test-infra/jenkins/job_InferenceBenchmarkTests_Python.groovy#L30)
analyzer are automatically filed as Beam GitHub issues with a label `perf-alert`.

The GitHub issue description will contain the information on the affected test and metric by providing the metric values for N consecutive runs with timestamps
before and after the observed change point. Observed change point is pointed as `Anomaly` in the issue description.

Example: [sample perf alert GitHub issue](https://github.com/AnandInguva/beam/issues/83).

If a performance alert is created on a test, a GitHub issue will be created and the GitHub issue metadata such as GitHub issue
URL, issue number along with the change point value and timestamp are exported to BigQuery. This data will be used to analyze the next change point observed on the same test to
update already created GitHub issue or ignore performance alert by not creating GitHub issue to avoid duplicate issue creation.

##  Config file structure
The config file defines the structure to run change point analysis on a given test. To add a test to the config file,
please follow the below structure.

**NOTE**: The Change point analysis only supports reading the metric data from Big Query for now.

```
# the test_1 must be a unique id.
test_1:
  test_name: Pytorch image classification on 50k images of size 224 x 224 with resnet 152
  test_target: apache_beam.testing.benchmarks.inference.pytorch_image_classification_benchmarks
  source: big_query
  metrics_dataset: beam_run_inference
  metrics_table: torch_inference_imagenet_results_resnet152
  project: apache-beam-testing
  metric_name: mean_load_model_latency_milli_secs
  labels:
    - run-inference
  min_runs_between_change_points: 3 # optional parameter
  num_runs_in_change_point_window: 30 # optional parameter
```

**NOTE**: `test_target` is optional. It is used for identifying the test that was causing the regression.

**Note**: If the source is **BigQuery**, the `metrics_dataset`, `metrics_table`, `project` and `metric_name` should match with the values defined for performance/load tests.
The above example uses this [test configuration](https://github.com/apache/beam/blob/0a91d139dea4276dc46176c4cdcdfce210fc50c4/.test-infra/jenkins/job_InferenceBenchmarkTests_Python.groovy#L30)
to fill up the values required to fetch the data from source.

### Different ways to avoid false positive change points

**min_runs_between_change_points**: As the metric data moves across the runs, the change point analysis can place the
change point in a slightly different place. These change points refer to the same regression and are just noise.
When we find a new change point, we will search up to the `min_runs_between_change_points` in both directions from the
current change point. If an existing change point is found within the distance, then the current change point will be
suppressed.

**num_runs_in_change_point_window**: This defines how many runs to consider from the most recent run to be in change point window.
Sometimes, the change point found might be way back in time and could be irrelevant. For a test, if a change point needs to be
reported only when it was observed in the last 7 runs from the current run,
setting `num_runs_in_change_point_window=7` will achieve it.

##  Register a test for performance alerts

If a new test needs to be registered for the performance alerting tool, please add the required test parameters to the
config file.

## Triage performance alert issues

All the performance/load tests metrics defined at [beam/.test-infra/jenkins](https://github.com/apache/beam/tree/master/.test-infra/jenkins) are imported to [Grafana dashboards](http://104.154.241.245/d/1/getting-started?orgId=1) for visualization. Please
find the alerted test dashboard to find a spike in the metric values.

For example, for the below configuration,
* test_target: `apache_beam.testing.benchmarks.inference.pytorch_image_classification_benchmarks`
* metric_name: `mean_load_model_latency_milli_secs`

Grafana dashboard can be found at http://104.154.241.245/d/ZpS8Uf44z/python-ml-runinference-benchmarks?orgId=1&viewPanel=7

If the dashboard for a test is not found, you can use the
notebook `analyze_metric_data.ipynb` to generate a plot for the given test, metric_name.

If you confirm there is a change in the pattern of the values for a test, find the timestamp of when that change happened
and use that timestamp to find possible culprit commit.

If the performance alert is a `false positive`, close the issue as `Close as not planned`.
