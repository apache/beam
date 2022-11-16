/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import CommonJobProperties as common
import LoadTestsBuilder as loadTestsBuilder
import InfluxDBCredentialsHelper

def now = new Date().format("MMddHHmmss", TimeZone.getTimeZone('UTC'))

def jobs = [
  [
    name               : 'beam_PerformanceTests_TextIOIT_Python',
    description        : 'Runs performance tests for Python TextIOIT',
    test               : 'apache_beam.io.filebasedio_perf_test',
    githubTitle        : 'Python TextIO Performance Test',
    githubTriggerPhrase: 'Run Python TextIO Performance Test',
    pipelineOptions    : [
      publish_to_big_query : true,
      metrics_dataset      : 'beam_performance',
      metrics_table        : 'python_textio_1GB_results',
      influx_measurement   : 'python_textio_1GB_results',
      test_class           : 'TextIOPerfTest',
      input_options        : '\'{' +
      '"num_records": 25000000,' +
      '"key_size": 9,' +
      '"value_size": 21}\'',
      dataset_size         : '1050000000',
      num_workers          : '5',
      autoscaling_algorithm: 'NONE'
    ]
  ]
]

jobs.findAll {
  it.name in [
    'beam_PerformanceTests_TextIOIT_Python',
  ]
}.forEach { testJob -> createGCSFileBasedIOITTestJob(testJob) }

private void createGCSFileBasedIOITTestJob(testJob) {
  job(testJob.name) {
    description(testJob.description)
    common.setTopLevelMainJobProperties(delegate)
    common.enablePhraseTriggeringFromPullRequest(delegate, testJob.githubTitle, testJob.githubTriggerPhrase)
    common.setAutoJob(delegate, 'H H * * *')
    InfluxDBCredentialsHelper.useCredentials(delegate)
    additionalPipelineArgs = [
      influxDatabase: InfluxDBCredentialsHelper.InfluxDBDatabaseName,
      influxHost: InfluxDBCredentialsHelper.InfluxDBHostUrl,
    ]
    testJob.pipelineOptions.putAll(additionalPipelineArgs)

    def dataflowSpecificOptions = [
      runner          : 'DataflowRunner',
      project         : 'apache-beam-testing',
      region          : 'us-central1',
      temp_location   : 'gs://temp-storage-for-perf-tests/',
      filename_prefix : "gs://temp-storage-for-perf-tests/${testJob.name}/\${BUILD_ID}/",
    ]

    Map allPipelineOptions = dataflowSpecificOptions << testJob.pipelineOptions

    loadTestsBuilder.loadTest(
        delegate, testJob.name, CommonTestProperties.Runner.DATAFLOW, CommonTestProperties.SDK.PYTHON, allPipelineOptions, testJob.test)
  }
}
