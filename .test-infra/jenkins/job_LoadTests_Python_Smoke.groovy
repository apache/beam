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

import LoadTestsBuilder as loadTestsBuilder
import PhraseTriggeringPostCommitBuilder

def now = new Date().format("MMddHHmmss", TimeZone.getTimeZone('UTC'))

def smokeTestConfigurations = { datasetName ->
  [
    [
      title          : 'GroupByKey Python load test Direct',
      test           : 'apache_beam.testing.load_tests.group_by_key_test',
      runner         : CommonTestProperties.Runner.DIRECT,
      pipelineOptions: [
        publish_to_big_query: true,
        project             : 'apache-beam-testing',
        metrics_dataset     : datasetName,
        metrics_table       : 'python_direct_gbk',
        input_options       : '\'{"num_records": 100000,' +
        '"key_size": 1,' +
        '"value_size":1}\'',

      ]
    ],
    [
      title          : 'GroupByKey Python load test Dataflow',
      test           : 'apache_beam.testing.load_tests.group_by_key_test',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        job_name            : 'load-tests-python-dataflow-batch-gbk-smoke-' + now,
        project             : 'apache-beam-testing',
        region               : 'us-central1',
        temp_location       : 'gs://temp-storage-for-perf-tests/smoketests',
        publish_to_big_query: true,
        metrics_dataset     : datasetName,
        metrics_table       : 'python_dataflow_gbk',
        input_options       : '\'{"num_records": 100000,' +
        '"key_size": 1,' +
        '"value_size":1}\'',
        max_num_workers       : 1,
      ]
    ],
  ]
}

// Runs a tiny version load test suite to ensure nothing is broken.
PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_Python_LoadTests_Smoke',
    'Run Python Load Tests Smoke',
    'Python Load Tests Smoke',
    this
    ) {
      def datasetName = loadTestsBuilder.getBigQueryDataset('load_test_SMOKE', CommonTestProperties.TriggeringContext.PR)
      loadTestsBuilder.loadTests(delegate, CommonTestProperties.SDK.PYTHON_37, smokeTestConfigurations(datasetName), "GBK", "smoke")
    }
