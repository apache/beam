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

import CommonJobProperties as commonJobProperties
import CommonTestProperties
import LoadTestsBuilder as loadTestsBuilder
import PhraseTriggeringPostCommitBuilder
import Flink
import InfluxDBCredentialsHelper

import static LoadTestsBuilder.DOCKER_CONTAINER_REGISTRY

String now = new Date().format("MMddHHmmss", TimeZone.getTimeZone('UTC'))

def scenarios = { datasetName ->
  [
    [
      title          : 'CoGroupByKey Python Load test: 2GB of 100B records with a single key',
      test           : 'apache_beam.testing.load_tests.co_group_by_key_test',
      runner         : CommonTestProperties.Runner.PORTABLE,
      pipelineOptions: [
        project              : 'apache-beam-testing',
        job_name             : 'load-tests-python-flink-batch-cogbk-1-' + now,
        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
        publish_to_big_query : true,
        metrics_dataset      : datasetName,
        metrics_table        : "python_flink_batch_cogbk_1",
        influx_measurement   : 'python_batch_cogbk_1',
        input_options        : '\'{' +
        '"num_records": 20000000,' +
        '"key_size": 10,' +
        '"value_size": 90,' +
        '"num_hot_keys": 1,' +
        '"hot_key_fraction": 1}\'',
        co_input_options      : '\'{' +
        '"num_records": 2000000,' +
        '"key_size": 10,' +
        '"value_size": 90,' +
        '"num_hot_keys": 1000,' +
        '"hot_key_fraction": 1}\'',
        iterations           : 1,
        parallelism          : 5,
        job_endpoint         : 'localhost:8099',
        environment_type     : 'DOCKER',
        environment_config   : "${DOCKER_CONTAINER_REGISTRY}/beam_python3.7_sdk:latest",
      ]
    ],
    [
      title          : 'CoGroupByKey Python Load test: 2GB of 100B records with multiple keys',
      test           : 'apache_beam.testing.load_tests.co_group_by_key_test',
      runner         : CommonTestProperties.Runner.PORTABLE,
      pipelineOptions: [
        project              : 'apache-beam-testing',
        job_name             : 'load-tests-python-flink-batch-cogbk-2-' + now,
        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
        publish_to_big_query : true,
        metrics_dataset      : datasetName,
        metrics_table        : 'python_flink_batch_cogbk_2',
        influx_measurement   : 'python_batch_cogbk_2',
        input_options        : '\'{' +
        '"num_records": 20000000,' +
        '"key_size": 10,' +
        '"value_size": 90,' +
        '"num_hot_keys": 5,' +
        '"hot_key_fraction": 1}\'',
        co_input_options      : '\'{' +
        '"num_records": 2000000,' +
        '"key_size": 10,' +
        '"value_size": 90,' +
        '"num_hot_keys": 1000,' +
        '"hot_key_fraction": 1}\'',
        iterations           : 1,
        parallelism          : 5,
        job_endpoint         : 'localhost:8099',
        environment_type     : 'DOCKER',
        environment_config   : "${DOCKER_CONTAINER_REGISTRY}/beam_python3.7_sdk:latest",
      ]
    ],
    [
      title          : 'CoGroupByKey Python Load test: reiterate 4 times 10kB values',
      test           : 'apache_beam.testing.load_tests.co_group_by_key_test',
      runner         : CommonTestProperties.Runner.PORTABLE,
      pipelineOptions: [
        project              : 'apache-beam-testing',
        job_name             : 'load-tests-python-flink-batch-cogbk-3-' + now,
        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
        publish_to_big_query : true,
        metrics_dataset      : datasetName,
        metrics_table        : "python_flink_batch_cogbk_3",
        influx_measurement   : 'python_batch_cogbk_3',
        input_options        : '\'{' +
        '"num_records": 20000000,' +
        '"key_size": 10,' +
        '"value_size": 90,' +
        '"num_hot_keys": 200000,' +
        '"hot_key_fraction": 1}\'',
        co_input_options      : '\'{' +
        '"num_records": 2000000,' +
        '"key_size": 10,' +
        '"value_size": 90,' +
        '"num_hot_keys": 1000,' +
        '"hot_key_fraction": 1}\'',
        iterations           : 4,
        parallelism          : 5,
        job_endpoint         : 'localhost:8099',
        environment_type     : 'DOCKER',
        environment_config   : "${DOCKER_CONTAINER_REGISTRY}/beam_python3.7_sdk:latest",
      ]
    ],
  ].each { test -> test.pipelineOptions.putAll(additionalPipelineArgs) }
}

def loadTest = { scope, triggeringContext ->
  def datasetName = loadTestsBuilder.getBigQueryDataset('load_test', triggeringContext)
  def numberOfWorkers = 5
  List<Map> testScenarios = scenarios(datasetName)

  def flink = new Flink(scope, 'beam_LoadTests_Python_CoGBK_Flink_Batch')
  flink.setUp(
      [
        "${DOCKER_CONTAINER_REGISTRY}/beam_python3.7_sdk:latest"
      ],
      numberOfWorkers,
      "${DOCKER_CONTAINER_REGISTRY}/beam_flink1.10_job_server:latest")

  loadTestsBuilder.loadTests(scope, CommonTestProperties.SDK.PYTHON_37, testScenarios, 'CoGBK', 'batch')
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_LoadTests_Python_CoGBK_Flink_Batch',
    'Run Load Tests Python CoGBK Flink Batch',
    'Load Tests Python CoGBK Flink Batch suite',
    this
    ) {
      additionalPipelineArgs = [:]
      loadTest(delegate, CommonTestProperties.TriggeringContext.PR)
    }

// TODO(BEAM-9761) Re-enable auto builds after these tests pass.
