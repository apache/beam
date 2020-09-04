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
      title          : 'Load test: 2GB of 10B records',
      test           : 'apache_beam.testing.load_tests.group_by_key_test',
      runner         : CommonTestProperties.Runner.PORTABLE,
      pipelineOptions: [
        job_name            : "load_tests_Python_Flink_Batch_GBK_1_${now}",
        publish_to_big_query: true,
        project             : 'apache-beam-testing',
        metrics_dataset     : datasetName,
        metrics_table       : "python_flink_batch_GBK_1",
        influx_measurement  : 'python_batch_gkb_1',
        input_options       : '\'{"num_records": 200000000,"key_size": 1,"value_size":9}\'',
        iterations          : 1,
        fanout              : 1,
        parallelism         : 5,
        job_endpoint        : 'localhost:8099',
        environment_type    : 'DOCKER',
        environment_config  : "${DOCKER_CONTAINER_REGISTRY}/beam_python3.7_sdk:latest",
      ]
    ],
    [
      title          : 'Load test: 2GB of 100B records',
      test           : 'apache_beam.testing.load_tests.group_by_key_test',
      runner         : CommonTestProperties.Runner.PORTABLE,
      pipelineOptions: [
        job_name            : "load_tests_Python_Flink_Batch_GBK_2_${now}",
        publish_to_big_query: true,
        project             : 'apache-beam-testing',
        metrics_dataset     : datasetName,
        metrics_table       : "python_flink_batch_GBK_2",
        influx_measurement  : 'python_batch_gbk_2',
        input_options       : '\'{"num_records": 20000000,"key_size": 10,"value_size":90}\'',
        iterations          : 1,
        fanout              : 1,
        parallelism         : 5,
        job_endpoint        : 'localhost:8099',
        environment_type    : 'DOCKER',
        environment_config  : "${DOCKER_CONTAINER_REGISTRY}/beam_python3.7_sdk:latest",
      ]
    ],
    [
      title          : 'Load test: fanout 4 times with 2GB 10-byte records total',
      test           : 'apache_beam.testing.load_tests.group_by_key_test',
      runner         : CommonTestProperties.Runner.PORTABLE,
      pipelineOptions: [
        job_name            : "load_tests_Python_Flink_Batch_GBK_4_${now}",
        publish_to_big_query: true,
        project             : 'apache-beam-testing',
        metrics_dataset     : datasetName,
        metrics_table       : "python_flink_batch_GBK_4",
        influx_measurement  : 'python_batch_gbk_4',
        input_options       : '\'{"num_records": 5000000,"key_size": 10,"value_size":90}\'',
        iterations          : 1,
        fanout              : 4,
        parallelism         : 16,
        job_endpoint        : 'localhost:8099',
        environment_type    : 'DOCKER',
        environment_config  : "${DOCKER_CONTAINER_REGISTRY}/beam_python3.7_sdk:latest",
      ]
    ],
    [
      title          : 'Load test: fanout 8 times with 2GB 10-byte records total',
      test           : 'apache_beam.testing.load_tests.group_by_key_test',
      runner         : CommonTestProperties.Runner.PORTABLE,
      pipelineOptions: [
        job_name            : "load_tests_Python_Flink_Batch_GBK_5_${now}",
        publish_to_big_query: true,
        project             : 'apache-beam-testing',
        metrics_dataset     : datasetName,
        metrics_table       : "python_flink_batch_GBK_5",
        influx_measurement  : 'python_batch_gbk_5',
        input_options       : '\'{"num_records": 2500000,"key_size": 10,"value_size":90}\'',
        iterations          : 1,
        fanout              : 8,
        parallelism         : 16,
        job_endpoint        : 'localhost:8099',
        environment_type    : 'DOCKER',
        environment_config  : "${DOCKER_CONTAINER_REGISTRY}/beam_python3.7_sdk:latest",
      ]
    ],
    [
      title          : 'Load test: reiterate 4 times 10kB values',
      test           : 'apache_beam.testing.load_tests.group_by_key_test',
      runner         : CommonTestProperties.Runner.PORTABLE,
      pipelineOptions: [
        job_name            : "load_tests_Python_Flink_Batch_GBK_6_${now}",
        publish_to_big_query: true,
        project             : 'apache-beam-testing',
        metrics_dataset     : datasetName,
        metrics_table       : "python_flink_batch_GBK_6",
        influx_measurement  : 'python_batch_gbk_6',
        input_options       : '\'{"num_records": 20000000,"key_size": 10,"value_size":90, "num_hot_keys": 200, "hot_key_fraction": 1}\'',
        iterations          : 4,
        fanout              : 1,
        parallelism         : 5,
        job_endpoint        : 'localhost:8099',
        environment_type    : 'DOCKER',
        environment_config  : "${DOCKER_CONTAINER_REGISTRY}/beam_python3.7_sdk:latest",
      ]
    ],
  ].each { test -> test.pipelineOptions.putAll(additionalPipelineArgs) }
}

def loadTest = { scope, triggeringContext ->
  def sdk = CommonTestProperties.SDK.PYTHON_37

  def datasetName = loadTestsBuilder.getBigQueryDataset('load_test', triggeringContext)
  def numberOfWorkers = 16
  List<Map> testScenarios = scenarios(datasetName)

  def flink = new Flink(scope, 'beam_LoadTests_Python_GBK_Flink_Batch')
  flink.setUp(
      [
        "${DOCKER_CONTAINER_REGISTRY}/beam_python3.7_sdk:latest"
      ],
      numberOfWorkers,
      "${DOCKER_CONTAINER_REGISTRY}/beam_flink1.10_job_server:latest")

  def configurations = testScenarios.findAll { it.pipelineOptions?.parallelism?.value == numberOfWorkers }
  loadTestsBuilder.loadTests(scope, sdk, configurations, "GBK", "batch")

  numberOfWorkers = 5
  flink.scaleCluster(numberOfWorkers)

  configurations = testScenarios.findAll { it.pipelineOptions?.parallelism?.value == numberOfWorkers }
  loadTestsBuilder.loadTests(scope, sdk, configurations, "GBK", "batch")
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_LoadTests_Python_GBK_Flink_Batch',
    'Run Load Tests Python GBK Flink Batch',
    'Load Tests Python GBK Flink Batch suite',
    this
    ) {
      additionalPipelineArgs = [:]
      loadTest(delegate, CommonTestProperties.TriggeringContext.PR)
    }

// TODO(BEAM-9761) Re-enable auto builds after these tests pass.
