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

// TODO(BEAM-10852): Skipping some cases because they are too slow.
def TESTS_TO_SKIP = [
  'load-tests-python-flink-streaming-combine-1',
]

def loadTestConfigurations = { mode, datasetName ->
  [
    [
      title          : 'Combine Python Load test: 2GB 10 byte records',
      test           : 'apache_beam.testing.load_tests.combine_test',
      runner         : CommonTestProperties.Runner.PORTABLE,
      pipelineOptions: [
        job_name            : "load-tests-python-flink-${mode}-combine-1-${now}",
        project             : 'apache-beam-testing',
        publish_to_big_query: true,
        metrics_dataset     : datasetName,
        metrics_table       : "python_flink_${mode}_combine_1",
        influx_measurement  : "python_${mode}_combine_1",
        input_options       : '\'{' +
        '"num_records": 200000000,' +
        '"key_size": 1,' +
        '"value_size": 9}\'',
        parallelism         : 5,
        job_endpoint        : 'localhost:8099',
        environment_type    : 'DOCKER',
        environment_config  : "${DOCKER_CONTAINER_REGISTRY}/beam_python3.7_sdk:latest",
        top_count           : 20,
      ]
    ],
    [
      title          : 'Combine Python Load test: 2GB Fanout 4',
      test           : 'apache_beam.testing.load_tests.combine_test',
      runner         : CommonTestProperties.Runner.PORTABLE,
      pipelineOptions: [
        job_name            : "load-tests-python-flink-${mode}-combine-4-${now}",
        project             : 'apache-beam-testing',
        publish_to_big_query: true,
        metrics_dataset     : datasetName,
        metrics_table       : "python_flink_${mode}_combine_4",
        influx_measurement  : "python_${mode}_combine_4",
        input_options       : '\'{' +
        '"num_records": 5000000,' +
        '"key_size": 10,' +
        '"value_size": 90}\'',
        parallelism         : 16,
        job_endpoint        : 'localhost:8099',
        environment_type    : 'DOCKER',
        environment_config  : "${DOCKER_CONTAINER_REGISTRY}/beam_python3.7_sdk:latest",
        fanout              : 4,
        top_count           : 20,
      ]
    ],
    [
      title          : 'Combine Python Load test: 2GB Fanout 8',
      test           : 'apache_beam.testing.load_tests.combine_test',
      runner         : CommonTestProperties.Runner.PORTABLE,
      pipelineOptions: [
        job_name            : "load-tests-python-flink-${mode}-combine-5-${now}",
        project             : 'apache-beam-testing',
        publish_to_big_query: true,
        metrics_dataset     : datasetName,
        metrics_table       : "python_flink_${mode}_combine_5",
        influx_measurement  : "python_${mode}_combine_5",
        input_options       : '\'{' +
        '"num_records": 2500000,' +
        '"key_size": 10,' +
        '"value_size": 90}\'',
        parallelism         : 16,
        job_endpoint        : 'localhost:8099',
        environment_type    : 'DOCKER',
        environment_config  : "${DOCKER_CONTAINER_REGISTRY}/beam_python3.7_sdk:latest",
        fanout              : 8,
        top_count           : 20,
      ]
    ]
  ]
  .each { test -> test.pipelineOptions.putAll(additionalPipelineArgs) }
  .each { test -> (mode != 'streaming') ?: addStreamingOptions(test) }
  .collectMany { test ->
    TESTS_TO_SKIP.any { element -> test.pipelineOptions.job_name.startsWith(element) } ? []: [test]
  }
}

def addStreamingOptions(test) {
  test.pipelineOptions << [streaming: null,
    use_stateful_load_generator: null
  ]
}

def loadTestJob = { scope, triggeringContext, mode ->
  def datasetName = loadTestsBuilder.getBigQueryDataset('load_test', triggeringContext)
  List<Map> testScenarios = loadTestConfigurations(mode, datasetName)
  Map<Integer, List> testScenariosByParallelism = testScenarios.groupBy { test ->
    test.pipelineOptions.parallelism
  }
  Integer initialParallelism = testScenariosByParallelism.keySet().iterator().next()
  List initialScenarios = testScenariosByParallelism.remove(initialParallelism)

  def flink = new Flink(scope, "beam_LoadTests_Python_Combine_Flink_${mode.capitalize()}")
  flink.setUp(
      [
        "${DOCKER_CONTAINER_REGISTRY}/beam_python3.7_sdk:latest"
      ],
      initialParallelism,
      "${DOCKER_CONTAINER_REGISTRY}/beam_flink1.10_job_server:latest")

  // Execute all scenarios connected with initial parallelism.
  loadTestsBuilder.loadTests(scope, CommonTestProperties.SDK.PYTHON_37, initialScenarios, 'Combine', mode)

  // Execute the rest of scenarios.
  testScenariosByParallelism.each { parallelism, scenarios ->
    flink.scaleCluster(parallelism)
    loadTestsBuilder.loadTests(scope, CommonTestProperties.SDK.PYTHON_37, scenarios, 'Combine', mode)
  }
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_LoadTests_Python_Combine_Flink_Batch',
    'Run Load Tests Python Combine Flink Batch',
    'Load Tests Python Combine Flink Batch suite',
    this
    ) {
      additionalPipelineArgs = [:]
      loadTestJob(delegate, CommonTestProperties.TriggeringContext.PR, 'batch')
    }

CronJobBuilder.cronJob('beam_LoadTests_Python_Combine_Flink_Batch', 'H 15 * * *', this) {
  additionalPipelineArgs = [
    influx_db_name: InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influx_hostname: InfluxDBCredentialsHelper.InfluxDBHostUrl,
  ]
  loadTestJob(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT, 'batch')
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_LoadTests_Python_Combine_Flink_Streaming',
    'Run Load Tests Python Combine Flink Streaming',
    'Load Tests Python Combine Flink Streaming suite',
    this
    ) {
      additionalPipelineArgs = [:]
      loadTestJob(delegate, CommonTestProperties.TriggeringContext.PR, 'streaming')
    }

CronJobBuilder.cronJob('beam_LoadTests_Python_Combine_Flink_Streaming', 'H 16 * * *', this) {
  additionalPipelineArgs = [
    influx_db_name: InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influx_hostname: InfluxDBCredentialsHelper.InfluxDBHostUrl,
  ]
  loadTestJob(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT, 'streaming')
}
