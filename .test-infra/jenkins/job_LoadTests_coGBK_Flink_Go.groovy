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

import static LoadTestsBuilder.DOCKER_BEAM_JOBSERVER
import static LoadTestsBuilder.GO_SDK_CONTAINER

String now = new Date().format("MMddHHmmss", TimeZone.getTimeZone('UTC'))

// TODO(BEAM-11398): Skipping the first test because it is too slow.
def TESTS_TO_SKIP = [
  'load-tests-go-flink-batch-cogbk-1-',
]

def batchScenarios = {
  [
    [
      title          : 'CoGroupByKey Go Load test: 2GB of 100B records with a single key',
      test           : 'cogbk',
      runner         : CommonTestProperties.Runner.FLINK,
      pipelineOptions: [
        job_name             : "load-tests-go-flink-batch-cogbk-1-${now}",
        influx_measurement   : 'go_batch_cogbk_1',
        influx_namespace     : 'flink',
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
        endpoint             : 'localhost:8099',
        environment_type     : 'DOCKER',
        environment_config   : GO_SDK_CONTAINER,
      ]
    ],
    [
      title          : 'CoGroupByKey Go Load test: 2GB of 100B records with multiple keys',
      test           : 'cogbk',
      runner         : CommonTestProperties.Runner.FLINK,
      pipelineOptions: [
        job_name             : "load-tests-go-flink-batch-cogbk-2-${now}",
        influx_measurement   : 'go_batch_cogbk_2',
        influx_namespace     : 'flink',
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
        endpoint             : 'localhost:8099',
        environment_type     : 'DOCKER',
        environment_config   : GO_SDK_CONTAINER,
      ]
    ],
    [
      title          : 'CoGroupByKey Go Load test: reiterate 4 times 10kB values',
      test           : 'cogbk',
      runner         : CommonTestProperties.Runner.FLINK,
      pipelineOptions: [
        job_name             : "load-tests-go-flink-batch-cogbk-3-${now}",
        influx_measurement   : 'go_batch_cogbk_3',
        influx_namespace     : 'flink',
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
        endpoint             : 'localhost:8099',
        environment_type     : 'DOCKER',
        environment_config   : GO_SDK_CONTAINER,
      ]
    ],
    [
      title          : 'CoGroupByKey Go Load test: reiterate 4 times 2MB values',
      test           : 'cogbk',
      runner         : CommonTestProperties.Runner.FLINK,
      pipelineOptions: [
        job_name             : "load-tests-go-flink-batch-cogbk-4-${now}",
        influx_measurement   : 'go_batch_cogbk_4',
        influx_namespace     : 'flink',
        input_options        : '\'{' +
        '"num_records": 20000000,' +
        '"key_size": 10,' +
        '"value_size": 90,' +
        '"num_hot_keys": 1000,' +
        '"hot_key_fraction": 1}\'',
        co_input_options      : '\'{' +
        '"num_records": 2000000,' +
        '"key_size": 10,' +
        '"value_size": 90,' +
        '"num_hot_keys": 1000,' +
        '"hot_key_fraction": 1}\'',
        iterations           : 4,
        parallelism          : 5,
        endpoint             : 'localhost:8099',
        environment_type     : 'DOCKER',
        environment_config   : GO_SDK_CONTAINER,
      ]
    ],
  ]
  .each { test -> test.pipelineOptions.putAll(additionalPipelineArgs) }
  .collectMany { test ->
    TESTS_TO_SKIP.any { element -> test.pipelineOptions.job_name.startsWith(element) } ? []: [test]
  }
}

def loadTestJob = { scope, triggeringContext, mode ->
  def numberOfWorkers = 5

  def flink = new Flink(scope, "beam_LoadTests_Go_CoGBK_Flink_${mode.capitalize()}")
  flink.setUp(
      [
        GO_SDK_CONTAINER
      ],
      numberOfWorkers,
      "${DOCKER_BEAM_JOBSERVER}/beam_flink${CommonTestProperties.getFlinkVersion()}_job_server:latest")

  loadTestsBuilder.loadTests(scope, CommonTestProperties.SDK.GO, batchScenarios(), 'CoGBK', mode)
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_LoadTests_Go_CoGBK_Flink_Batch',
    'Run Load Tests Go CoGBK Flink Batch',
    'Load Tests Go CoGBK Flink Batch suite',
    this
    ) {
      additionalPipelineArgs = [:]
      loadTestJob(delegate, CommonTestProperties.TriggeringContext.PR, 'batch')
    }

CronJobBuilder.cronJob('beam_LoadTests_Go_CoGBK_Flink_batch', 'H H * * *', this) {
  additionalPipelineArgs = [
    influx_db_name: InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influx_hostname: InfluxDBCredentialsHelper.InfluxDBHostUrl,
  ]
  // TODO(BEAM): Fixe  this test.
  loadTestJob(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT, 'batch')
}
