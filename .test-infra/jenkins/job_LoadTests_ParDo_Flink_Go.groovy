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


def batchScenarios = {
  [
    [
      title          : 'ParDo Go Load test: 20M 100 byte records 10 iterations',
      test           : 'pardo',
      runner         : CommonTestProperties.Runner.FLINK,
      pipelineOptions: [
        job_name             : "load-tests-go-flink-batch-pardo-1-${now}",
        influx_measurement   : 'go_batch_pardo_1',
        influx_namespace     : 'flink',
        input_options        : '\'{' +
        '"num_records": 20000000,' +
        '"key_size": 10,' +
        '"value_size": 90}\'',
        iterations           : 10,
        number_of_counter_operations: 0,
        number_of_counters   : 0,
        parallelism          : 5,
        endpoint             : 'localhost:8099',
        environment_type     : 'DOCKER',
        environment_config   : GO_SDK_CONTAINER,
      ]
    ],
    [
      title          : 'ParDo Go Load test: 20M 100 byte records 200 times',
      test           : 'pardo',
      runner         : CommonTestProperties.Runner.FLINK,
      pipelineOptions: [
        job_name             : "load-tests-go-flink-batch-pardo-2-${now}",
        influx_measurement   : 'go_batch_pardo_2',
        influx_namespace     : 'flink',
        input_options        : '\'{' +
        '"num_records": 20000000,' +
        '"key_size": 10,' +
        '"value_size": 90}\'',
        iterations           : 200,
        number_of_counter_operations: 0,
        number_of_counters   : 0,
        parallelism          : 5,
        endpoint             : 'localhost:8099',
        environment_type     : 'DOCKER',
        environment_config   : GO_SDK_CONTAINER,
      ]
    ],
    [
      title          : 'ParDo Go Load test: 20M 100 byte records 10 counters',
      test           : 'pardo',
      runner         : CommonTestProperties.Runner.FLINK,
      pipelineOptions: [
        job_name             : "load-tests-go-flink-batch-pardo-3-${now}",
        influx_measurement   : 'go_batch_pardo_3',
        influx_namespace     : 'flink',
        input_options        : '\'{' +
        '"num_records": 20000000,' +
        '"key_size": 10,' +
        '"value_size": 90}\'',
        iterations           : 1,
        number_of_counter_operations: 10,
        number_of_counters   : 1,
        parallelism          : 5,
        endpoint             : 'localhost:8099',
        environment_type     : 'DOCKER',
        environment_config   : GO_SDK_CONTAINER,
      ]
    ],
    [
      title          : 'ParDo Go Load test: 20M 100 byte records 100 counters',
      test           : 'pardo',
      runner         : CommonTestProperties.Runner.FLINK,
      pipelineOptions: [
        job_name             : "load-tests-go-flink-batch-pardo-4-${now}",
        influx_measurement   : 'go_batch_pardo_4',
        influx_namespace     : 'flink',
        input_options        : '\'{' +
        '"num_records": 20000000,' +
        '"key_size": 10,' +
        '"value_size": 90}\'',
        iterations           : 1,
        number_of_counter_operations: 100,
        number_of_counters   : 1,
        parallelism          : 5,
        endpoint             : 'localhost:8099',
        environment_type     : 'DOCKER',
        environment_config   : GO_SDK_CONTAINER,
      ]
    ],
  ].each { test -> test.pipelineOptions.putAll(additionalPipelineArgs) }
}

def loadTestJob = { scope, triggeringContext, mode ->
  def numberOfWorkers = 5

  Flink flink = new Flink(scope, "beam_LoadTests_Go_ParDo_Flink_${mode.capitalize()}")
  flink.setUp(
      [
        GO_SDK_CONTAINER
      ],
      numberOfWorkers,
      "${DOCKER_BEAM_JOBSERVER}/beam_flink${CommonTestProperties.getFlinkVersion()}_job_server:latest")

  loadTestsBuilder.loadTests(scope, CommonTestProperties.SDK.GO, batchScenarios(), 'ParDo', mode)
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_LoadTests_Go_ParDo_Flink_Batch',
    'Run Load Tests Go ParDo Flink Batch',
    'Load Tests Go ParDo Flink Batch suite',
    this
    ) {
      additionalPipelineArgs = [:]
      loadTestJob(delegate, CommonTestProperties.TriggeringContext.PR, 'batch')
    }

CronJobBuilder.cronJob('beam_LoadTests_Go_ParDo_Flink_Batch', 'H H * * *', this) {
  additionalPipelineArgs = [
    influx_db_name: InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influx_hostname: InfluxDBCredentialsHelper.InfluxDBHostUrl,
  ]
  // TODO(BEAM): Fix this test.
  loadTestJob(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT, 'batch')
}
