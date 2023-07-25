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

import CommonTestProperties
import CommonJobProperties as commonJobProperties
import LoadTestsBuilder as loadTestsBuilder
import PhraseTriggeringPostCommitBuilder
import InfluxDBCredentialsHelper

import static LoadTestsBuilder.DOCKER_BEAM_JOBSERVER
import static LoadTestsBuilder.GO_SDK_CONTAINER

def now = new Date().format("MMddHHmmss", TimeZone.getTimeZone('UTC'))

def batchScenarios = {
  [
    [
      title          : 'SideInput Go Load test: 400mb-1kb-10workers-1window-first-iterable',
      test           : 'sideinput',
      runner         : CommonTestProperties.Runner.FLINK,
      pipelineOptions: [
        job_name           : "load-tests-go-flink-batch-sideinput-3-${now}",
        influx_namespace   : 'flink',
        influx_measurement : 'go_batch_sideinput_3',
        input_options      : '\'{' +
        '"num_records": 400000,' +
        '"key_size": 100,' +
        '"value_size": 900}\'',
        access_percentage  : 1,
        parallelism        : 10,
        endpoint           : 'localhost:8099',
        environment_type   : 'DOCKER',
        environment_config : GO_SDK_CONTAINER,
      ]
    ],
    [
      title          : 'SideInput Go Load test: 400mb-1kb-10workers-1window-iterable',
      test           : 'sideinput',
      runner         : CommonTestProperties.Runner.FLINK,
      pipelineOptions: [
        job_name           : "load-tests-go-flink-batch-sideinput-4-${now}",
        influx_namespace   : 'flink',
        influx_measurement : 'go_batch_sideinput_4',
        input_options      : '\'{' +
        '"num_records": 400000,' +
        '"key_size": 100,' +
        '"value_size": 900}\'',
        parallelism        : 10,
        endpoint           : 'localhost:8099',
        environment_type   : 'DOCKER',
        environment_config : GO_SDK_CONTAINER,
      ]
    ],
  ]
  .each { test -> test.pipelineOptions.putAll(additionalPipelineArgs) }
}

def loadTestJob = { scope, triggeringContext, mode ->
  def numberOfWorkers = 10

  Flink flink = new Flink(scope, "beam_LoadTests_Go_SideInput_Flink_${mode.capitalize()}")
  flink.setUp(
      [
        GO_SDK_CONTAINER
      ],
      numberOfWorkers,
      "${DOCKER_BEAM_JOBSERVER}/beam_flink${CommonTestProperties.getFlinkVersion()}_job_server:latest")

  loadTestsBuilder.loadTests(scope, CommonTestProperties.SDK.GO,
      batchScenarios(), 'SideInput', mode)
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_LoadTests_Go_SideInput_Flink_Batch',
    'Run Load Tests Go SideInput Flink Batch',
    'Load Tests Go SideInput Flink Batch suite',
    this
    ) {
      additionalPipelineArgs = [:]
      loadTestJob(delegate, CommonTestProperties.TriggeringContext.PR, 'batch')
    }

CronJobBuilder.cronJob('beam_LoadTests_Go_SideInput_Flink_Batch', 'H H * * *', this) {
  additionalPipelineArgs = [
    influx_db_name: InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influx_hostname: InfluxDBCredentialsHelper.InfluxDBHostUrl,
  ]
  // TODO(BEAM): Fix this test.
  loadTestJob(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT, 'batch')
}
