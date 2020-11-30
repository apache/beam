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

def now = new Date().format("MMddHHmmss", TimeZone.getTimeZone('UTC'))

def fromTemplate = { mode, name, id, testSpecificOptions ->
  [
    title          : "SideInput Go Load test: ${name}",
    test           : 'sideinput',
    runner         : CommonTestProperties.Runner.FLINK,
    pipelineOptions: [
      job_name           : "load-tests-go-flink-${mode}-sideinput-${id}-${now}",
      influx_namespace   : 'flink',
      influx_measurement : "go_${mode}_sideinput_${id}_${now}",
      parallelism        : 10,
      endpoint           : 'localhost:8099',
      environment_type   : 'DOCKER',
      environment_config : "${DOCKER_CONTAINER_REGISTRY}/beam_go_sdk:latest",
    ] << testSpecificOptions
  ]
}

def loadTestConfigurations = { mode ->
  [
    [
      name: '10gb-1kb-10workers-1window-first-iterable',
      testSpecificOptions: [
        input_options    : '\'{' +
        '"num_records": 10000000,' +
        '"key_size": 100,' +
        '"value_size": 900}\'',
        access_percentage: 1,
      ]
    ],
    [
      name: '10gb-1kb-10workers-1window-iterable',
      testSpecificOptions: [
        input_options    : '\'{' +
        '"num_records": 10000000,' +
        '"key_size": 100,' +
        '"value_size": 900}\'',
      ]
    ],
    [
      name: '1gb-1kb-10workers-1window-first-list',
      testSpecificOptions: [
        input_options    : '\'{' +
        '"num_records": 1000000,' +
        '"key_size": 100,' +
        '"value_size": 900}\'',
        access_percentage: 1,
      ]
    ],
    [
      name: '1gb-1kb-10workers-1window-list',
      testSpecificOptions: [
        input_options    : '\'{' +
        '"num_records": 1000000,' +
        '"key_size": 100,' +
        '"value_size": 900}\'',
      ]
    ],
  ].indexed().collect { index, it ->
    fromTemplate(mode, it.name, index + 1, it.testSpecificOptions << additionalPipelineArgs)
  }
}

def loadTestJob = { scope, triggeringContext, mode ->
  def numberOfWorkers = 10

  Flink flink = new Flink(scope, "beam_LoadTests_Go_SideInput_Flink_${mode.capitalize()}")
  flink.setUp(
      [
        "${DOCKER_CONTAINER_REGISTRY}/beam_go_sdk:latest"
      ],
      numberOfWorkers,
      "${DOCKER_CONTAINER_REGISTRY}/beam_flink1.10_job_server:latest")

  loadTestsBuilder.loadTests(scope, CommonTestProperties.SDK.GO,
      loadTestConfigurations(mode), 'SideInput', mode)
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

CronJobBuilder.cronJob('beam_LoadTests_Go_SideInput_Flink_Batch', 'H 11 * * *', this) {
  additionalPipelineArgs = [
    influx_db_name: InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influx_hostname: InfluxDBCredentialsHelper.InfluxDBHostUrl,
  ]
  loadTestJob(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT, 'batch')
}
