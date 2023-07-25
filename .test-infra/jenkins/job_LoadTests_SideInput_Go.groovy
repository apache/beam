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
import InfluxDBCredentialsHelper

import static LoadTestsBuilder.GO_SDK_CONTAINER

String now = new Date().format('MMddHHmmss', TimeZone.getTimeZone('UTC'))

def batchScenarios = {
  [
    [
      title          : 'SideInput Go Load test: 10gb-1kb-10workers-1window-first-iterable',
      test           : 'sideinput',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        job_name             : "load-tests-go-dataflow-batch-sideinput-3-${now}",
        project              : 'apache-beam-testing',
        region               : 'us-central1',
        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
        staging_location     : 'gs://temp-storage-for-perf-tests/loadtests',
        influx_namespace     : 'dataflow',
        influx_measurement   : 'go_batch_sideinput_3',
        input_options        : '\'{' +
        '"num_records": 10000000,' +
        '"key_size": 100,' +
        '"value_size": 900}\'',
        access_percentage: 1,
        num_workers          : 10,
        autoscaling_algorithm: 'NONE',
        environment_type     : 'DOCKER',
        environment_config   : GO_SDK_CONTAINER,
      ]
    ],
    [
      title          : 'SideInput Go Load test: 10gb-1kb-10workers-1window-iterable',
      test           : 'sideinput',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        job_name             : "load-tests-go-dataflow-batch-sideinput-4-${now}",
        project              : 'apache-beam-testing',
        region               : 'us-central1',
        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
        staging_location     : 'gs://temp-storage-for-perf-tests/loadtests',
        influx_namespace     : 'dataflow',
        influx_measurement   : 'go_batch_sideinput_4',
        input_options        : '\'{' +
        '"num_records": 10000000,' +
        '"key_size": 100,' +
        '"value_size": 900}\'',
        num_workers          : 10,
        autoscaling_algorithm: 'NONE',
        environment_type     : 'DOCKER',
        environment_config   : GO_SDK_CONTAINER,
      ]
    ]
  ]
  .each { test -> test.pipelineOptions.putAll(additionalPipelineArgs) }
}

def loadTestJob = { scope, triggeringContext, mode ->
  loadTestsBuilder.loadTests(scope, CommonTestProperties.SDK.GO, batchScenarios(), 'sideinput', mode)
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_LoadTests_Go_SideInput_Dataflow_Batch',
    'Run Load Tests Go SideInput Dataflow Batch',
    'Load Tests Go SideInput Dataflow Batch suite',
    this
    ) {
      additionalPipelineArgs = [:]
      loadTestJob(delegate, CommonTestProperties.TriggeringContext.PR, 'batch')
    }

CronJobBuilder.cronJob('beam_LoadTests_Go_SideInput_Dataflow_Batch', 'H H * * *', this) {
  additionalPipelineArgs = [
    influx_db_name: InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influx_hostname: InfluxDBCredentialsHelper.InfluxDBHostUrl,
  ]
  loadTestJob(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT, 'batch')
}
