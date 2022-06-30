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

String now = new Date().format("MMddHHmmss", TimeZone.getTimeZone('UTC'))


def batchScenarios = {
  [
    [
      title          : 'ParDo Go Load test: 20M 100 byte records 10 iterations',
      test           : 'pardo',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        job_name             : "load-tests-go-dataflow-batch-pardo-1-${now}",
        project              : 'apache-beam-testing',
        region               : 'us-central1',
        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
        staging_location     : 'gs://temp-storage-for-perf-tests/loadtests',
        influx_measurement   : 'go_batch_pardo_1',
        influx_namespace     : 'dataflow',
        input_options        : '\'{' +
        '"num_records": 20000000,' +
        '"key_size": 10,' +
        '"value_size": 90}\'',
        iterations           : 10,
        number_of_counter_operations: 0,
        number_of_counters   : 0,
        num_workers          : 5,
        autoscaling_algorithm: 'NONE',
        environment_type     : 'DOCKER',
        environment_config   : GO_SDK_CONTAINER,
      ]
    ],
    [
      title          : 'ParDo Go Load test: 20M 100 byte records 200 times',
      test           : 'pardo',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        job_name             : "load-tests-go-dataflow-batch-pardo-2-${now}",
        project              : 'apache-beam-testing',
        region               : 'us-central1',
        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
        staging_location     : 'gs://temp-storage-for-perf-tests/loadtests',
        influx_measurement   : 'go_batch_pardo_2',
        influx_namespace     : 'dataflow',
        input_options        : '\'{' +
        '"num_records": 20000000,' +
        '"key_size": 10,' +
        '"value_size": 90}\'',
        iterations           : 200,
        number_of_counter_operations: 0,
        number_of_counters   : 0,
        num_workers          : 5,
        autoscaling_algorithm: 'NONE',
        environment_type     : 'DOCKER',
        environment_config   : GO_SDK_CONTAINER,
      ]
    ],
    [
      title          : 'ParDo Go Load test: 20M 100 byte records 10 counters',
      test           : 'pardo',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        job_name             : "load-tests-go-dataflow-batch-pardo-3-${now}",
        project              : 'apache-beam-testing',
        region               : 'us-central1',
        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
        staging_location     : 'gs://temp-storage-for-perf-tests/loadtests',
        influx_measurement   : 'go_batch_pardo_3',
        influx_namespace     : 'dataflow',
        input_options        : '\'{' +
        '"num_records": 20000000,' +
        '"key_size": 10,' +
        '"value_size": 90}\'',
        iterations           : 1,
        number_of_counter_operations: 10,
        number_of_counters   : 1,
        num_workers          : 5,
        autoscaling_algorithm: 'NONE',
        environment_type     : 'DOCKER',
        environment_config   : GO_SDK_CONTAINER,
      ]
    ],
    [
      title          : 'ParDo Go Load test: 20M 100 byte records 100 counters',
      test           : 'pardo',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        job_name             : "load-tests-go-dataflow-batch-pardo-4-${now}",
        project              : 'apache-beam-testing',
        region               : 'us-central1',
        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
        staging_location     : 'gs://temp-storage-for-perf-tests/loadtests',
        influx_measurement   : 'go_batch_pardo_4',
        influx_namespace     : 'dataflow',
        input_options        : '\'{' +
        '"num_records": 20000000,' +
        '"key_size": 10,' +
        '"value_size": 90}\'',
        iterations           : 1,
        number_of_counter_operations: 100,
        number_of_counters   : 1,
        num_workers          : 5,
        autoscaling_algorithm: 'NONE',
        environment_type     : 'DOCKER',
        environment_config   : GO_SDK_CONTAINER,
      ]
    ],
  ].each { test -> test.pipelineOptions.putAll(additionalPipelineArgs) }
}

def loadTestJob = { scope, triggeringContext, mode ->
  loadTestsBuilder.loadTests(scope, CommonTestProperties.SDK.GO, batchScenarios(), 'ParDo', mode)
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_LoadTests_Go_ParDo_Dataflow_Batch',
    'Run Load Tests Go ParDo Dataflow Batch',
    'Load Tests Go ParDo Dataflow Batch suite',
    this
    ) {
      additionalPipelineArgs = [:]
      loadTestJob(delegate, CommonTestProperties.TriggeringContext.PR, 'batch')
    }

CronJobBuilder.cronJob('beam_LoadTests_Go_ParDo_Dataflow_Batch', 'H 10 * * *', this) {
  additionalPipelineArgs = [
    influx_db_name: InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influx_hostname: InfluxDBCredentialsHelper.InfluxDBHostUrl,
  ]
  loadTestJob(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT, 'batch')
}
