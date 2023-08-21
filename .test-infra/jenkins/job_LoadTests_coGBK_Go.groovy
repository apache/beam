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
      title          : 'CoGroupByKey Go Load test: 2GB of 100B records with a single key',
      test           : 'cogbk',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        job_name             : "load-tests-go-dataflow-batch-cogbk-1-${now}",
        project              : 'apache-beam-testing',
        region               : 'us-central1',
        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
        staging_location     : 'gs://temp-storage-for-perf-tests/loadtests',
        influx_measurement   : 'go_batch_cogbk_1',
        influx_namespace     : 'dataflow',
        input_options        : '\'{' +
        '"num_records": 20000000,' +
        '"key_size": 10,' +
        '"value_size": 90,' +
        '"num_hot_keys": 1,' +
        '"hot_key_fraction": 1}\'',
        co_input_options     : '\'{' +
        '"num_records": 2000000,' +
        '"key_size": 10,' +
        '"value_size": 90,' +
        '"num_hot_keys": 1000,' +
        '"hot_key_fraction": 1}\'',
        iterations           : 1,
        num_workers          : 5,
        autoscaling_algorithm: 'NONE',
        environment_type     : 'DOCKER',
        environment_config   : GO_SDK_CONTAINER,
      ]
    ],
    [
      title          : 'CoGroupByKey Go Load test: 2GB of 100B records with multiple keys',
      test           : 'cogbk',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        job_name             : "load-tests-go-dataflow-batch-cogbk-2-${now}",
        project              : 'apache-beam-testing',
        region               : 'us-central1',
        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
        staging_location     : 'gs://temp-storage-for-perf-tests/loadtests',
        influx_measurement   : 'go_batch_cogbk_2',
        influx_namespace     : 'dataflow',
        input_options        : '\'{' +
        '"num_records": 20000000,' +
        '"key_size": 10,' +
        '"value_size": 90,' +
        '"num_hot_keys": 5,' +
        '"hot_key_fraction": 1}\'',
        co_input_options     : '\'{' +
        '"num_records": 2000000,' +
        '"key_size": 10,' +
        '"value_size": 90,' +
        '"num_hot_keys": 1000,' +
        '"hot_key_fraction": 1}\'',
        iterations           : 1,
        num_workers          : 5,
        autoscaling_algorithm: 'NONE',
        environment_type     : 'DOCKER',
        environment_config   : GO_SDK_CONTAINER,
      ]
    ],
    [
      title          : 'CoGroupByKey Go Load test: reiterate 4 times 10kB values',
      test           : 'cogbk',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        job_name             : "load-tests-go-dataflow-batch-cogbk-3-${now}",
        project              : 'apache-beam-testing',
        region               : 'us-central1',
        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
        staging_location     : 'gs://temp-storage-for-perf-tests/loadtests',
        influx_measurement   : 'go_batch_cogbk_3',
        influx_namespace     : 'dataflow',
        input_options        : '\'{' +
        '"num_records": 20000000,' +
        '"key_size": 10,' +
        '"value_size": 90,' +
        '"num_hot_keys": 200000,' +
        '"hot_key_fraction": 1}\'',
        co_input_options     : '\'{' +
        '"num_records": 2000000,' +
        '"key_size": 10,' +
        '"value_size": 90,' +
        '"num_hot_keys": 1000,' +
        '"hot_key_fraction": 1}\'',
        iterations           : 4,
        num_workers          : 5,
        autoscaling_algorithm: 'NONE',
        environment_type     : 'DOCKER',
        environment_config   : GO_SDK_CONTAINER,
      ]
    ],
    [
      title          : 'CoGroupByKey Go Load test: reiterate 4 times 2MB values',
      test           : 'cogbk',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        job_name             : "load-tests-go-dataflow-batch-cogbk-4-${now}",
        project              : 'apache-beam-testing',
        region               : 'us-central1',
        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
        staging_location     : 'gs://temp-storage-for-perf-tests/loadtests',
        influx_measurement   : 'go_batch_cogbk_4',
        influx_namespace     : 'dataflow',
        input_options        : '\'{' +
        '"num_records": 20000000,' +
        '"key_size": 10,' +
        '"value_size": 90,' +
        '"num_hot_keys": 1000,' +
        '"hot_key_fraction": 1}\'',
        co_input_options     : '\'{' +
        '"num_records": 2000000,' +
        '"key_size": 10,' +
        '"value_size": 90,' +
        '"num_hot_keys": 1000,' +
        '"hot_key_fraction": 1}\'',
        iterations           : 4,
        num_workers          : 5,
        autoscaling_algorithm: 'NONE',
        environment_type     : 'DOCKER',
        environment_config   : GO_SDK_CONTAINER,
      ]
    ],
  ]
  .each { test -> test.pipelineOptions.putAll(additionalPipelineArgs) }
}

def loadTestJob = { scope, triggeringContext, mode ->
  loadTestsBuilder.loadTests(scope, CommonTestProperties.SDK.GO, batchScenarios(), 'CoGBK', mode)
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_LoadTests_Go_CoGBK_Dataflow_Batch',
    'Run Load Tests Go CoGBK Dataflow Batch',
    'Load Tests Go CoGBK Dataflow Batch suite',
    this
    ) {
      additionalPipelineArgs = [:]
      loadTestJob(delegate, CommonTestProperties.TriggeringContext.PR, 'batch')
    }

CronJobBuilder.cronJob('beam_LoadTests_Go_CoGBK_Dataflow_batch', 'H H * * *', this) {
  additionalPipelineArgs = [
    influx_db_name: InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influx_hostname: InfluxDBCredentialsHelper.InfluxDBHostUrl,
  ]
  loadTestJob(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT, 'batch')
}
