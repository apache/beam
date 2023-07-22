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
      title          : 'Group By Key Go Load test: 2GB of 10B records',
      test           : 'group_by_key',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        job_name             : "load-tests-go-dataflow-batch-gbk-1-${now}",
        project              : 'apache-beam-testing',
        region               : 'us-central1',
        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
        staging_location     : 'gs://temp-storage-for-perf-tests/loadtests',
        influx_namespace     : 'dataflow',
        influx_measurement   : 'go_batch_gbk_1',
        input_options        : '\'{' +
        '"num_records": 200000000,' +
        '"key_size": 1,' +
        '"value_size": 9}\'',
        iterations           : 1,
        fanout               : 1,
        num_workers          : 5,
        autoscaling_algorithm: 'NONE',
        environment_type     : 'DOCKER',
        environment_config   : GO_SDK_CONTAINER,
      ]
    ],
    [
      title          : 'Group By Key Go Load test: 2GB of 100B records',
      test           : 'group_by_key',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        job_name             : "load-tests-go-dataflow-batch-gbk-2-${now}",
        project              : 'apache-beam-testing',
        region               : 'us-central1',
        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
        staging_location     : 'gs://temp-storage-for-perf-tests/loadtests',
        influx_namespace     : 'dataflow',
        influx_measurement   : 'go_batch_gbk_2',
        input_options        : '\'{' +
        '"num_records": 20000000,' +
        '"key_size": 10,' +
        '"value_size": 90}\'',
        iterations           : 1,
        fanout               : 1,
        num_workers          : 5,
        autoscaling_algorithm: 'NONE',
        environment_type     : 'DOCKER',
        environment_config   : GO_SDK_CONTAINER,
      ]
    ],
    [
      title          : 'Group By Key Go Load test: 2GB of 100kB records',
      test           : 'group_by_key',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        job_name             : "load-tests-go-dataflow-batch-gbk-3-${now}",
        project              : 'apache-beam-testing',
        region               : 'us-central1',
        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
        staging_location     : 'gs://temp-storage-for-perf-tests/loadtests',
        influx_namespace     : 'dataflow',
        influx_measurement   : 'go_batch_gbk_3',
        input_options        : '\'{' +
        '"num_records": 20000,' +
        '"key_size": 10000,' +
        '"value_size": 90000}\'',
        iterations           : 1,
        fanout               : 1,
        num_workers          : 5,
        autoscaling_algorithm: 'NONE',
        environment_type     : 'DOCKER',
        environment_config   : GO_SDK_CONTAINER,
      ]
    ],
    [
      title          : 'Group By Key Go Load test: fanout 4 times with 2GB 10-byte records total',
      test           : 'group_by_key',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        job_name             : "load-tests-go-dataflow-batch-gbk-4-${now}",
        project              : 'apache-beam-testing',
        region               : 'us-central1',
        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
        staging_location     : 'gs://temp-storage-for-perf-tests/loadtests',
        influx_namespace     : 'dataflow',
        influx_measurement   : 'go_batch_gbk_4',
        input_options        : '\'{' +
        '"num_records": 5000000,' +
        '"key_size": 10,' +
        '"value_size": 90}\'',
        iterations           : 1,
        fanout               : 4,
        num_workers          : 16,
        autoscaling_algorithm: 'NONE',
        environment_type     : 'DOCKER',
        environment_config   : GO_SDK_CONTAINER,
      ]
    ],
    [
      title          : 'Group By Key Go Load test: fanout 8 times with 2GB 10-byte records total',
      test           : 'group_by_key',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        job_name             : "load-tests-go-dataflow-batch-gbk-5-${now}",
        project              : 'apache-beam-testing',
        region               : 'us-central1',
        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
        staging_location     : 'gs://temp-storage-for-perf-tests/loadtests',
        influx_namespace     : 'dataflow',
        influx_measurement   : 'go_batch_gbk_5',
        input_options        : '\'{' +
        '"num_records": 2500000,' +
        '"key_size": 10,' +
        '"value_size": 90}\'',
        iterations           : 1,
        fanout               : 8,
        num_workers          : 16,
        autoscaling_algorithm: 'NONE',
        environment_type     : 'DOCKER',
        environment_config   : GO_SDK_CONTAINER,
      ]
    ],
    [
      title          : 'Group By Key Go Load test: reiterate 4 times 10kB values',
      test           : 'group_by_key',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        job_name           : "load-tests-go-dataflow-batch-gbk-6-${now}",
        project              : 'apache-beam-testing',
        region               : 'us-central1',
        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
        staging_location     : 'gs://temp-storage-for-perf-tests/loadtests',
        influx_namespace   : 'dataflow',
        influx_measurement : 'go_batch_gbk_6',
        input_options      : '\'{' +
        '"num_records": 20000000,' +
        '"key_size": 10,' +
        '"value_size": 90,' +
        '"num_hot_keys": 200,' +
        '"hot_key_fraction": 1}\'',
        iterations           : 4,
        fanout               : 1,
        num_workers          : 5,
        autoscaling_algorithm: 'NONE',
        environment_type     : 'DOCKER',
        environment_config   : GO_SDK_CONTAINER,
      ]
    ],
    [
      title          : 'Group By Key Go Load test: reiterate 4 times 2MB values',
      test           : 'group_by_key',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        job_name             : "load-tests-go-dataflow-batch-gbk-7-${now}",
        project              : 'apache-beam-testing',
        region               : 'us-central1',
        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
        staging_location     : 'gs://temp-storage-for-perf-tests/loadtests',
        influx_namespace     : 'dataflow',
        influx_measurement   : 'go_batch_gbk_7',
        input_options        : '\'{' +
        '"num_records": 20000000,' +
        '"key_size": 10,' +
        '"value_size": 90,' +
        '"num_hot_keys": 10,' +
        '"hot_key_fraction": 1}\'',
        iterations           : 4,
        fanout               : 1,
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
  loadTestsBuilder.loadTests(scope, CommonTestProperties.SDK.GO, batchScenarios(), 'group_by_key', mode)
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_LoadTests_Go_GBK_Dataflow_Batch',
    'Run Load Tests Go GBK Dataflow Batch',
    'Load Tests Go GBK Dataflow Batch suite',
    this
    ) {
      additionalPipelineArgs = [:]
      loadTestJob(delegate, CommonTestProperties.TriggeringContext.PR, 'batch')
    }

CronJobBuilder.cronJob('beam_LoadTests_Go_GBK_Dataflow_Batch', 'H H * * *', this) {
  additionalPipelineArgs = [
    influx_db_name: InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influx_hostname: InfluxDBCredentialsHelper.InfluxDBHostUrl,
  ]
  loadTestJob(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT, 'batch')
}
