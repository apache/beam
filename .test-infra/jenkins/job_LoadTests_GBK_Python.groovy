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

import LoadTestsBuilder as loadTestsBuilder
import PhraseTriggeringPostCommitBuilder
import InfluxDBCredentialsHelper

def now = new Date().format("MMddHHmmss", TimeZone.getTimeZone('UTC'))

// TODO(BEAM-10774): Skipping some cases because they are too slow.
def STREAMING_TESTS_TO_SKIP = [1, 2, 4, 5]

def loadTestConfigurations = { mode, datasetName ->
  [
    [
      title          : 'GroupByKey Python Load test: 2GB of 10B records',
      test           : 'apache_beam.testing.load_tests.group_by_key_test',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        job_name             : "load-tests-python-dataflow-${mode}-gbk-1-${now}",
        project              : 'apache-beam-testing',
        region               : 'us-central1',
        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
        publish_to_big_query : true,
        metrics_dataset      : datasetName,
        metrics_table        : "python_dataflow_${mode}_gbk_1",
        influx_measurement   : "python_${mode}_gbk_1",
        input_options        : '\'{"num_records": 200000000,' +
        '"key_size": 1,' +
        '"value_size": 9}\'',
        iterations           : 1,
        fanout               : 1,
        num_workers          : 5,
        autoscaling_algorithm: 'NONE',
      ]
    ],
    [
      title          : 'GroupByKey Python Load test: 2GB of 100B records',
      test           : 'apache_beam.testing.load_tests.group_by_key_test',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        job_name             : 'load-tests-python-dataflow-${mode}-gbk-2-' + now,
        project              : 'apache-beam-testing',
        region               : 'us-central1',
        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
        publish_to_big_query : true,
        metrics_dataset      : datasetName,
        metrics_table        : 'python_dataflow_${mode}_gbk_2',
        influx_measurement   : 'python_${mode}_gbk_2',
        input_options        : '\'{"num_records": 20000000,' +
        '"key_size": 10,' +
        '"value_size": 90}\'',
        iterations           : 1,
        fanout               : 1,
        num_workers          : 5,
        autoscaling_algorithm: 'NONE',
      ]
    ],
    [
      title          : 'GroupByKey Python Load test: 2GB of 100kB records',
      test           : 'apache_beam.testing.load_tests.group_by_key_test',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        job_name             : "load-tests-python-dataflow-${mode}-gbk-3-${now}",
        project              : 'apache-beam-testing',
        region               : 'us-central1',
        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
        publish_to_big_query : true,
        metrics_dataset      : datasetName,
        metrics_table        : "python_dataflow_${mode}_gbk_3",
        influx_measurement   : "python_${mode}_gbk_3",
        input_options        : '\'{"num_records": 20000,' +
        '"key_size": 10000,' +
        '"value_size": 90000}\'',
        iterations           : 1,
        fanout               : 1,
        num_workers          : 5,
        autoscaling_algorithm: 'NONE',
      ]
    ],
    [
      title          : 'GroupByKey Python Load test: fanout 4 times with 2GB 10-byte records total',
      test           : 'apache_beam.testing.load_tests.group_by_key_test',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        job_name             : "load-tests-python-dataflow-${mode}-gbk-4-${now}",
        project              : 'apache-beam-testing',
        region               : 'us-central1',
        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
        publish_to_big_query : true,
        metrics_dataset      : datasetName,
        metrics_table        : "python_dataflow_${mode}_gbk_4",
        influx_measurement   : "python_${mode}_gbk_4",
        input_options        : '\'{"num_records": 5000000,' +
        '"key_size": 10,' +
        '"value_size": 90}\'',
        iterations           : 1,
        fanout               : 4,
        num_workers          : 16,
        autoscaling_algorithm: 'NONE',
      ]
    ],
    [
      title          : 'GroupByKey Python Load test: fanout 8 times with 2GB 10-byte records total',
      test           : 'apache_beam.testing.load_tests.group_by_key_test',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        job_name             : "load-tests-python-dataflow-${mode}-gbk-5-${now}",
        project              : 'apache-beam-testing',
        region               : 'us-central1',
        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
        publish_to_big_query : true,
        metrics_dataset      : datasetName,
        metrics_table        : "python_dataflow_${mode}_gbk_5",
        influx_measurement   : "python_${mode}_gbk_5",
        input_options        : '\'{"num_records": 2500000,' +
        '"key_size": 10,' +
        '"value_size": 90}\'',
        iterations           : 1,
        fanout               : 8,
        num_workers          : 16,
        autoscaling_algorithm: 'NONE',
      ]
    ],
  ]
  .each { test -> test.pipelineOptions.putAll(additionalPipelineArgs) }
  .each { test -> (mode != 'streaming') ?: addStreamingOptions(test) }
  .withIndex().collectMany { test, i ->
    mode == 'streaming' && STREAMING_TESTS_TO_SKIP.contains(i + 1) ? []: [test]
  }
}

def addStreamingOptions(test) {
  test.pipelineOptions << [streaming: null, experiments: 'use_runner_v2',
    enable_streaming_engine: null ]
}

def loadTestJob = { scope, triggeringContext, mode ->
  def datasetName = loadTestsBuilder.getBigQueryDataset('load_test', triggeringContext)
  loadTestsBuilder.loadTests(scope, CommonTestProperties.SDK.PYTHON_37,
      loadTestConfigurations(mode, datasetName), 'GBK', mode)
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_LoadTests_Python_GBK_Dataflow_Batch',
    'Run Load Tests Python GBK Dataflow Batch',
    'Load Tests Python GBK Dataflow Batch suite',
    this
    ) {
      additionalPipelineArgs = [:]
      loadTestJob(delegate, CommonTestProperties.TriggeringContext.PR, 'batch')
    }

CronJobBuilder.cronJob('beam_LoadTests_Python_GBK_Dataflow_Batch', 'H 12 * * *', this) {
  additionalPipelineArgs = [
    influx_db_name: InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influx_hostname: InfluxDBCredentialsHelper.InfluxDBHostname,
  ]
  loadTestJob(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT, 'batch')
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_LoadTests_Python_GBK_Dataflow_Streaming',
    'Run Load Tests Python GBK Dataflow Streaming',
    'Load Tests Python GBK Dataflow Streaming suite',
    this
    ) {
      additionalPipelineArgs = [:]
      loadTestJob(delegate, CommonTestProperties.TriggeringContext.PR, 'streaming')
    }

CronJobBuilder.cronJob('beam_LoadTests_Python_GBK_Dataflow_Streaming', 'H 12 * * *', this) {
  additionalPipelineArgs = [
    influx_db_name: InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influx_hostname: InfluxDBCredentialsHelper.InfluxDBHostname,
  ]
  loadTestJob(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT, 'streaming')
}

