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
import CronJobBuilder
import InfluxDBCredentialsHelper

def now = new Date().format("MMddHHmmss", TimeZone.getTimeZone('UTC'))

def loadTestConfigurations = { mode, datasetName ->
  [
    [
      title          : 'GroupByKey Python Load test: reiterate 4 times 10kB values',
      test           :  'apache_beam.testing.load_tests.group_by_key_test',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        project              : 'apache-beam-testing',
        region               : 'us-central1',
        job_name             : "load-tests-python-dataflow-${mode}-gbk-6-${now}",
        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
        publish_to_big_query : true,
        metrics_dataset      : datasetName,
        metrics_table        : "python_dataflow_${mode}_gbk_6",
        influx_measurement   : "python_${mode}_gbk_6",
        input_options        : '\'{"num_records": 20000000,' +
        '"key_size": 10,' +
        '"value_size": 90,' +
        '"num_hot_keys": 200,' +
        '"hot_key_fraction": 1}\'',
        iterations           : 4,
        fanout               : 1,
        num_workers          : 5,
        autoscaling_algorithm: 'NONE',
      ]
    ],
    [
      title          : 'GroupByKey Python Load test: reiterate 4 times 2MB values',
      test           :  'apache_beam.testing.load_tests.group_by_key_test',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        project              : 'apache-beam-testing',
        region               : 'us-central1',
        job_name             : "load-tests-python-dataflow-${mode}-gbk-7-${now}",
        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
        publish_to_big_query : true,
        metrics_dataset      : datasetName,
        metrics_table        : "python_dataflow_${mode}_gbk_7",
        influx_measurement   : "python_${mode}_gbk_7",
        input_options        : '\'{"num_records": 20000000,' +
        '"key_size": 10,' +
        '"value_size": 90,' +
        '"num_hot_keys": 10,' +
        '"hot_key_fraction": 1}\'',
        iterations           : 4,
        fanout               : 1,
        num_workers          : 5,
        autoscaling_algorithm: 'NONE',
      ]
    ]
  ]
  .each { test -> test.pipelineOptions.putAll(additionalPipelineArgs) }
  .each { test -> (mode != 'streaming') ?: addStreamingOptions(test) }
}

def addStreamingOptions(test) {
  test.pipelineOptions << [
    streaming: null,
    // Use the new Dataflow runner, which offers improved efficiency of Dataflow jobs.
    // See https://cloud.google.com/dataflow/docs/guides/deploying-a-pipeline#dataflow-runner-v2
    // for more details.
    experiments: 'use_runner_v2',
  ]
}

def loadTestJob = { scope, triggeringContext, mode ->
  def datasetName = loadTestsBuilder.getBigQueryDataset('load_test', triggeringContext)
  loadTestsBuilder.loadTests(scope, CommonTestProperties.SDK.PYTHON_37,
      loadTestConfigurations(mode, datasetName), 'GBK reiterate', mode)
}

CronJobBuilder.cronJob('beam_LoadTests_Python_GBK_reiterate_Dataflow_Batch',
    'H 14 * * *', this) {
      additionalPipelineArgs = [
        influx_db_name: InfluxDBCredentialsHelper.InfluxDBDatabaseName,
        influx_hostname: InfluxDBCredentialsHelper.InfluxDBHostUrl,
      ]
      loadTestJob(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT, 'batch')
    }

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_LoadTests_Python_GBK_reiterate_Dataflow_Batch',
    'Run Load Tests Python GBK reiterate Dataflow Batch',
    'Load Tests Python GBK reiterate Dataflow Batch suite',
    this
    ) {
      additionalPipelineArgs = [:]
      loadTestJob(delegate, CommonTestProperties.TriggeringContext.PR, 'batch')
    }

CronJobBuilder.cronJob('beam_LoadTests_Python_GBK_reiterate_Dataflow_Streaming',
    'H 14 * * *', this) {
      additionalPipelineArgs = [
        influx_db_name: InfluxDBCredentialsHelper.InfluxDBDatabaseName,
        influx_hostname: InfluxDBCredentialsHelper.InfluxDBHostUrl,
      ]
      loadTestJob(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT, 'streaming')
    }

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_LoadTests_Python_GBK_reiterate_Dataflow_Streaming',
    'Run Load Tests Python GBK reiterate Dataflow Streaming',
    'Load Tests Python GBK reiterate Dataflow Streaming suite',
    this
    ) {
      additionalPipelineArgs = [:]
      loadTestJob(delegate, CommonTestProperties.TriggeringContext.PR, 'streaming')
    }
