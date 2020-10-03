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
import LoadTestsBuilder as loadTestsBuilder
import PhraseTriggeringPostCommitBuilder
import InfluxDBCredentialsHelper

def now = new Date().format("MMddHHmmss", TimeZone.getTimeZone('UTC'))

def fromTemplate = { mode, name, id, datasetName, testSpecificOptions ->
  [
    title          : "SideInput Python Load test: ${name}",
    test           : 'apache_beam.testing.load_tests.sideinput_test',
    runner         : CommonTestProperties.Runner.DATAFLOW,
    pipelineOptions: [
      job_name             : "load-tests-python-dataflow-${mode}-sideinput-${id}-${now}",
      project              : 'apache-beam-testing',
      region               : 'us-central1',
      temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
      publish_to_big_query : true,
      metrics_dataset      : datasetName,
      metrics_table        : "python_dataflow_${mode}_sideinput_${id}",
      influx_measurement   : "python_${mode}_sideinput_${id}",
      num_workers          : 10,
      autoscaling_algorithm: 'NONE',
      experiments          : 'use_runner_v2',
    ] << testSpecificOptions
  ]
}

def loadTestConfigurations = { mode, datasetName ->
  [
    [
      name: '1gb-1kb-10workers-1window-1key-percent-dict',
      testSpecificOptions: [
        input_options    : '\'{' +
        '"num_records": 1000000,' +
        '"key_size": 100,' +
        '"value_size": 900}\'',
        side_input_type  : 'dict',
        access_percentage: 1,
      ]
    ],
    [
      name: '1gb-1kb-10workers-1window-99key-percent-dict',
      testSpecificOptions: [
        input_options    : '\'{' +
        '"num_records": 1000000,' +
        '"key_size": 100,' +
        '"value_size": 900}\'',
        side_input_type  : 'dict',
        access_percentage: 99,
      ]
    ],
    [
      name: '10gb-1kb-10workers-1window-first-iterable',
      testSpecificOptions: [
        input_options    : '\'{' +
        '"num_records": 10000000,' +
        '"key_size": 100,' +
        '"value_size": 900}\'',
        side_input_type  : 'iter',
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
        side_input_type  : 'iter',
      ]
    ],
    [
      name: '1gb-1kb-10workers-1window-first-list',
      testSpecificOptions: [
        input_options    : '\'{' +
        '"num_records": 1000000,' +
        '"key_size": 100,' +
        '"value_size": 900}\'',
        side_input_type  : 'list',
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
        side_input_type  : 'list',
      ]
    ],
    [
      name: '1gb-1kb-10workers-1000window-1key-percent-dict',
      testSpecificOptions: [
        input_options    : '\'{' +
        '"num_records": 1000000,' +
        '"key_size": 100,' +
        '"value_size": 900}\'',
        side_input_type  : 'dict',
        access_percentage: 1,
        window_count     : 1000,
      ]
    ],
    [
      name: '1gb-1kb-10workers-1000window-99key-percent-dict',
      testSpecificOptions: [
        input_options    : '\'{' +
        '"num_records": 1000000,' +
        '"key_size": 100,' +
        '"value_size": 900}\'',
        side_input_type  : 'dict',
        access_percentage: 99,
        window_count     : 1000,
      ]
    ],
    [
      name: '10gb-1kb-10workers-1000window-first-iterable',
      testSpecificOptions: [
        input_options    : '\'{' +
        '"num_records": 10000000,' +
        '"key_size": 100,' +
        '"value_size": 900}\'',
        side_input_type  : 'iter',
        access_percentage: 1,
        window_count     : 1000,
      ]
    ],
    [
      name: '10gb-1kb-10workers-1000window-iterable',
      testSpecificOptions: [
        input_options    : '\'{' +
        '"num_records": 10000000,' +
        '"key_size": 100,' +
        '"value_size": 900}\'',
        side_input_type  : 'iter',
        window_count     : 1000,
      ]
    ],
  ].indexed().collect { index, it ->
    fromTemplate(mode, it.name, index + 1, datasetName, it.testSpecificOptions << additionalPipelineArgs)
  }
}


def loadTestJob = { scope, triggeringContext, mode ->
  def datasetName = loadTestsBuilder.getBigQueryDataset('load_test', triggeringContext)
  loadTestsBuilder.loadTests(scope, CommonTestProperties.SDK.PYTHON_37,
      loadTestConfigurations(mode, datasetName), 'SideInput', mode)
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_LoadTests_Python_SideInput_Dataflow_Batch',
    'Run Python Load Tests SideInput Dataflow Batch',
    'Load Tests Python SideInput Dataflow Batch suite',
    this
    ) {
      additionalPipelineArgs = [:]
      loadTestJob(delegate, CommonTestProperties.TriggeringContext.PR, 'batch')
    }

CronJobBuilder.cronJob('beam_LoadTests_Python_SideInput_Dataflow_Batch', 'H 11 * * *', this) {
  additionalPipelineArgs = [
    influx_db_name: InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influx_hostname: InfluxDBCredentialsHelper.InfluxDBHostUrl,
  ]
  loadTestJob(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT, 'batch')
}
