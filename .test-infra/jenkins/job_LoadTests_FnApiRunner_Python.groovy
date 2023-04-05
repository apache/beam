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

def loadTestConfigurations = { datasetName ->
  [
    [
      title          : 'FnApiRunner Python load test - microbenchmark',
      test           : 'apache_beam.testing.load_tests.microbenchmarks_test',
      runner         : CommonTestProperties.Runner.DIRECT,
      pipelineOptions: [
        publish_to_big_query: true,
        influx_measurement  : 'python_direct_microbenchmarks',
        project             : 'apache-beam-testing',
        metrics_dataset     : datasetName,
        metrics_table       : 'python_direct_microbenchmarks',
        input_options        : '\'{}\'',
      ]
    ],
  ]
  .each { test -> test.pipelineOptions.putAll(additionalPipelineArgs) }
}

def loadTestJob = { scope, triggeringContext ->
  scope.description("Runs Python FnApiRunner Microbenchmark")
  commonJobProperties.setTopLevelMainJobProperties(scope, 'master', 120)

  def datasetName = loadTestsBuilder.getBigQueryDataset('load_test', triggeringContext)
  for (testConfiguration in loadTestConfigurations(datasetName)) {
    loadTestsBuilder.loadTest(scope, testConfiguration.title, testConfiguration.runner, CommonTestProperties.SDK.PYTHON, testConfiguration.pipelineOptions, testConfiguration.test)
  }
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_Python_LoadTests_FnApiRunner_Microbenchmark',
    'Run Python Load Tests FnApiRunner Microbenchmark',
    'Python Load Tests FnApiRunner Microbenchmark',
    this
    ) {
      additionalPipelineArgs = [:]
      loadTestJob(delegate, CommonTestProperties.TriggeringContext.PR)
    }


// Run this job every 6 hours on a random minute.
CronJobBuilder.cronJob('beam_Python_LoadTests_FnApiRunner_Microbenchmark', 'H H/6 * * *', this) {
  additionalPipelineArgs = [
    influx_db_name: InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influx_hostname: InfluxDBCredentialsHelper.InfluxDBHostUrl,
  ]
  loadTestJob(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT)
}

