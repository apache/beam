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
import Flink
import InfluxDBCredentialsHelper

import static LoadTestsBuilder.DOCKER_BEAM_JOBSERVER
import static LoadTestsBuilder.GO_SDK_CONTAINER


String now = new Date().format('MMddHHmmss', TimeZone.getTimeZone('UTC'))

def batchScenarios = {
  [
    [
      title          : 'Combine Go Load test: 2GB of 10B records',
      test           : 'combine',
      runner         : CommonTestProperties.Runner.FLINK,
      pipelineOptions: [
        job_name             : "load-tests-go-flink-batch-combine-1-${now}",
        influx_namespace     : 'flink',
        influx_measurement   : 'go_batch_combine_1',
        input_options        : '\'{' +
        '"num_records": 200000000,' +
        '"key_size": 1,' +
        '"value_size": 9}\'',
        fanout               : 1,
        top_count            : 20,
        parallelism          : 5,
        endpoint             : 'localhost:8099',
        environment_type     : 'DOCKER',
        environment_config   : GO_SDK_CONTAINER,
      ]
    ],
    [
      title          : 'Combine Go Load test: fanout 4 times with 2GB 10-byte records total',
      test           : 'combine',
      runner         : CommonTestProperties.Runner.FLINK,
      pipelineOptions: [
        job_name             : "load-tests-go-flink-batch-combine-4-${now}",
        influx_namespace     : 'flink',
        influx_measurement   : 'go_batch_combine_4',
        input_options        : '\'{' +
        '"num_records": 5000000,' +
        '"key_size": 10,' +
        '"value_size": 90}\'',
        fanout               : 4,
        top_count            : 20,
        parallelism          : 16,
        endpoint             : 'localhost:8099',
        environment_type     : 'DOCKER',
        environment_config   : GO_SDK_CONTAINER,
      ]
    ],
    [
      title          : 'Combine Go Load test: fanout 8 times with 2GB 10-byte records total',
      test           : 'combine',
      runner         : CommonTestProperties.Runner.FLINK,
      pipelineOptions: [
        job_name             : "load-tests-go-flink-batch-combine-5-${now}",
        influx_namespace     : 'flink',
        influx_measurement   : 'go_batch_combine_5',
        fanout               : 8,
        top_count            : 20,
        parallelism          : 16,
        input_options        : '\'{' +
        '"num_records": 2500000,' +
        '"key_size": 10,' +
        '"value_size": 90}\'',
        endpoint             : 'localhost:8099',
        environment_type     : 'DOCKER',
        environment_config   : GO_SDK_CONTAINER,
      ]
    ],
  ].each { test -> test.pipelineOptions.putAll(additionalPipelineArgs) }
}

def loadTestJob = { scope, triggeringContext, mode ->
  Map<Integer, List> testScenariosByParallelism = batchScenarios().groupBy { test ->
    test.pipelineOptions.parallelism
  }
  Integer initialParallelism = testScenariosByParallelism.keySet().iterator().next()
  List initialScenarios = testScenariosByParallelism.remove(initialParallelism)

  def flink = new Flink(scope, "beam_LoadTests_Go_Combine_Flink_${mode.capitalize()}")
  flink.setUp(
      [
        GO_SDK_CONTAINER
      ],
      initialParallelism,
      "${DOCKER_BEAM_JOBSERVER}/beam_flink${CommonTestProperties.getFlinkVersion()}_job_server:latest")

  // Execute all scenarios connected with initial parallelism.
  loadTestsBuilder.loadTests(scope, CommonTestProperties.SDK.GO, initialScenarios, 'combine', mode)

  // Execute the rest of scenarios.
  testScenariosByParallelism.each { parallelism, scenarios ->
    flink.scaleCluster(parallelism)
    loadTestsBuilder.loadTests(scope, CommonTestProperties.SDK.GO, scenarios, 'combine', mode)
  }
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_LoadTests_Go_Combine_Flink_Batch',
    'Run Load Tests Go Combine Flink Batch',
    'Load Tests Go Combine Flink Batch suite',
    this
    ) {
      additionalPipelineArgs = [:]
      loadTestJob(delegate, CommonTestProperties.TriggeringContext.PR, 'batch')
    }

CronJobBuilder.cronJob('beam_LoadTests_Go_Combine_Flink_Batch', 'H H * * *', this) {
  additionalPipelineArgs = [
    influx_db_name: InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influx_hostname: InfluxDBCredentialsHelper.InfluxDBHostUrl,
  ]
  // TODO(BEAM): Fix this test.
  loadTestJob(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT, 'batch')
}
