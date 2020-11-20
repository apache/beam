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

import static LoadTestsBuilder.DOCKER_CONTAINER_REGISTRY

String now = new Date().format('MMddHHmmss', TimeZone.getTimeZone('UTC'))

def batchScenarios = {
  [
      [
        title          : 'Load test: 2GB of 10B records',
        test           : 'combine',
        runner         : CommonTestProperties.Runner.FLINK,
        pipelineOptions: [
          job_name             : "load-tests-go-flink-batch-combine-1-${now}",
          influx_measurement   : 'go_batch_combine_1',
          input_options        : """
                                   {
                                     "num_records": 200000000,
                                     "key_size": 1,
                                     "value_size": 9
                                   }
                               """.trim().replaceAll("\\s", ""),
          fanout               : 1,
          top_count            : 20,
          parallelism          : 5,
          endpoint             : 'localhost:8099',
          environment_type     : 'DOCKER',
          environment_config   : "${DOCKER_CONTAINER_REGISTRY}/beam_go_sdk:latest",
        ]
      ],
      [
        title          : 'Load test: fanout 4 times with 2GB 10-byte records total',
        test           : 'combine',
        runner         : CommonTestProperties.Runner.FLINK,
        pipelineOptions: [
          job_name             : "load-tests-go-flink-batch-combine-1-${now}",
          influx_measurement   : 'go_batch_combine_1',
          input_options        : """
                                   {
                                     "num_records": 5000000,
                                     "key_size": 10,
                                     "value_size": 90
                                   }
                               """.trim().replaceAll("\\s", ""),
          fanout               : 4,
          top_count            : 20,
          parallelism          : 16,
          endpoint             : 'localhost:8099',
          environment_type     : 'DOCKER',
          environment_config   : "${DOCKER_CONTAINER_REGISTRY}/beam_go_sdk:latest",
        ]
      ],
      [
        title          : 'Load test: fanout 8 times with 2GB 10-byte records total',
        test           : 'combine',
        runner         : CommonTestProperties.Runner.FLINK,
        pipelineOptions: [
          job_name             : "load-tests-go-flink-batch-combine-1-${now}",
          influx_measurement   : 'go_batch_combine_1',
          fanout               : 8,
          top_count            : 20,
          parallelism          : 16,
          input_options        : """
                                   {
                                     "num_records": 2500000,
                                     "key_size": 10,
                                     "value_size": 90
                                   }
                               """.trim().replaceAll("\\s", ""),
          endpoint             : 'localhost:8099',
          environment_type     : 'DOCKER',
          environment_config   : "${DOCKER_CONTAINER_REGISTRY}/beam_go_sdk:latest",
        ]
      ],
    ].each { test -> test.pipelineOptions.putAll(additionalPipelineArgs) }
}

def loadTestJob = { scope, triggeringContext, mode ->
  def numberOfWorkers = 5

  Flink flink = new Flink(scope, "beam_LoadTests_Go_Combine_Flink_${mode.capitalize()}")
  flink.setUp(
      [
        "${DOCKER_CONTAINER_REGISTRY}/beam_go_sdk:latest"
      ],
      numberOfWorkers,
      "${DOCKER_CONTAINER_REGISTRY}/beam_flink1.10_job_server:latest")

  loadTestsBuilder.loadTests(scope, CommonTestProperties.SDK.GO, batchScenarios(), 'combine', mode)
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_LoadTests_Go_Combine_Flink_Batch',
    'Run Load Tests Go Combine Flink Batch',
    'Load Tests Go Combine Batch suite',
    this
    ) {
      additionalPipelineArgs = [:]
      loadTestJob(delegate, CommonTestProperties.TriggeringContext.PR, 'batch')
    }

CronJobBuilder.cronJob('beam_LoadTests_Go_Combine_Flink_Batch', 'H 10 * * *', this) {
  additionalPipelineArgs = [
    influx_db_name: InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influx_hostname: InfluxDBCredentialsHelper.InfluxDBHostUrl,
  ]
  loadTestJob(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT, 'batch')
}
