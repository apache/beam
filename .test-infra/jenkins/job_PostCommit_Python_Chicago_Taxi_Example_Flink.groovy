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
import CronJobBuilder
import Flink
import LoadTestsBuilder
import PostcommitJobBuilder

import static LoadTestsBuilder.DOCKER_CONTAINER_REGISTRY

def chicagoTaxiJob = { scope ->
  scope.description('Runs the Chicago Taxi Example on the Flink runner.')
  commonJobProperties.setTopLevelMainJobProperties(scope, 'master', 120)

  def numberOfWorkers = 5

  Flink flink = new Flink(scope, 'beam_PostCommit_Python_Chicago_Taxi_Flink')
  flink.setUp(
      [
        "${DOCKER_CONTAINER_REGISTRY}/beam_python3.7_sdk:latest"
      ],
      numberOfWorkers,
      "${DOCKER_CONTAINER_REGISTRY}/beam_flink1.10_job_server:latest")

  def pipelineOptions = [
    parallelism             : numberOfWorkers,
    job_endpoint            : 'localhost:8099',
    environment_config      : "${DOCKER_CONTAINER_REGISTRY}/beam_python3.7_sdk:latest",
    environment_type        : 'DOCKER',
    execution_mode_for_batch: 'BATCH_FORCED',
  ]

  scope.steps {
    gradle {
      rootBuildScriptDir(commonJobProperties.checkoutDir)
      tasks(':sdks:python:test-suites:portable:py37:chicagoTaxiExample')
      switches('-PgcsRoot=gs://temp-storage-for-perf-tests/chicago-taxi')
      switches("-PpipelineOptions=\"${LoadTestsBuilder.parseOptions(pipelineOptions)}\"")
    }
  }
}

PostcommitJobBuilder.postCommitJob(
    'beam_PostCommit_Python_Chicago_Taxi_Flink',
    'Run Chicago Taxi on Flink',
    'Chicago Taxi Example on Flink ("Run Chicago Taxi on Flink")',
    this
    ) {
      chicagoTaxiJob(delegate)
    }

// TODO(BEAM-9154): Chicago Taxi Example doesn't work in Python 3.
// Uncomment below once it is fixed.
//
// CronJobBuilder.cronJob(
//     'beam_PostCommit_Python_Chicago_Taxi_Flink',
//     'H 14 * * *',
//     this
//     ) {
//       chicagoTaxiJob(delegate)
//     }
