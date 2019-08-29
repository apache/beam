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
import PostcommitJobBuilder
import CronJobBuilder
import LoadTestsBuilder
import Flink
import Docker

def job = { scope ->
  scope.description('Runs the Chicago Taxi Example on the Flink runner')
  commonJobProperties.setTopLevelMainJobProperties(scope)

  Docker publisher = new Docker(scope, LoadTestsBuilder.DOCKER_CONTAINER_REGISTRY)
  def sdk = CommonTestProperties.SDK.PYTHON
  String sdkName = sdk.name().toLowerCase()
  String pythonHarnessImageTag = publisher.getFullImageName(sdkName)

  def numberOfWorkers = 5

  publisher.publish(":sdks:${sdkName}:container:docker", sdkName)
  publisher.publish(':runners:flink:1.7:job-server-container:docker', 'flink-job-server')

  Flink flink = new Flink(scope, 'beam_PostCommit_Python_Chicago_Taxi_Flink')
  flink.setUp([pythonHarnessImageTag], numberOfWorkers, publisher.getFullImageName('flink-job-server'))

  scope.steps {
    gradle {
      rootBuildScriptDir(commonJobProperties.checkoutDir)
      tasks(':sdks:python:apache_beam:testing:benchmarks:chicago_taxi:run')
      switches('-PgcsRoot=gs://temp-storage-for-perf-tests/chicago-taxi')
      switches('-Prunner=PortableRunner')
      switches('-PpublishToBigQuery=false')
      switches('-PmetricsTableSuffix=-flink')
      switches('-PjobEndpoint=localhost:8099')
      switches("-PenvironmentConfig=${pythonHarnessImageTag}")
      switches('-PenvironmentType=DOCKER')
      switches("-Pparallelism=${numberOfWorkers}")
    }
  }
}

PostcommitJobBuilder.postCommitJob(
  'beam_PostCommit_Python_Chicago_Taxi_Flink',
  'Run Chicago Taxi on Flink',
  'Google Cloud Flink Runner Chicago Taxi Example', this
) {
  job(delegate)
}

CronJobBuilder.cronJob(
  'beam_PostCommit_Python_Chicago_Taxi_Flink', 'H 14 * * *', this) {
  job(delegate)
}
