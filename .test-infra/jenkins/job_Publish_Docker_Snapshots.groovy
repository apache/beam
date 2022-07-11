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
import static PythonTestProperties.SUPPORTED_CONTAINER_TASKS

job('beam_Publish_Docker_Snapshots') {
  description('Builds SDK harness images and job server images for testing purposes.')

  // Set common parameters.
  commonJobProperties.setTopLevelMainJobProperties(delegate)

  // Allows triggering this build against pull requests.
  commonJobProperties.enablePhraseTriggeringFromPullRequest(
      delegate,
      'Beam Publish Docker Snapshots',
      'Publish Docker Snapshots',
      false
      )

  // Runs once per day.
  commonJobProperties.setAutoJob(delegate, '@daily')

  steps {
    gradle {
      rootBuildScriptDir(commonJobProperties.checkoutDir)
      commonJobProperties.setGradleSwitches(delegate)
      SUPPORTED_CONTAINER_TASKS.each { taskVer ->
        tasks(":sdks:python:container:${taskVer}:dockerPush")
      }
      tasks(":runners:flink:${CommonTestProperties.getFlinkVersion()}:job-server-container:dockerPush")
      switches("-Pdocker-repository-root=gcr.io/apache-beam-testing/beam_portability")
      switches("-Pdocker-tag=latest")
    }
  }
}
