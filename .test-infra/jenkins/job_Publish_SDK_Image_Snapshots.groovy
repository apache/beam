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
import static JavaTestProperties.SUPPORTED_CONTAINER_TASKS as SUPPORTED_JAVA_CONTAINER_TASKS
import static PythonTestProperties.SUPPORTED_CONTAINER_TASKS as SUPPORTED_PYTHON_CONTAINER_TASKS

// This job publishes regular snapshots of the SDK harness containers for
// testing purposes. It builds and pushes the SDK container to the
// specified GCR repo, tagged at the current Git commit.
job('beam_Publish_Beam_SDK_Snapshots') {
  description('Builds SDK harness images snapshots regularly for testing purposes.')

  // Set common parameters.
  commonJobProperties.setTopLevelMainJobProperties(delegate)

  // Runs once every four hours.
  commonJobProperties.setAutoJob(delegate, 'H H/4 * * *')

  // Use jenkins env var interpolation - leave in single quotes
  def imageRepo = 'gcr.io/apache-beam-testing/beam-sdk'
  def imageTag = '${GIT_COMMIT}'

  steps {
    shell("echo 'Pushing SDK snapshots to ${imageRepo} at tag: ${imageTag}'")
    gradle {
      rootBuildScriptDir(commonJobProperties.checkoutDir)
      commonJobProperties.setGradleSwitches(delegate)
      tasks(':sdks:go:container:dockerTagPush')
      SUPPORTED_JAVA_CONTAINER_TASKS.each { taskVer ->
        tasks(":sdks:java:container:${taskVer}:dockerTagPush")
      }
      SUPPORTED_PYTHON_CONTAINER_TASKS.each { taskVer ->
        tasks(":sdks:python:container:${taskVer}:dockerTagPush")
      }
      switches("-Pdocker-repository-root=${imageRepo}")
      switches("-Pdocker-tag-list=${imageTag},latest")
      switches("-Pjava11Home=${commonJobProperties.JAVA_11_HOME}")
    }
  }
}
