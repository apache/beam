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
 * 
 * This test builds Beam Python SDK multi-arch containers and use the
 * Arm component to run Dataflow wordcount pipelines on Arm.
 *
 */
import CommonJobProperties as commonJobProperties
import PostcommitJobBuilder

import static PythonTestProperties.VALIDATES_CONTAINER_DATAFLOW_PYTHON_VERSIONS
import java.time.LocalDateTime

// This job runs the suite of Python Arm tests against the
// Dataflow runner.
PostcommitJobBuilder.postCommitJob('beam_PostCommit_Py_Dataflow_Arm',
    'Run Python Dataflow Arm', 'Google Cloud Dataflow Runner Python Arm Tests', this) {
      description('Runs Python Arm suite on the Dataflow runner.')
      // Set common parameters.
      commonJobProperties.setTopLevelMainJobProperties(delegate)
      publishers {
        archiveJunit('**/pytest*.xml')
      }

      // Generates a unique tag for the container as the current time.
      def now = LocalDateTime.now()
      def unique_tag = '${now.date}${now.hour}${now.minute}${now.second}'
      // Execute shell command to test Python SDK.
      steps {
        gradle {
          rootBuildScriptDir(commonJobProperties.checkoutDir)
          tasks(':sdks:python:test-suites:dataflow:validatesContainerTests')
          switches('-Pcontainer-architecture-list=arm64,amd64')
          switches('-Ppush-containers')
          // Push multi-arch containers to the repository set in run_validatescontainer.sh
          switches('-Pdocker-repository-root=us.gcr.io/apache-beam-testing/jenkins')
          switches("-Pdocker-tag=${unique_tag}")
          commonJobProperties.setGradleSwitches(delegate)
        }
      }
    }
