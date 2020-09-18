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
import PostcommitJobBuilder

// This job runs the suite of Python ValidatesRunner tests against the
// Dataflow runner V2.
PostcommitJobBuilder.postCommitJob('beam_PostCommit_Py_VR_Dataflow_V2', 'Run Python Dataflow V2 ValidatesRunner',
    'Google Cloud Dataflow Runner V2 Python ValidatesRunner Tests', this) {
      description('Runs Python ValidatesRunner suite on the Dataflow runner v2.')

      // Set common parameters.
      commonJobProperties.setTopLevelMainJobProperties(delegate)

      publishers {
        archiveJunit('**/nosetests*.xml')
      }

      // Execute gradle task to test Python SDK.
      steps {
        gradle {
          rootBuildScriptDir(commonJobProperties.checkoutDir)
          // TODO: Enable following tests after making sure we have enough capacity.
          // tasks(':sdks:python:test-suites:dataflow:py35:validatesRunnerBatchTests')
          // tasks(':sdks:python:test-suites:dataflow:py36:validatesRunnerBatchTests')
          tasks(':sdks:python:test-suites:dataflow:py37:validatesRunnerBatchTests')
          // tasks(':sdks:python:test-suites:dataflow:py35:validatesRunnerStreamingTests')
          // tasks(':sdks:python:test-suites:dataflow:py36:validatesRunnerStreamingTests')
          tasks(':sdks:python:test-suites:dataflow:py37:validatesRunnerStreamingTests')
          switches('-PuseRunnerV2')
          commonJobProperties.setGradleSwitches(delegate)
        }
      }
    }
