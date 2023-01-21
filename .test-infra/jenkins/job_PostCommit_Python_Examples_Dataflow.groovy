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

// This job runs the Python examples tests with DataflowRunner.
PostcommitJobBuilder.postCommitJob('beam_PostCommit_Python_Examples_Dataflow',
    'Run Python Examples_Dataflow', 'Python Dataflow Runner Examples', this) {

      description('Runs the Python Examples with DataflowRunner')

      commonJobProperties.setTopLevelMainJobProperties(delegate, 'master', 180)

      // Publish all test results to Jenkins
      publishers {
        archiveJunit('**/pytest*.xml')
      }

      // Execute shell command to run examples.
      steps {
        gradle {
          rootBuildScriptDir(commonJobProperties.checkoutDir)
          tasks(":sdks:python:test-suites:dataflow:examplesPostCommit")
          commonJobProperties.setGradleSwitches(delegate)
        }
      }
    }
