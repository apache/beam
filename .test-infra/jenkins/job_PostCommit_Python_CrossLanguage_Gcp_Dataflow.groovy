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


import static PythonTestProperties.CROSS_LANGUAGE_VALIDATES_RUNNER_PYTHON_VERSIONS


// This job runs end-to-end cross language GCP IO tests with DataflowRunner.
// Collects tests with the @pytest.mark.uses_gcp_java_expansion_service decorator
PostcommitJobBuilder.postCommitJob('beam_PostCommit_Python_Xlang_Gcp_Dataflow',
    'Run Python_Xlang_Gcp_Dataflow PostCommit', 'Python_Xlang_Gcp_Dataflow (\"Run Python_Xlang_Gcp_Dataflow PostCommit\")', this) {
      description('Runs end-to-end cross language GCP IO tests on the Dataflow runner. \"Run Python_Xlang_Gcp_Dataflow PostCommit\"')


      // Set common parameters.
      commonJobProperties.setTopLevelMainJobProperties(delegate, 'master', 180)


      // Publish all test results to Jenkins
      publishers {
        archiveJunit('**/pytest*.xml')
      }


      // Gradle goals for this job.

      steps {
        gradle {
          rootBuildScriptDir(commonJobProperties.checkoutDir)
          tasks(":sdks:python:test-suites:dataflow:gcpCrossLanguagePostCommit")
          commonJobProperties.setGradleSwitches(delegate)
          switches("-PuseWheelDistribution")
        }
      }
    }
