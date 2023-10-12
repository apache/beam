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

// This job runs multi-language pipelines using the Docker Compose based TransformService against the Direct runner.
// Collects tests with the @pytest.mark.uses_transform_service decorator
PostcommitJobBuilder.postCommitJob('beam_PostCommit_TransformService_Direct',
    'Run TransformService_Direct PostCommit', 'Direct TransformService Tests', this) {
      description('Runs the TransformService suite on the Direct runner.')

      // Set common parameters.
      commonJobProperties.setTopLevelMainJobProperties(delegate, 'master', 120)

      // Publish all test results to Jenkins
      publishers {
        archiveJunit('**/pytest*.xml')
      }

      // Gradle goals for this job.
      steps {
        CROSS_LANGUAGE_VALIDATES_RUNNER_PYTHON_VERSIONS.each { pythonVersion ->
          shell("echo \"*** RUN TRANSFORM SERVICE Python SDK TESTS USING THE DIRECT RUNNER AND THE PYTHON VERSION ${pythonVersion} ***\"")
          gradle {
            rootBuildScriptDir(commonJobProperties.checkoutDir)
            tasks(':sdks:python:test-suites:direct:xlang:transformServicePythonUsingJava')
            commonJobProperties.setGradleSwitches(delegate)
            switches '-PtestJavaVersion=11'
            switches "-Pjava11Home=${commonJobProperties.JAVA_11_HOME}"
            switches("-PuseWheelDistribution")
            switches("-PpythonVersion=${pythonVersion}")
          }
        }
      }
    }
