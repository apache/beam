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

// This job runs the suite of ValidatesRunner tests against the Flink runner.
PostcommitJobBuilder.postCommitJob('beam_PostCommit_XVR_Spark3',
    'Run XVR_Spark3 PostCommit', 'Spark3 CrossLanguageValidatesRunner Tests', this) {
      description('Runs the CrossLanguageValidatesRunner suite on the Spark3 runner.')

      // Set common parameters.
      commonJobProperties.setTopLevelMainJobProperties(delegate)

      // Publish all test results to Jenkins
      publishers {
        archiveJunit('**/build/test-results/**/*.xml')
      }

      // Gradle goals for this job.
      steps {
        CROSS_LANGUAGE_VALIDATES_RUNNER_PYTHON_VERSIONS.each { pythonVersion ->
          shell("echo \"*** RUN CROSS-LANGUAGE SPARK3 USING PYTHON ${pythonVersion} ***\"")
          gradle {
            rootBuildScriptDir(commonJobProperties.checkoutDir)
            tasks(':runners:spark:3:job-server:validatesCrossLanguageRunner')
            commonJobProperties.setGradleSwitches(delegate)
            switches("-PpythonVersion=${pythonVersion}")
            // only run non-python task (e.g. GoUsingJava) once
            switches("-PskipNonPythonTask=${pythonVersion != CROSS_LANGUAGE_VALIDATES_RUNNER_PYTHON_VERSIONS[0]}")
          }
        }
      }
    }
