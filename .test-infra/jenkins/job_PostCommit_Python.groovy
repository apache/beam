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

import static PythonTestProperties.ALL_SUPPORTED_VERSIONS

// This job defines the Python postcommit tests.
ALL_SUPPORTED_VERSIONS.each { pythonVersion ->
  def versionSuffix = pythonVersion.replace('.', '')
  PostcommitJobBuilder.postCommitJobWithTrigger("beam_PostCommit_Python${versionSuffix}",
      "Run Python ${pythonVersion} PostCommit",
      "Python${versionSuffix}_PC(\"Run Python ${pythonVersion} PostCommit\")", this) {
        description('Runs Python postcommit tests using Python ${pythonVersion}.')

        // Set common parameters.
        commonJobProperties.setTopLevelMainJobProperties(delegate, 'master', 180)

        publishers {
          archiveJunit('**/pytest*.xml')
        }

        // Execute shell command to test Python SDK.
        steps {
          gradle {
            rootBuildScriptDir(commonJobProperties.checkoutDir)
            tasks(":python${versionSuffix}PostCommit")
            commonJobProperties.setGradleSwitches(delegate)
          }
        }
      }
}

