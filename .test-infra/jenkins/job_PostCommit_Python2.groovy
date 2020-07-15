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

// This job defines the Python postcommit tests.
PostcommitJobBuilder.postCommitJob('beam_PostCommit_Python2', 'Run Python 2 PostCommit',
    'Python2_PC("Run Python 2 PostCommit")', this) {
      description('Runs Python postcommit tests using Python 2.7.')

      previousNames('/beam_PostCommit_Python_Verify/')

      // Set common parameters.
      commonJobProperties.setTopLevelMainJobProperties(delegate)

      publishers {
        archiveJunit('**/nosetests*.xml')
      }

      // Execute shell command to test Python SDK.
      steps {
        gradle {
          rootBuildScriptDir(commonJobProperties.checkoutDir)
          tasks(':python2PostCommit')
          commonJobProperties.setGradleSwitches(delegate)
        }
      }
    }
