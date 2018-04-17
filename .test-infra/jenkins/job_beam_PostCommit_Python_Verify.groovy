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

import common_job_properties

// This job defines the Python postcommit tests.
job('beam_PostCommit_Python_Verify') {
  description('Runs postcommit tests on the Python SDK.')

  previousNames('beam_PostCommit_PythonVerify')

  // Set common parameters.
  common_job_properties.setTopLevelMainJobProperties(delegate)

  // Sets that this is a PostCommit job.
  common_job_properties.setPostCommit(delegate, '0 3-22/6 * * *')

  // Allows triggering this build against pull requests.
  common_job_properties.enablePhraseTriggeringFromPullRequest(
    delegate,
    'Python SDK PostCommit Tests',
    'Run Python PostCommit')

  // Allow the test to only run on particular nodes
  // TODO(BEAM-1817): Remove once the tests can run on all nodes
  parameters {
      nodeParam('TEST_HOST') {
          description('select test host as either beam1, 2 or 3')
          defaultNodes(['beam1', 'beam2', 'beam3'])
          allowedNodes(['beam1', 'beam2', 'beam3'])
          trigger('multiSelectionDisallowed')
          eligibility('IgnoreOfflineNodeEligibility')
      }
  }

  // Execute shell command to test Python SDK.
  steps {
    gradle {
      rootBuildScriptDir(common_job_properties.checkoutDir)
      tasks(':pythonPostCommit')
      common_job_properties.setGradleSwitches(delegate)
    }
  }
}
