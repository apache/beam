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

// This job runs the suite of ValidatesRunner tests against the Gearpump
// runner.
job('beam_PostCommit_Java_ValidatesRunner_Gearpump_Gradle') {
  description('Runs the ValidatesRunner suite on the Gearpump runner.')
  previousNames('beam_PostCommit_Java_ValidatesRunner_Gearpump')
  previousNames('beam_PostCommit_Java_RunnableOnService_Gearpump')

  // Set common parameters.
  common_job_properties.setTopLevelMainJobProperties(
      delegate,
      'gearpump-runner')

  // Publish all test results to Jenkins
  publishers {
    archiveJunit('**/build/test-results/**/*.xml')
  }

  // Sets that this is a PostCommit job.
  // 0 5 31 2 * will run on Feb 31 (i.e. never) according to job properties.
  // In post-commit this job triggers only on SCM changes.
  common_job_properties.setPostCommit(delegate, '0 5 31 2 *')

  // Allows triggering this build against pull requests.
  common_job_properties.enablePhraseTriggeringFromPullRequest(
    delegate,
    'Apache Gearpump Runner ValidatesRunner Tests',
    'Run Gearpump ValidatesRunner')

  // Gradle goals for this job.
  steps {
    gradle {
      rootBuildScriptDir(common_job_properties.checkoutDir)
      tasks(':beam-runners-gearpump:validatesRunner')
      common_job_properties.setGradleSwitches(delegate)
    }
  }
}
