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

// This job runs the suite of ValidatesRunner tests against the Apex runner.
job('beam_PostCommit_Java_ValidatesRunner_Apex_Gradle') {
  description('Runs the ValidatesRunner suite on the Apex runner.')
  previousNames('beam_PostCommit_Java_ValidatesRunner_Apex')
  previousNames('beam_PostCommit_Java_RunnableOnService_Apex')

  // Set common parameters.
  common_job_properties.setTopLevelMainJobProperties(delegate)

  def gradle_switches = [
    // Gradle log verbosity enough to diagnose basic build issues
    "--info",
    // Continue the build even if there is a failure to show as many potential failures as possible.
    '--continue',
    // Until we verify the build cache is working appropriately, force rerunning all tasks
    '--rerun-tasks',
  ]

  // Sets that this is a PostCommit job.
  common_job_properties.setPostCommit(delegate)

  // Allows triggering this build against pull requests.
  common_job_properties.enablePhraseTriggeringFromPullRequest(
    delegate,
    'Apache Apex Runner ValidatesRunner Tests',
    'Run Apex ValidatesRunner')

  // Gradle goals for this job.
  steps {
    gradle {
      rootBuildScriptDir(common_job_properties.checkoutDir)
      tasks(':runners:apex:validatesRunner')
      for (String gradle_switch : gradle_switches) {
        switches(gradle_switch)
      }
    }
  }
}
