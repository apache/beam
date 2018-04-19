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

// This job runs the suite of ValidatesRunner tests against the Dataflow
// runner.
job('beam_PostCommit_Java_ValidatesRunner_Dataflow_Gradle') {
  description('Runs the ValidatesRunner suite on the Dataflow runner.')
  previousNames('beam_PostCommit_Java_ValidatesRunner_Dataflow')
  previousNames('beam_PostCommit_Java_RunnableOnService_Dataflow')

  // Set common parameters. Sets a long (3 hour) timeout due to timeouts in [BEAM-3775].
  common_job_properties.setTopLevelMainJobProperties(delegate, 'master', 180)

  // Publish all test results to Jenkins
  publishers {
    archiveJunit('**/build/test-results/**/*.xml')
  }

  // Sets that this is a PostCommit job.
  common_job_properties.setPostCommit(delegate)

  // Allows triggering this build against pull requests.
  common_job_properties.enablePhraseTriggeringFromPullRequest(
    delegate,
    'Google Cloud Dataflow Runner ValidatesRunner Tests',
    'Run Dataflow ValidatesRunner')

  // Gradle goals for this job.
  steps {
    gradle {
      rootBuildScriptDir(common_job_properties.checkoutDir)
      tasks(':beam-runners-google-cloud-dataflow-java:validatesRunner')
      common_job_properties.setGradleSwitches(delegate)
    }
  }
}
