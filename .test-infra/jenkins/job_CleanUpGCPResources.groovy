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

job("beam_CleanUpGCPResources") {
  description("Clean up stale resources on Beam's GCP testing project (BQ datasets, )")

  // Set common parameters.
  commonJobProperties.setTopLevelMainJobProperties(delegate)

  // Sets that this is a cron job, run once randomly per day.
  commonJobProperties.setCronJob(delegate, 'H H * * *')

  // Allows triggering this build against pull requests.
  commonJobProperties.enablePhraseTriggeringFromPullRequest(
      delegate,
      'Clean Up GCP Resources',
      'Run Clean GCP Resources')

  // Gradle goals for this job.
  steps {
    gradle {
      rootBuildScriptDir(commonJobProperties.checkoutDir)
      tasks(':beam-test-tools:cleanupOtherStaleResources')
      commonJobProperties.setGradleSwitches(delegate)
    }
  }
}
