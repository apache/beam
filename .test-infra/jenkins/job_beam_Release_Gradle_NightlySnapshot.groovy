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

// This creates the nightly snapshot build.
// Into https://repository.apache.org/content/groups/snapshots/org/apache/beam.
job('beam_Release_Gradle_NightlySnapshot') {
  description('Publish a nightly snapshot.')

  // Execute concurrent builds if necessary.
  concurrentBuild()

  // Set common parameters.
  common_job_properties.setTopLevelMainJobProperties(delegate)

  // This is a post-commit job that runs once per day, not for every push.
  common_job_properties.setPostCommit(
      delegate,
      '0 7 * * *',
      false,
      'dev@beam.apache.org')


  def gradle_switches = [
  ]

  // Allows triggering this build against pull requests.
  common_job_properties.enablePhraseTriggeringFromPullRequest(
      delegate,
      './gradlew publish',
      'Run Gradle Publish')

  steps {
    gradle {
      rootBuildScriptDir(common_job_properties.checkoutDir)
      tasks('publish')
      common_job_properties.setGradleSwitches(delegate)
      // Publish a snapshot build.
      switches("-Ppublishing")
      // Don't run tasks in parallel, currently the maven-publish/signing plugins
      // cause build failures when run in parallel with messages like 'error snapshotting'
      switches('--no-parallel')
    }
  }
}

