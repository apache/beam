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
import PostcommitJobBuilder


// This is the Go postcommit which runs a gradle build, and the current set
// of postcommit tests.
PostcommitJobBuilder.postCommitJob('beam_PostCommit_Go_GradleBuild', 'Run Go PostCommit',
  './gradlew :goPostCommit', this) {
  description('Runs Go PostCommit tests against master.')

  // Execute concurrent builds if necessary.
  concurrentBuild()

  // Set common parameters.
  common_job_properties.setTopLevelMainJobProperties(
    delegate,
    'master',
    150)

  steps {
    gradle {
      rootBuildScriptDir(common_job_properties.checkoutDir)
      tasks(':goPostCommit')
      common_job_properties.setGradleSwitches(delegate)
    }
  }
}
