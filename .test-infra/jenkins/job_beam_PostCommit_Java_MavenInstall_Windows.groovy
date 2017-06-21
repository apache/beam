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

// This is the Java postcommit which runs maven install targeting Jenkins running on Windows.
mavenJob('beam_PostCommit_Java_MavenInstall_Windows') {
  description('Runs postcommit tests on Windows for the Java SDK.')

  // Execute concurrent builds if necessary.
  concurrentBuild()

  // Set common parameters. Note the usage of the Windows label to filter Jenkins executors.
  common_job_properties.setTopLevelMainJobProperties(delegate, 'master', 100, 'Windows')

  // Set Maven parameters. Note the usage of the Windows Maven installation
  common_job_properties.setMavenConfig(delegate, 'Maven 3.3.3 (Windows)')

  // Sets that this is a PostCommit job.
  // TODO(BEAM-1042, BEAM-1045, BEAM-2269, BEAM-2299) Turn notifications back on once fixed.
  common_job_properties.setPostCommit(delegate, '0 */6 * * *', false, '', false)

  // Allows triggering this build against pull requests.
  common_job_properties.enablePhraseTriggeringFromPullRequest(
          delegate,
          'Java SDK Windows PostCommit Tests',
          'Run Java Windows PostCommit')

  // Maven goals for this job.
  goals('-B -e -Prelease,direct-runner -DrepoToken=$COVERALLS_REPO_TOKEN -DpullRequest=$ghprbPullId help:effective-settings clean install coveralls:report')
}
