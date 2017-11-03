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

// This job runs the Java postcommit tests, including the suite of integration
// tests.
mavenJob('beam_PostCommit_Java_MavenInstall') {
  description('Runs postcommit tests on the Java SDK.')

  previousNames('beam_PostCommit_MavenVerify')

  // Execute concurrent builds if necessary.
  concurrentBuild()

  // Set common parameters.
  common_job_properties.setTopLevelMainJobProperties(delegate, 'master', 240)

  // Set maven parameters.
  common_job_properties.setMavenConfig(delegate)

  // Sets that this is a PostCommit job.
  common_job_properties.setPostCommit(delegate)

  // Allows triggering this build against pull requests.
  common_job_properties.enablePhraseTriggeringFromPullRequest(
          delegate,
          'Java SDK Post Commit Tests',
          'Run Java PostCommit')

  // Maven goals for this job.
  goals([
      'clean',
      'install',
      '--projects sdks/java/core,runners/direct-java,sdks/java/fn-execution',
      ' --also-make',
      '--also-make-dependents',
      '--batch-mode',
      '--errors',
      '--fail-at-end',
      '-P release,dataflow-runner',
      '-DrepoToken=$COVERALLS_REPO_TOKEN',
      '-D skipITs=false',
      '''-D integrationTestPipelineOptions=\'[ \
          "--project=apache-beam-testing", \
          "--tempRoot=gs://temp-storage-for-end-to-end-tests", \
          "--runner=TestDataflowRunner" \
        ]\' '''
  ].join(' '))
}
