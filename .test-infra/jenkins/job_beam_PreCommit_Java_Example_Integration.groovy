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

// This is the Java Precommit which executes integration tests on each runner.
// This Precommit only compiles the SDK and tests, and executes the Integration
// tests within the examples/java module.
mavenJob('beam_PreCommit_Java_Integration_Tests') {
  description('Runs Java Examples Integration Tests of the current GitHub Pull Request.')

  previousNames('beam_PreCommit_Java_MavenInstall')

  // Execute concurrent builds if necessary.
  concurrentBuild()

  // Set common parameters.
  common_job_properties.setTopLevelMainJobProperties(
    delegate,
    'master',
    60)

  // Set Maven parameters.
  common_job_properties.setMavenConfig(delegate)

  // Sets that this is a PreCommit job which runs the integration-tests.
  common_job_properties.setPreCommit(delegate, 'mvn failsafe:integration-test -pl examples/java -am', 'Run Java Integration PreCommit')

  // Maven goals for this job: The Java Examples and their dependencies
  goals([
      '--batch-mode',
      '--errors',
      '--activate-profiles jenkins-precommit,direct-runner,dataflow-runner,spark-runner,flink-runner,apex-runner',
      '--projects examples/java',
      '--also-make',
      '-D checkstyle.skip',
      '-D findbugs.skip',
      '-D pullRequest=$ghprbPullId',
      'help:effective-settings',
      'clean',
      'compile',
      'test-compile',
      'failsafe:integration-test',
      'failsafe:verify'].join(' '))
}

