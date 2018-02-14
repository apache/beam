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

// This is the Python precommit which runs a maven install, and the current set
// of precommit tests.
mavenJob('beam_PreCommit_Python_MavenInstall') {
  description('Runs an install of the current GitHub Pull Request.')

  previousNames('beam_PreCommit_MavenVerify')

  // Execute concurrent builds if necessary.
  concurrentBuild()

  // Set common parameters.
  common_job_properties.setTopLevelMainJobProperties(
    delegate,
    'master',
    150)

  // Set Maven parameters.
  common_job_properties.setMavenConfig(delegate)

  // Publish all test results to Jenkins
  publishers {
    archiveJunit('**/nosetests.xml')
  }

  // Sets that this is a PreCommit job.
  common_job_properties.setPreCommit(delegate, 'mvn clean install -pl sdks/python -am -amd', 'Run Python PreCommit')

  // Maven modules for this job: The Python SDK, its dependencies, and things that depend on it,
  // excluding the container.
  goals([
    '--batch-mode',
    '--errors',
    '--activate-profiles release',
    '--projects sdks/python,!sdks/python/container',
    '--also-make',
    '--also-make-dependents',
    '-D pullRequest=$ghprbPullId',
    'help:effective-settings',
    'clean',
    'install',
  ].join(' '))
}
