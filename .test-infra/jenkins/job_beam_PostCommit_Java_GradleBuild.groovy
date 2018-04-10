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
job('beam_PostCommit_Java_GradleBuild') {
  description('Runs PostCommit tests on the Java SDK.')

  // Execute concurrent builds if necessary.
  concurrentBuild()

  // Set common parameters.
  common_job_properties.setTopLevelMainJobProperties(delegate, 'master', 240)

  // Publish all test results to Jenkins
  publishers {
    archiveJunit('**/build/test-results/**/*.xml')
  }

  // Sets that this is a PostCommit job.
  common_job_properties.setPostCommit(delegate)

  // Allows triggering this build against pull requests.
  common_job_properties.enablePhraseTriggeringFromPullRequest(
      delegate,
      'Java SDK Post Commit Tests',
      'Run Java PostCommit')

  // Gradle goals for this job.
  steps {
    gradle {
      rootBuildScriptDir(common_job_properties.checkoutDir)
      tasks(':javaPostCommit')
      common_job_properties.setGradleSwitches(delegate)
      // Specify maven home on Jenkins, needed by Maven archetype integration tests.
      switches('-Pmaven_home=/home/jenkins/tools/maven/apache-maven-3.5.2')
    }
  }
}
