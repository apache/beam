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

// This is the Java precommit which runs a Gradle build, and the current set
// of precommit tests.
job('beam_PreCommit_Java_GradleBuild') {
  description('Runs Java PreCommit tests for the current GitHub Pull Request.')

  // Execute concurrent builds if necessary.
  concurrentBuild()

  // Set common parameters.
  common_job_properties.setTopLevelMainJobProperties(
    delegate,
    'master',
    90)

  // Publish all test results to Jenkins
  publishers {
    archiveJunit('**/build/test-results/**/*.xml')
  }

  def gradle_command_line = './gradlew ' + gradle_switches.join(' ') + ' :javaPreCommit'
  // Sets that this is a PreCommit job.
  common_job_properties.setPreCommit(delegate, gradle_command_line, 'Run Java PreCommit')
  steps {
    gradle {
      rootBuildScriptDir(common_job_properties.checkoutDir)
      tasks(':javaPreCommit')
      common_job_properties.setGradleSwitches(delegate)
      // Specify maven home on Jenkins, needed by Maven archetype integration tests.
      switches('-Pmaven_home=/home/jenkins/tools/maven/apache-maven-3.5.2')
    }
  }
}
