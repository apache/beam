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

// This is the Go precommit which runs a gradle build, and the current set
// of precommit tests.
job('beam_PreCommit_Go_GradleBuild') {
  description('Runs Go PreCommit tests for the current GitHub Pull Request.')

  // Execute concurrent builds if necessary.
  concurrentBuild()

  // Set common parameters.
  common_job_properties.setTopLevelMainJobProperties(
    delegate,
    'master',
    240)

  def gradle_switches = [
    // Gradle log verbosity enough to diagnose basic build issues
    "--info",
    // Continue the build even if there is a failure to show as many potential failures as possible.
    '--continue',
    // Until we verify the build cache is working appropriately, force rerunning all tasks
    '--rerun-tasks',
  ]

  def gradle_command_line = './gradlew ' + gradle_switches.join(' ') + ' :goPreCommit'
  // Sets that this is a PreCommit job.
  common_job_properties.setPreCommit(delegate, gradle_command_line, 'Run Go PreCommit')
  steps {
    gradle {
      rootBuildScriptDir(common_job_properties.checkoutDir)
      tasks(':goPreCommit')
      for (String gradle_switch : gradle_switches) {
        switches(gradle_switch)
      }
    }
  }
}
