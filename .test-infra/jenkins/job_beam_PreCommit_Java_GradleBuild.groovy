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
  description('Runs a build of the current GitHub Pull Request.')

  // Execute concurrent builds if necessary.
  concurrentBuild()

  // Set common parameters.
  common_job_properties.setTopLevelMainJobProperties(
    delegate,
    'master',
    240)

  // Sets that this is a PreCommit job.
  common_job_properties.setPreCommit(delegate, './gradlew --continue --rerun-tasks :beam-sdks-parent:beam-sdks-java-parent:beam-sdks-java-core:buildDependents :beam-runners-parent:beam-runners-direct-java:buildDependents :beam-sdks-parent:beam-sdks-java-parent:beam-sdks-java-fn-execution:buildDependents :beam-sdks-parent:beam-sdks-java-parent:beam-sdks-java-core:buildNeeded :beam-runners-parent:beam-runners-direct-java:buildNeeded :beam-sdks-parent:beam-sdks-java-parent:beam-sdks-java-fn-execution:buildNeeded', 'Run Java Gradle PreCommit')

  steps {
    gradle {
      tasks(':beam-sdks-parent:beam-sdks-java-parent:beam-sdks-java-core:buildNeeded')
      tasks(':beam-sdks-parent:beam-sdks-java-parent:beam-sdks-java-core:buildDependents')
      tasks(':beam-runners-parent:beam-runners-direct-java:buildNeeded')
      tasks(':beam-runners-parent:beam-runners-direct-java:buildDependents')
      tasks(':beam-sdks-parent:beam-sdks-java-parent:beam-sdks-java-fn-execution:buildNeeded')
      tasks(':beam-sdks-parent:beam-sdks-java-parent:beam-sdks-java-fn-execution:buildDependents')
      // Continue the build even if there is a failure to show as many potential failures as possible.
      switches('--continue')
      // Until we verify the build cache is working appropriately, force rerunning all tasks
      switches('--rerun-tasks')
    }
  }
}
