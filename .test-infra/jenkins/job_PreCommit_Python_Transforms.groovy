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

import PrecommitJobBuilder

def pythonPrecommitJob(String gradleTask, String nameBase, boolean commitTriggering = true) {
  PrecommitJobBuilder builder = new PrecommitJobBuilder(
    scope: this,
    nameBase: nameBase,
    gradleTask: gradleTask,
    gradleSwitches: [
      '-Pposargs=apache_beam/transforms/'
    ],
    timeoutMins: 180,
    triggerPathPatterns: [
      '^model/.*$',
      '^sdks/python/.*$',
      '^release/.*$',
    ],
    commitTriggering: commitTriggering,
    )
  builder.build {
    // Publish all test results to Jenkins.
    publishers {
      archiveJunit('**/pytest*.xml')
    }
  }
}

// This job uses stable release dependencies so we will run it on every commit.
pythonPrecommitJob(gradleTask=":pythonPreCommit", nameBase="Python_Transforms", commitTriggering=true)
// This jon uses release candidate dependencies. making it run on every commit might create noise. so we will run it as a cron job.
pythonPrecommitJob(gradleTask=":pythonPreCommitRC", nameBase="PythonRC_Transforms", commitTriggering=false)
