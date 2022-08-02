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

PrecommitJobBuilder builder = new PrecommitJobBuilder(
    scope: this,
    nameBase: 'Java',
    gradleTask: ':javaPreCommit',
    gradleSwitches: [
      '-PdisableSpotlessCheck=true',
      '-PdisableCheckStyle=true'
    ], // spotless checked in separate pre-commit
    timeoutMins: 180,
    triggerPathPatterns: [
      '^model/.*$',
      '^sdks/java/.*$',
      '^runners/.*$',
      '^examples/java/.*$',
      '^examples/kotlin/.*$',
      '^release/.*$',
    ],
    excludePathPatterns: [
      '^sdks/java/extensions/sql/.*$'
    ]
    )
builder.build {
  publishers {
    archiveJunit('**/build/test-results/**/*.xml')
    recordIssues {
      tools {
        errorProne()
        java()
        spotBugs {
          pattern('**/build/reports/spotbugs/*.xml')
        }
      }
      enabledForFailure(true)
    }
    jacocoCodeCoverage {
      execPattern('**/build/jacoco/*.exec')
      exclusionPattern('**/org/apache/beam/gradle/**,**/org/apache/beam/model/**,' +
          '**/org/apache/beam/runners/dataflow/worker/windmill/**,**/AutoValue_*')
    }
  }
}
