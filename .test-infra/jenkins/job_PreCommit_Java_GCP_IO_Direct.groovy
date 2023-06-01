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
    nameBase: 'Java_GCP_IO_Direct',
    gradleTasks: [
      ':sdks:java:io:google-cloud-platform:build',
      ':sdks:java:io:google-cloud-platform:expansion-service:build',
      ':sdks:java:io:google-cloud-platform:postCommit',
    ],
    gradleSwitches: [
      '-PdisableSpotlessCheck=true',
      '-PdisableCheckStyle=true',
      '-PenableJacocoReport'
    ], // spotless checked in separate pre-commit
    timeoutMins: 120,
    triggerPathPatterns: [
      '^runners/core-construction-java/.*$',
      '^runners/core-java/.*$',
      '^sdks/java/core/src/main/.*$',
      '^sdks/java/extensions/arrow/.*$',
      '^sdks/java/extensions/google-cloud-platform-core/.*$',
      '^sdks/java/extensions/protobuf/.*$',
      '^sdks/java/testing/test-utils/.*$',
      '^sdks/java/io/common/.*$',
      '^sdks/java/io/expansion-service/.*$',
      '^sdks/java/io/google-cloud-platform/.*$',
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
      exclusionPattern('**/AutoValue_*')
      inclusionPattern("**/org/apache/beam/sdk/io/gcp/**")
    }
  }
}
