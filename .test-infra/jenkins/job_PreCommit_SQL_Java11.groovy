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
import CommonJobProperties as properties

PrecommitJobBuilder builder = new PrecommitJobBuilder(
    scope: this,
    nameBase: 'SQL_Java11',
    gradleTask: ':sqlPreCommit',
    gradleSwitches: [
      '-PdisableSpotlessCheck=true',
      '-PcompileAndRunTestsWithJava11',
      '-PskipCheckerFramework',
      // Gradle itself is running under JDK8 so plugin configures wrong for JDK11
      "-Pjava11Home=${properties.JAVA_11_HOME}"
    ], // spotless checked in job_PreCommit_Spotless
    triggerPathPatterns: [
      '^sdks/java/extensions/sql.*$',
    ],
    timeoutMins: 30
    )
builder.build {
  publishers {
    archiveJunit('**/build/test-results/**/*.xml')
    recordIssues {
      tools {
        errorProne()
        java()
        checkStyle {
          pattern('**/build/reports/checkstyle/*.xml')
        }
        configure { node ->
          node / 'spotBugs' << 'io.jenkins.plugins.analysis.warnings.SpotBugs' {
            pattern('**/build/reports/spotbugs/*.xml')
          }
        }
      }
      enabledForFailure(true)
    }
  }
}
