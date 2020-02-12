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
import CommonJobProperties as commonJobProperties

PrecommitJobBuilder builder = new PrecommitJobBuilder(
    scope: this,
    nameBase: 'Java11',
    gradleTask: ':clean', // Do nothing here
    gradleSwitches: ['-PdisableSpotlessCheck=true'], // spotless checked in separate pre-commit
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
    jacocoCodeCoverage {
      execPattern('**/build/jacoco/*.exec')
    }
  }

  steps {
    gradle {
      rootBuildScriptDir(commonJobProperties.checkoutDir)
      tasks 'javaPreCommit'
      switches "-Dorg.gradle.java.home=${commonJobProperties.JAVA_8_HOME}"
      switches '-PdisableSpotlessCheck=true'
      switches '-x directRunnerPreCommit'
      switches '-x flinkRunnerPreCommit'
      switches '-x sparkRunnerPreCommit'
      switches '-x shadowJarTest'
      switches '-x runNeedsRunnerTests'
      commonJobProperties.setGradleSwitches(delegate)
    }

    gradle {
      rootBuildScriptDir(commonJobProperties.checkoutDir)
      tasks ':examples:java:directRunnerPreCommit'
      tasks ':examples:java:flinkRunnerPreCommit'
      tasks ':examples:java:sparkRunnerPreCommit'
      tasks ':sdks:java:extensions:sql:jdbc:shadowJarTest'
      tasks ':runners:direct-java:runNeedsRunnerTests'
      switches '-x shadowJar'
      switches '-x compileJava'
      switches '-x jar'
      switches '-x classes'
      switches "-Dorg.gradle.java.home=${commonJobProperties.JAVA_11_HOME}"
      switches '-PdisableSpotlessCheck=true'
      commonJobProperties.setGradleSwitches(delegate)
    }
  }
}
