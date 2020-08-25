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
    nameBase: 'Java_Examples_Dataflow_Java11',
    gradleTask: ':clean',
    gradleSwitches: [
      '-PdisableSpotlessCheck=true',
      '-PskipCheckerFramework' // Gradle itself is running under JDK8 so plugin configures wrong for JDK11
    ], // spotless checked in separate pre-commit
    triggerPathPatterns: [
      '^model/.*$',
      '^sdks/java/.*$',
      '^runners/google-cloud-dataflow-java/.*$',
      '^examples/java/.*$',
      '^examples/kotlin/.*$',
      '^release/.*$',
    ],
    timeoutMins: 30,
    )
builder.build {
  publishers {
    archiveJunit('**/build/test-results/**/*.xml')
  }

  steps {
    gradle {
      rootBuildScriptDir(properties.checkoutDir)
      tasks 'javaExamplesDataflowPreCommit'
      switches '-Pdockerfile=Dockerfile-java11'
      switches '-PdisableSpotlessCheck=true'
      switches '-PskipCheckerFramework' // Gradle itself is running under JDK8 so plugin configures wrong for JDK11
      switches '-PcompileAndRunTestsWithJava11'
      switches "-Pjava11Home=${properties.JAVA_11_HOME}"
      properties.setGradleSwitches(delegate, 3 * Runtime.runtime.availableProcessors())
    }
  }
}
