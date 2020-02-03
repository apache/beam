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
    nameBase: 'Java_Examples_Dataflow_Java11',
    gradleTask: ':java11ExamplesDataflowPrecommit',
    gradleSwitches: ['-PdisableSpotlessCheck=true'], // spotless checked in separate pre-commit
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

    def JAVA_11_HOME = '/usr/lib/jvm/java-11-openjdk-amd64'
    def JAVA_8_HOME = '/usr/lib/jvm/java-8-openjdk-amd64'

    steps {
        gradle {
            rootBuildScriptDir(commonJobProperties.checkoutDir)
            tasks ':runners:google-cloud-dataflow-java:testJar'
            tasks ':runners:google-cloud-dataflow-java:worker:legacy-worker:shadowJar'
            switches "-Dorg.gradle.java.home=${JAVA_8_HOME}"
        }

        gradle {
            rootBuildScriptDir(commonJobProperties.checkoutDir)
            tasks ":runners:google-cloud-dataflow-java:examples:java11PreCommit"
            tasks ":runners:google-cloud-dataflow-java:examples-streaming:java11PreCommit"
            switches('-x shadowJar')
            switches('-x shadowTestJar')
            switches('-x compileJava')
            switches('-x compileTestJava')
            switches('-x jar')
            switches('-x testJar')
            switches('-x classes')
            switches('-x testClasses')
            switches "-Dorg.gradle.java.home=${JAVA_11_HOME}"
        }
    }
}
