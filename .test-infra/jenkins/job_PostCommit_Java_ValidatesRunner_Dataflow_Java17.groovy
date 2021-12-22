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

import CommonJobProperties as commonJobProperties
import PostcommitJobBuilder

// This job runs the suite of ValidatesRunner tests using Java 17 against the Dataflow
// runner compiled with Java 8.
PostcommitJobBuilder.postCommitJob('beam_PostCommit_Java_ValidatesRunner_Dataflow_Java17',
    'Run Dataflow ValidatesRunner Java 17', 'Google Cloud Dataflow Runner ValidatesRunner Tests On Java 17', this) {

      description('Runs the ValidatesRunner suite on the Dataflow runner with Java 17 worker harness.')

      def JAVA_17_HOME = '/usr/lib/jvm/java-17-openjdk-amd64'
      def JAVA_8_HOME = '/usr/lib/jvm/java-8-openjdk-amd64'

      commonJobProperties.setTopLevelMainJobProperties(delegate, 'master', 420)
      publishers {
        archiveJunit('**/build/test-results/**/*.xml')
      }

      steps {
        gradle {
          rootBuildScriptDir(commonJobProperties.checkoutDir)
          tasks(':runners:google-cloud-dataflow-java:testJar')
          tasks(':runners:google-cloud-dataflow-java:worker:legacy-worker:shadowJar')
          switches("-Dorg.gradle.java.home=${JAVA_8_HOME}")
        }

        gradle {
          rootBuildScriptDir(commonJobProperties.checkoutDir)
          tasks(':runners:google-cloud-dataflow-java:validatesRunnerJavaSDK17')
          switches('-x shadowJar')
          switches('-x shadowTestJar')
          switches('-x compileJava')
          switches('-x compileTestJava')
          switches('-x jar')
          switches('-x testJar')
          switches('-x classes')
          switches('-x testClasses')
          //switches("-Dorg.gradle.java.home=${JAVA_17_HOME}")

          commonJobProperties.setGradleSwitches(delegate, 3 * Runtime.runtime.availableProcessors())
        }
      }
    }
