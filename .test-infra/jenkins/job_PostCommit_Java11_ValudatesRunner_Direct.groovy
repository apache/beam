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


// This job runs the suite of ValidatesRunner tests against the Direct
// runner using Java 8 to build binaries and JRE 11 to run them.
PostcommitJobBuilder.postCommitJob('beam_PostCommit_Java11_ValidatesRunner_Direct',
        'Run Direct ValidatesRunner in Java 11', 'Direct Runner ValidatesRunner Tests for Java 11', this) {

    description('Runs the ValidatesRunner suite on the Direct runner using Java 11.')

    // Set common parameters. Sets a 3 hour timeout.
    commonJobProperties.setTopLevelMainJobProperties(delegate, 'master', 300)

    // Publish all test results to Jenkins
    publishers {
        archiveJunit('**/build/test-results/**/*.xml')
    }

    delegate.jdk('1.8')
    // Gradle goals for this job.
    def JAVA_JDK_8=tool name: 'JDK 1.8 (latest)', type: 'hudson.model.JDK'
    def JAVA_JDK_11=tool name: 'JDK 11 (latest)', type: 'hudson.model.JDK'

    steps {

        withEnv(["Path+JDK=$JAVA_JDK_8/bin","JAVA_HOME=$JAVA_JDK_8"]) {
            gradle {
                rootBuildScriptDir(commonJobProperties.checkoutDir)
                tasks(':beam-runners-direct-java:shadowJar')
                tasks(':beam-runners-direct-java:shadowTestJar')
                // Increase parallel worker threads above processor limit since most time is
                // spent waiting on Dataflow jobs. ValidatesRunner tests on Dataflow are slow
                // because each one launches a Dataflow job with about 3 mins of overhead.
                // 3 x num_cores strikes a good balance between maxing out parallelism without
                // overloading the machines.
                commonJobProperties.setGradleSwitches(delegate, 3 * Runtime.runtime.availableProcessors())
            }
        }

        withEnv(["Path+JDK=$JAVA_JDK_11/bin","JAVA_HOME=$JAVA_JDK_11"]) {
            gradle {
                rootBuildScriptDir(commonJobProperties.checkoutDir)
                tasks(':beam-runners-direct-java:validatesRunner')
                switches('-x shadowJar')
                switches('-x shadowTestJar')
                switches('-x compileJava')
                switches('-x compileTestJava')
                switches('-x build')
                commonJobProperties.setGradleSwitches(delegate, 3 * Runtime.runtime.availableProcessors())
            }
        }
    }
}
